use std::cmp::min;
use std::io::Read;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use anyhow::{bail, ensure};
use bitvec::{bitbox, bitvec};
use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use log::{debug, error, info};
use num_enum::TryFromPrimitive;
use portable_atomic::AtomicBool;
use tokio::io::AsyncRead;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::{join, select};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use crate::message::Message;
use crate::piece::{Block, PieceFiles, PieceWriter};
use crate::speed_estimator::SpeedEstimator;
use crate::torrent::{Torrent};
use crate::types::*;

pub struct PeerConnectionTasks {
    pub rx_task: JoinHandle<()>,
    pub tx_task: JoinHandle<()>
}

// impl Drop for PeerConnectionTasks {
//     fn drop(&mut self) {
//         self.rx_task.abort();
//         self.tx_task.abort();
//     }
// }

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum InternalMessage {
    Interested,
    BitField,
    RequestBlock {
        piece: u32,
        block: Block
    },
    Choked,
    Unchoked,
    Have {
        piece: u32
    }
}

pub struct PeerConnection {
    pub addr: SocketAddr,
    pub torrent: Rc<Torrent>,
    pub status: Mutex<PeerStatus>,
    pub interested: AtomicBool,
    pub internal_channel: Sender<InternalMessage>,
}

#[derive(Default)]
pub struct PeerStatus {
    pub map: BitBox<u8, Msb0>,
    pub download_speed: SpeedEstimator<usize, 2>,
    pub upload_speed: SpeedEstimator<u64, 4>
}
pub struct PeerConnectionDownload {
    pub peer_connection: IoRc<PeerConnection>,
    pub read: OwnedReadHalf,
    pub to_send: Sender<(u32, Block)>,
    pub choked: bool,
    pub verify_task: Option<JoinHandle<()>>,
    pub downloading_piece: Option<(u32, PieceWriter, usize)>,
    pub last_block_receive: Instant
}

pub struct PeerConnectionUpload {
    pub peer_connection: IoRc<PeerConnection>,
    pub write: OwnedWriteHalf,
    pub to_send: Receiver<(u32, Block)>,
    pub internal_channel: Receiver<InternalMessage>,
    pub choked_upload: bool,
    pub uploading_piece: Option<(u32, PieceFiles)>
}

impl PeerConnection {
    pub async fn connect(addr: SocketAddr, torrent: IoRc<Torrent>, socket: TcpStream, incoming: bool) -> anyhow::Result<(IoRc<PeerConnection>, PeerConnectionTasks)> {
        let (mut read, write) = socket.into_split();
        let (sender, receiver) = channel(torrent.client.max_pending_per_peer);
        let (internal_channel_send, internal_channel_recv) = channel(2);
        let piece_count = torrent.hashes.len();

        let status = PeerStatus {
            map: bitbox![u8, Msb0; 0u8; piece_count],
            download_speed: Default::default(),
            upload_speed: Default::default(),
        };

        let peer_connection = IoRc::new(PeerConnection {
            addr,
            torrent,
            status: Mutex::new(status),
            interested: AtomicBool::new(false),
            internal_channel: internal_channel_send
        });

        let mut upload = PeerConnectionUpload {
            peer_connection: peer_connection.clone(),
            write,
            to_send: receiver,
            internal_channel: internal_channel_recv,
            choked_upload: true,
            uploading_piece: None
        };

        info!("start connecting");
        upload.send_handshake().await?;
        info!("handshake sent");
        if !incoming {
            if receive_handshake(&mut read).await? != peer_connection.torrent.hash {
                bail!("tried to connect to peer, hash mismatch");
            }
        }

        let send_handle = spawn(async move {
            info!("send task started");
            let result: anyhow::Result<()> = try {
                upload.send_bitfield().await?;
                upload.unchoke().await?;
                info!("sent bitfield");
                loop {
                    upload.transmit_update().await?;
                }
            };
            if let Err(e) = result {
                info!("peer transmit stopped with {e:#?}");
            }
        });

        let mut download = PeerConnectionDownload {
            peer_connection: peer_connection.clone(),
            read,
            to_send: sender,
            choked: false,
            verify_task: None,
            downloading_piece: None,
            last_block_receive: Instant::now(),
        };

        let download_handle = spawn(async move {
            let result: anyhow::Result<()> = try {
                loop {
                    download.receive_message_retry().await?;
                }
            };
            info!("peer disconnected with: {:#?}", result);
            if let Err(e) = result {
                info!("{:#?}", e.backtrace())
            }
            download.peer_connection.torrent.remove_peer(&download.peer_connection.addr).await;
        });
        Ok((
            peer_connection,
            PeerConnectionTasks {
                rx_task: download_handle,
                tx_task: send_handle,
            }
        ))
    }

    //drop downloading state
    pub async fn drop_download_piece(&self, piece: u32) {
        self.torrent.to_download_map.write().await.set(piece as usize, false);
    }

    //tell that it's choked now
    pub async fn choked_download_piece(&self, piece: u32) {
        //self.torrent.to_download_map.write().await.set(piece as usize, false);
    }

    //tell that it's choked now
    pub async fn unchoked_download_piece(&self, piece: u32) {
        //self.torrent.to_download_map.write().await.set(piece as usize, false);
    }

    //flag as downloading and return number
    pub async fn get_download_piece(&self) -> Option<u32> {
        //select piece to download, better be unique among peers connected(TODO)
        let (status, mut torrent_map) = join!(self.status.lock(), self.torrent.to_download_map.write());
        //println!("status: {:#?}", status.map);
        //println!("torrent_map: {:#?}", torrent_map);
        status.map.iter().zip(torrent_map.iter()).enumerate().filter(|(_, (x, y))| (x == &true) && (y == &false)).next().map(|(x, _)| x).map(|x| {
            torrent_map.set(x, true);
            x as u32
        })
    }

    //flag as downloading and return number
    pub async fn get_download_piece_with_idx(&self, idx: usize) -> bool {
        let mut map = self.torrent.to_download_map.write().await;
        let r = if let Some(mut a) = map.get_mut(idx) {
            if a == &true {
                false
            } else {
                a.set(false);
                true
            }
        } else {
            false
        };
        r
    }

    pub async fn drop_download_piece_with_idx(&self, idx: usize) {
        //select piece to download, better be unique among peers connected(TODO)
        let mut map = self.torrent.to_download_map.write().await;
        if let Some(mut a) = map.get_mut(idx) {
            a.set(false);
        };
    }
}
impl PeerConnectionDownload {

    async fn request(&mut self, count: usize, retry: bool) {
        if let Some((idx, piece, start)) = self.downloading_piece.as_mut() {
            if retry {
                *start = 0;
            }
            let (requests, new_start) = piece.generate_requests(&self.peer_connection.torrent, count, *start);
            *start = new_start;
            for x in requests {
                self.peer_connection.internal_channel.send(InternalMessage::RequestBlock {piece: *idx, block: x}).await.ok();
            }
        }
    }

    async fn set_piece(&mut self, idx: u32) {
        let files = self.peer_connection.torrent.new_piece(idx).unwrap();
        let piece_writer = PieceWriter::new(files, &self.peer_connection.torrent);
        self.downloading_piece = Some((idx, piece_writer, 0));
        if self.choked {
            self.peer_connection.choked_download_piece(idx).await;
        } else {
            self.request(16, false).await;
        }
    }

    pub async fn receive_message_retry(&mut self) -> anyhow::Result<()> {
        let sleep_until = self.last_block_receive + Duration::from_millis(2000);
        let dummy = [0u8; 4];//for reading size
        select! {
            _ = tokio::time::sleep_until(sleep_until) => {
                //been a while since last received piece
                self.last_block_receive = Instant::now();
                if !self.choked {
                    self.request(16, true).await;
                }
            },
            r = self.read.readable() => {
                r?;
                self.receive_message().await?;
            }
        }
        Ok(())
    }

    pub async fn receive_message(&mut self) -> anyhow::Result<()> {
        let mut to_read = self.read.read_u32().await? as usize;
        if to_read == 0 { // keep-alive
            info!("received keep-alive");
            return Ok(());
        }
        to_read -= 1;
        match Message::try_from_primitive(self.read.read_u8().await?)? {
            Message::Choke => {
                info!("choked");
                ensure!(to_read == 0);
                if let Some(p) = self.downloading_piece.as_ref() {
                    self.peer_connection.unchoked_download_piece(p.0).await;
                }
                self.choked = true;
            },
            Message::UnChoke => {
                info!("unchoked");
                ensure!(to_read == 0);
                self.choked = false;
                self.request(16, false).await;
            }
            Message::Interested => {
                ensure!(to_read == 0);
                self.peer_connection.interested.store(true, Ordering::Relaxed);
            }
            Message::NotInterested => {
                ensure!(to_read == 0);
                self.peer_connection.interested.store(false, Ordering::Relaxed);
            }
            Message::Have => {
                ensure!(to_read == 4);
                to_read -= 4;
                let piece_idx = self.read.read_u32().await?;
                self.peer_connection.status.lock().await.map.set(piece_idx as usize, true);
                if self.downloading_piece.is_none() {
                    if self.peer_connection.get_download_piece_with_idx(piece_idx as usize).await {
                        self.set_piece(piece_idx).await;
                    }
                }
            }
            Message::BitField => {
                info!("start receiving bitfield");
                {
                    let mut status = self.peer_connection.status.lock().await;
                    let mut raw = status.map.as_raw_mut_slice();
                    if raw.len() != to_read {
                        bail!("received bitfield length doesn't match the torrent");
                    }
                    self.read.read_exact(raw).await?;
                    to_read -= raw.len();
                    dbg!(&status.map);
                    while to_read != 0 {
                        let mut buffer = [0u8; 64];
                        self.read.read_exact(&mut buffer[..min(64, to_read)]).await?;
                    }
                }
                info!("done receiving bitfield");
                if self.downloading_piece.is_none() {
                    if let Some(to_download) = self.peer_connection.get_download_piece().await {
                        self.set_piece(to_download).await;
                    }
                }
            }
            Message::Request => {
                ensure!(to_read == 12);
                to_read -= 12;
                let piece_idx = self.read.read_u32().await?;
                let begin = self.read.read_u32().await?;
                let length = self.read.read_u32().await?;
                if let Some(nonzero) = NonZeroU32::new(length) {
                    if self.to_send.try_send((piece_idx, Block {
                        begin,
                        len: nonzero,
                    })).is_err() {
                        self.peer_connection.internal_channel.send(InternalMessage::Choked).await?
                    }
                }
            }
            Message::Piece => {
                ensure!(to_read >= 8);
                to_read -= 8;
                let piece_idx = self.read.read_u32().await?;
                let begin = self.read.read_u32().await?;
                let length = to_read as u32;
                self.last_block_receive = Instant::now();
                if self.downloading_piece.as_ref().map(|p| p.0) == Some(piece_idx) {
                    let full = self.downloading_piece.as_mut().unwrap().1.write_block(&self.peer_connection.torrent, begin, &mut self.read, length).await?;
                    if full {
                        let mut piece = self.downloading_piece.take().unwrap();
                        let peer = self.peer_connection.clone();
                        if let Some(t) = self.verify_task.take() {
                            t.await?; //wait till last verification finish
                        }
                        let joinhandle = spawn(async move { let result: anyhow::Result<()> = try {
                            println!("try verification");
                            if piece.1.hash(&peer.torrent).await? == peer.torrent.hashes[piece.0 as usize] {
                                peer.status.lock().await.download_speed.update(1);
                                peer.torrent.map.write().await.set(piece.0 as usize, true);
                                peer.torrent.have(piece.0).await;
                            } else {
                                //unlikely to happen
                                //queue for redownload
                                println!("verification failed");
                                peer.torrent.to_download_map.write().await.set(piece.0 as usize, false);
                            }
                        }; result.map_err(|e| {
                            //error!("{e:#?}");
                            //error!("{}", e.backtrace());
                        }).ok(); });
                        self.verify_task = Some(joinhandle);
                        if let Some(to_download) = self.peer_connection.get_download_piece().await {
                            info!("request new piece");
                            self.set_piece(to_download).await;
                        }
                    } else {
                        self.request(1, false).await;
                    }
                } else {
                    while to_read != 0 {
                        let mut buffer = [0u8; 1024];
                        let limit = min(buffer.len(), to_read);
                        to_read -= self.read.read(&mut buffer[..limit]).await?;
                    }
                }
            }
        };
        Ok(())
    }

}

impl PeerConnectionUpload {
    pub async fn send_handshake(&mut self) -> anyhow::Result<()> {
        let pstr = b"BitTorrent protocol";
        self.write.write_u8(pstr.len() as u8).await?;
        self.write.write_all(pstr).await?;
        self.write.write_all(&[0u8; 8]).await?;
        self.write.write_all(&self.peer_connection.torrent.hash.v).await?;
        self.write.write_all(&self.peer_connection.torrent.client.peer_id.v).await?;
        Ok(())
    }

    // lot more things to consider, choked, speed limit just return false for testing download
    async fn should_upload(&mut self) -> anyhow::Result<bool> {
        Ok(true)
    }

    pub async fn send_bitfield(&mut self) -> anyhow::Result<()> {
        let map = self.peer_connection.torrent.map.read().await;
        let map_slice = map.as_raw_slice();
        self.write.write_u32(1 + (map_slice.len() as u32)).await?;
        self.write.write_u8(Message::BitField as u8).await?;
        self.write.write_all(&map_slice).await?;
        Ok(())
    }

    pub async fn choke(&mut self) -> anyhow::Result<()> {
        self.choked_upload = true;
        self.write.write_u32(1).await?;
        self.write.write_u8(Message::Choke as u8).await?;
        Ok(())
    }

    pub async fn unchoke(&mut self) -> anyhow::Result<()> {
        self.choked_upload = false;
        self.write.write_u32(1).await?;
        self.write.write_u8(Message::UnChoke as u8).await?;
        Ok(())
    }

    pub async fn transmit_update(&mut self) -> anyhow::Result<usize> {
        select! {
            opt = self.internal_channel.recv() => { if opt.is_none() { bail!("rx stopped"); } match opt.unwrap() {
                InternalMessage::Interested => {
                    info!("sending interested");
                    self.write.write_u32(1).await?;
                    self.write.write_u8(Message::Interested as u8).await?;
                },
                InternalMessage::BitField => {},
                InternalMessage::RequestBlock {
                    piece: idx,
                    block
                } => {
                    //request new block
                    self.write.write_u32(13).await?;
                    self.write.write_u8(Message::Request as u8).await?;
                    self.write.write_u32(idx).await?;
                    self.write.write_u32(block.begin).await?;
                    self.write.write_u32(block.len.get()).await?;
                },
                InternalMessage::Have {
                    piece
                } => {
                    self.write.write_u32(5).await;
                    self.write.write_u8(Message::Have as u8).await;
                    self.write.write_u32(piece).await;
                    info!("sent have");
                },
                InternalMessage::Choked => {
                    self.choke().await?;
                },
                InternalMessage::Unchoked => {

                }
            }},
            opt = self.to_send.recv() => {
                if opt.is_none() {
                    bail!("rx stopped");
                }
                let (idx, block) = opt.unwrap();
                if !self.uploading_piece.as_ref().is_some_and(|x| x.0 == idx) {
                    self.uploading_piece = self.peer_connection.torrent.new_piece(idx).map(|x| (idx, x))
                }

                if let Some((_, piece_cached)) = self.uploading_piece.as_mut() {
                    let len = 9 + block.len.get();
                    self.write.write_u32(len).await?;
                    self.write.write_u8(Message::Piece as u8).await?;
                    self.write.write_u32(idx).await?;
                    self.write.write_u32(block.begin).await?;
                    let read = piece_cached.read(&*self.peer_connection.torrent, block.begin, block.len.get(), &mut self.write).await?;
                    self.peer_connection.status.lock().await.upload_speed.update(len as u64);
                    return Ok(len as usize)
                } else {
                    bail!("request out of range");
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(60000)) => {
                self.write.write_u32(0).await?; // keep-alive
            },
            _ = async {
                if !self.choked_upload {
                    std::future::pending::<()>().await;
                }
            } => {
                self.unchoke().await?;
            }
        }
        Ok(0)
    }
}


pub async fn receive_handshake(read: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<Hash20> {
    let pstrlen = read.read_u8().await?;
    let mut pstr = vec![0; pstrlen as usize];
    read.read_exact(&mut pstr).await?; //TODO use data
    let mut reserved = [0u8; 8];
    read.read_exact(&mut reserved).await?;
    let mut info_hash = [0u8; 20];
    read.read_exact(&mut info_hash).await?;
    let mut peer_id = [0u8; 20];
    read.read_exact(&mut peer_id).await?;
    Ok(Hash20 {
        v: info_hash
    })
}