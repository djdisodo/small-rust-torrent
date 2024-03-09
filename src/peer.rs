use std::cmp::min;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use anyhow::{bail, ensure};
use log::{debug, error, info, trace};
use num_enum::TryFromPrimitive;
use portable_atomic::{AtomicBool, AtomicU16};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::{join, select};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use crate::handshake::HandShake;
use crate::message::Message;
use crate::piece::{Block, PieceFiles, PieceWriter};
use crate::speed_estimator::SpeedEstimator;
use crate::torrent::{Torrent};
use crate::types::*;

pub struct PeerConnectionTasks {
    pub rx_task: JoinHandle<()>,
    pub tx_task: JoinHandle<()>
}

impl Drop for PeerConnectionTasks {
    fn drop(&mut self) {
        self.rx_task.abort();
        self.tx_task.abort();
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum InternalMessage {
    Interested,
    BitField,
    Choked,
    Unchoked,
    Have {
        piece: u32
    }
}

pub struct PieceRequest {
    piece: u32,
    block: Block
}

pub struct PeerConnection {
    pub addr: SocketAddr,
    pub torrent: Rc<Torrent>,
    pub status: Mutex<PeerStatus>,
    pub interested: AtomicBool,
    pub internal_channel: Sender<InternalMessage>,
    pub request_channel: Sender<PieceRequest>,
    pub request_pacing: AtomicU16,
    pub peer_id: Hash20,
}


pub struct PeerStatus {
    pub bitfield: BitField,
    pub download_speed: SpeedEstimator<usize, 2>,
    pub upload_speed: SpeedEstimator<u64, 4>
}
pub struct PeerConnectionRx {
    pub peer_connection: IoRc<PeerConnection>,
    pub read: OwnedReadHalf,
    pub to_send: Sender<(u32, Block)>,
    pub choked: bool,
    pub verify_task: Option<JoinHandle<()>>,
    pub downloading_piece: Option<(u32, PieceWriter, usize)>,
    pub last_block_receive: Instant
}

pub struct PeerConnectionTx {
    pub peer_connection: IoRc<PeerConnection>,
    pub write: OwnedWriteHalf,
    pub to_send: Receiver<(u32, Block)>,
    pub internal_channel: Receiver<InternalMessage>,
    pub choked_upload: bool,
    pub uploading_piece: Option<(u32, PieceFiles)>,
    pub request_channel: Receiver<PieceRequest>,
    pub next_request: Instant,
}

impl PeerConnection {
    pub async fn connect(addr: SocketAddr, torrent: IoRc<Torrent>, socket: TcpStream, incoming: Option<HandShake>) -> anyhow::Result<(IoRc<PeerConnection>, PeerConnectionTasks)> {
        let (mut read, mut write) = socket.into_split();
        let (sender, receiver) = channel(torrent.client.config.max_pending_per_peer);
        let (internal_channel_send, internal_channel_recv) = channel(1);
        let (piece_request_cannel_send, piece_request_channel_receive) = channel(16);

        (HandShake {
            info_hash: torrent.hash,
            peer_id: torrent.client.config.peer_id,
        }).write_to(&mut write).await?;
        let handshake = match incoming {
            None => HandShake::read_from(&mut read).await?,
            Some(handshake) => handshake
        };
        info!("{addr:#?}({}) handshake success", handshake.peer_id);

        if handshake.info_hash != torrent.hash {
            bail!("handshake mismatch");
        }

        let status = PeerStatus {
            bitfield: BitField::new(torrent.hashes.len()),
            download_speed: Default::default(),
            upload_speed: Default::default(),
        };

        let peer_connection = IoRc::new(PeerConnection {
            addr,
            torrent,
            status: Mutex::new(status),
            interested: AtomicBool::new(false),
            internal_channel: internal_channel_send,
            request_channel: piece_request_cannel_send,
            request_pacing: Default::default(),
            peer_id: handshake.peer_id,
        });

        let mut upload = PeerConnectionTx {
            peer_connection: peer_connection.clone(),
            write,
            to_send: receiver,
            internal_channel: internal_channel_recv,
            choked_upload: true,
            uploading_piece: None,
            request_channel: piece_request_channel_receive,
            next_request: Instant::now(),
        };

        let send_handle = spawn(async move {
            trace!("{:#?}({}) tx task started", upload.peer_connection.addr, upload.peer_connection.peer_id);
            let result: anyhow::Result<()> = try {
                upload.bitfield().await?;
                upload.unchoke().await?;
                loop {
                    upload.tx_update().await?;
                }
            };
            if let Err(e) = result {
                info!("peer transmit stopped with {e:#?}");
            }
        });

        debug!("{addr:#?}({}) tx task spawned", peer_connection.peer_id);

        let mut download = PeerConnectionRx {
            peer_connection: peer_connection.clone(),
            read,
            to_send: sender,
            choked: false,
            verify_task: None,
            downloading_piece: None,
            last_block_receive: Instant::now(),
        };

        let download_handle = spawn(async move {
            trace!("{:#?}({}) rx task started", download.peer_connection.addr, download.peer_connection.peer_id);
            let result: anyhow::Result<()> = try {
                loop {
                    download.receive_message_retry().await?;
                }
            };
            info!("peer disconnected with: {:#?}", result);
            if let Err(e) = result {
                info!("{:#?}", e.backtrace())
            }
            if let Some((idx, _, _)) = download.downloading_piece.as_ref() {
                download.peer_connection.drop_download_piece(*idx).await;
            }
            download.peer_connection.torrent.disconnect_peer(&download.peer_connection.addr).await;
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
        self.torrent.to_download_bitfield.write().await.set(piece as usize, false);
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
        let (status, mut torrent_map) = join!(self.status.lock(), self.torrent.to_download_bitfield.write());
        //println!("status: {:#?}", status.map);
        //println!("torrent_map: {:#?}", torrent_map);
        status.bitfield.iter().zip(torrent_map.iter()).enumerate().filter(|(_, (x, y))| (x == &true) && (y == &false)).next().map(|(x, _)| x).map(|x| {
            torrent_map.set(x, true);
            x as u32
        })
    }

    //flag as downloading and return number
    pub async fn get_download_piece_with_idx(&self, idx: usize) -> bool {
        let mut map = self.torrent.to_download_bitfield.write().await;
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
        let mut map = self.torrent.to_download_bitfield.write().await;
        if let Some(mut a) = map.get_mut(idx) {
            a.set(false);
        };
    }

    pub async fn choke_tx(&self) {
        self.internal_channel.send(InternalMessage::Choked).await.ok();
    }
}
impl PeerConnectionRx {

    async fn queue_request(&mut self, count: usize, retry: bool) {
        if let Some((idx, piece, start)) = self.downloading_piece.as_mut() {
            if retry {
                *start = 0;
            }
            let (requests, new_start) = piece.generate_requests(&self.peer_connection.torrent, count, *start);
            *start = new_start;
            for x in requests {
                self.peer_connection.request_channel.send(PieceRequest {piece: *idx, block: x}).await.ok();
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
            self.queue_request(16, false).await;
        }
    }

    async fn choke(&mut self) {
        trace!("{:#?}({}) rx choked", self.peer_connection.addr, self.peer_connection.peer_id);
        if let Some(p) = self.downloading_piece.as_ref() {
            self.peer_connection.choked_download_piece(p.0).await;
        }
        self.choked = true;
    }

    async fn unchoke(&mut self) {
        trace!("{:#?}({}) rx unchoked", self.peer_connection.addr, self.peer_connection.peer_id);
        if let Some(p) = self.downloading_piece.as_ref() {
            self.peer_connection.unchoked_download_piece(p.0).await;
        }
        self.choked = false;
        self.queue_request(16, true).await;
    }

    fn interested(&self) {
        trace!("{:#?}({}) rx interested", self.peer_connection.addr, self.peer_connection.peer_id);
        self.peer_connection.interested.store(true, Ordering::Relaxed);
    }

    fn notinterested(&self) {
        trace!("{:#?}({}) rx not interested", self.peer_connection.addr, self.peer_connection.peer_id);
        self.peer_connection.interested.store(false, Ordering::Relaxed);
    }

    async fn have(&mut self) -> anyhow::Result<()> {
        let piece_idx = self.read.read_u32().await?;
        self.peer_connection.status.lock().await.bitfield.set(piece_idx as usize, true);
        if self.downloading_piece.is_none() {
            if self.peer_connection.get_download_piece_with_idx(piece_idx as usize).await {
                self.set_piece(piece_idx).await;
            }
        }
        Ok(())
    }

    async fn bitfield(&mut self, to_read: &mut usize) -> anyhow::Result<()> {
        let (all, ones) = {
            let mut status = self.peer_connection.status.lock().await;
            let raw = status.bitfield.as_raw_mut_slice();
            if raw.len() != *to_read {
                bail!("received bitfield length doesn't match the torrent");
            }
            self.read.read_exact(raw).await?;
            *to_read -= raw.len();
            while *to_read != 0 {
                let mut buffer = [0u8; 64];
                self.read.read_exact(&mut buffer[..min(64, *to_read)]).await?;
            }
            (status.bitfield.len(), status.bitfield.count_ones())
        };
        trace!("{:#?}({}) received bitfield {ones}/{all}", self.peer_connection.addr, self.peer_connection.peer_id);
        if self.downloading_piece.is_none() {
            if let Some(to_download) = self.peer_connection.get_download_piece().await {
                self.set_piece(to_download).await;
            }
        }
        Ok(())
    }

    async fn request(&mut self) -> anyhow::Result<()> {
        let piece_idx = self.read.read_u32().await?;
        let begin = self.read.read_u32().await?;
        let length = self.read.read_u32().await?;
        if let Some(nonzero) = NonZeroU32::new(length) {
            if self.to_send.try_send((piece_idx, Block {
                begin,
                len: nonzero,
            })).is_err() {
                self.peer_connection.choke_tx().await;
            }
        }
        Ok(())
    }

    async fn piece(&mut self, to_read: &mut usize) -> anyhow::Result<()> {
        ensure!(*to_read >= 8);
        *to_read -= 8;
        let piece_idx = self.read.read_u32().await?;
        let begin = self.read.read_u32().await?;

        let length = *to_read as u32;
        if self.downloading_piece.as_ref().map(|p| p.0) == Some(piece_idx) {
            let full = self.downloading_piece.as_mut().unwrap().1.write_block(&self.peer_connection.torrent, begin, &mut self.read, length).await?;
            if full {
                if let Some(t) = self.verify_task.take() {
                    t.await?; //wait till last verification finish
                }

                let mut piece = self.downloading_piece.take().unwrap();
                let peer = self.peer_connection.clone();

                if !piece.1.hash_done(&self.peer_connection.torrent) {
                    self.peer_connection.request_pacing.add(self.peer_connection.torrent.client.config.request_pacing_step_up, Ordering::SeqCst);
                    println!("request delay incrased to {}", self.peer_connection.request_pacing.load(Ordering::SeqCst));
                } else {
                    let mut pacing = self.peer_connection.request_pacing.load(Ordering::Relaxed);
                    if self.peer_connection.torrent.client.config.request_pacing_step_down < pacing {
                        pacing -= self.peer_connection.torrent.client.config.request_pacing_step_down;
                    } else {
                        pacing = 0;
                    }
                    self.peer_connection.request_pacing.store(pacing, Ordering::SeqCst);
                }

                let joinhandle = spawn(async move { let result: anyhow::Result<()> = try {
                    println!("try verification");
                    if piece.1.hash(&peer.torrent).await? == peer.torrent.hashes[piece.0 as usize] {
                        peer.status.lock().await.download_speed.update(1);
                        peer.torrent.bitfield.write().await.set(piece.0 as usize, true);
                        peer.torrent.have(piece.0).await;
                    } else {
                        //unlikely to happen
                        //queue for redownload
                        println!("verification failed");
                        peer.torrent.to_download_bitfield.write().await.set(piece.0 as usize, false);
                    }
                }; result.map_err(|e| {
                    error!("Error while piece verification {e:#?}");
                    //error!("{}", e.backtrace());
                }).ok(); });
                self.verify_task = Some(joinhandle);
                if let Some(to_download) = self.peer_connection.get_download_piece().await {
                    info!("request new piece");
                    self.set_piece(to_download).await;
                }
            } else {
                self.queue_request(1, false).await;
            }
        } else {
            while *to_read != 0 {
                let mut buffer = [0u8; 1024];
                let limit = min(buffer.len(), *to_read);
                *to_read -= self.read.read(&mut buffer[..limit]).await?;
            }
        }
        *to_read = 0;
        self.last_block_receive = Instant::now();
        Ok(())
    }





    pub async fn receive_message_retry(&mut self) -> anyhow::Result<()> {
        let sleep_until = self.last_block_receive + Duration::from_millis(2000);
        select! {
            _ = tokio::time::sleep_until(sleep_until) => {
                //been a while since last received piece
                self.last_block_receive = Instant::now();
                if !self.choked {
                    self.queue_request(16, true).await;
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
        let sleep_until = self.last_block_receive + Duration::from_millis(2000);
        select! {
            _ = tokio::time::sleep_until(sleep_until) => {
                //been a while since last received piece
                self.last_block_receive = Instant::now();
                if !self.choked {
                    self.queue_request(16, true).await;
                }
            },
            r = self.read.readable() => {
                r?;
                let mut to_read = self.read.read_u32().await? as usize;
                if to_read == 0 { // keep-alive
                    info!("received keep-alive");
                    return Ok(());
                }
                to_read -= 1;
                match Message::try_from_primitive(self.read.read_u8().await?)? {
                    Message::Choke => {
                        self.choke().await;
                    },
                    Message::UnChoke => {
                        self.unchoke().await;
                    }
                    Message::Interested => {
                        self.interested();
                    }
                    Message::NotInterested => {
                        self.notinterested();
                    }
                    Message::Have => {
                        ensure!(to_read == 4, "have message has incorrect length");
                        self.have().await?;
                        to_read -= 4;
                    }
                    Message::BitField => {
                        self.bitfield(&mut to_read).await?;
                    }
                    Message::Request => {
                        ensure!(to_read == 12);
                        self.request().await?;
                        to_read -= 12;
                    }
                    Message::Piece => {
                        self.piece(&mut to_read).await?;
                    }
                };
                ensure!(to_read == 0, "unexpected data left after reading message");
            }
        }
        Ok(())

    }

}

impl PeerConnectionTx {

    // lot more things to consider, choked, speed limit just return false for testing download
    async fn should_upload(&mut self) -> anyhow::Result<bool> {
        Ok(true)
    }

    pub async fn bitfield(&mut self) -> anyhow::Result<()> {
        let map = self.peer_connection.torrent.bitfield.read().await;
        let map_slice = map.as_raw_slice();
        self.write.write_u32(1 + (map_slice.len() as u32)).await?;
        self.write.write_u8(Message::BitField as u8).await?;
        self.write.write_all(&map_slice).await?;
        trace!("{:#?}({}) sent bitfield", self.peer_connection.addr, self.peer_connection.peer_id);
        Ok(())
    }

    pub async fn choke(&mut self) -> anyhow::Result<()> {
        self.choked_upload = true;
        self.write.write_u32(1).await?;
        self.write.write_u8(Message::Choke as u8).await?;
        trace!("{:#?}({}) tx choked", self.peer_connection.addr, self.peer_connection.peer_id);
        Ok(())
    }

    pub async fn unchoke(&mut self) -> anyhow::Result<()> {
        self.choked_upload = false;
        self.write.write_u32(1).await?;
        self.write.write_u8(Message::UnChoke as u8).await?;
        trace!("{:#?}({}) tx unchoked", self.peer_connection.addr, self.peer_connection.peer_id);
        Ok(())
    }



    async fn internal_message(&mut self, message: InternalMessage) -> anyhow::Result<()> {
        match message {
            InternalMessage::Interested => {
                self.write.write_u32(1).await?;
                self.write.write_u8(Message::Interested as u8).await?;
            },
            InternalMessage::BitField => {},
            InternalMessage::Have {
                piece
            } => {
                self.write.write_u32(5).await?;
                self.write.write_u8(Message::Have as u8).await?;
                self.write.write_u32(piece).await?;
            },
            InternalMessage::Choked => {
                self.choke().await?;
            },
            InternalMessage::Unchoked => {

            }
        }
        Ok(())
    }

    async fn request(&mut self, PieceRequest {
        piece: idx,
        block
    }: PieceRequest) -> anyhow::Result<()> {
        self.write.write_u32(13).await?;
        self.write.write_u8(Message::Request as u8).await?;
        self.write.write_u32(idx).await?;
        self.write.write_u32(block.begin).await?;
        self.write.write_u32(block.len.get()).await?;
        self.next_request = Instant::now() + Duration::from_millis((self.peer_connection.request_pacing.load(Ordering::SeqCst) / 10) as _);
        Ok(())
    }

    async fn piece(&mut self, idx: u32, block: Block) -> anyhow::Result<()> {
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
            ensure!(block.len.get() == read as u32, "unknown error while transmitting piece");
            self.peer_connection.status.lock().await.upload_speed.update(len as u64);
        } else {
            bail!("request out of range");
        }
        Ok(())
    }

    pub async fn tx_update(&mut self) -> anyhow::Result<()> {
        select! {
            opt = self.internal_channel.recv() => {
                if opt.is_none() { bail!("rx stopped"); }
                self.internal_message(opt.unwrap()).await?;
            },
            opt = async {
                tokio::time::sleep_until(self.next_request).await;
                self.request_channel.recv().await
            } => {
                if opt.is_none() {
                    bail!("rx stopped");
                }
                self.request(opt.unwrap()).await?;
            },
            opt = self.to_send.recv() => {
                if opt.is_none() {
                    bail!("rx stopped");
                }
                let (idx, block) = opt.unwrap();
                self.piece(idx, block).await?;
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
        Ok(())
    }
}