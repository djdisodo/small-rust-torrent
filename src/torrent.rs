use std::cmp::min;
use std::collections::VecDeque;
use std::io::Read;
use std::net::{SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;
use bitvec::macros::internal::funty::Fundamental;
use tokio::sync::{OwnedSemaphorePermit, RwLock};
use crate::client::Client;
use crate::peer::{InternalMessage, PeerConnection, PeerConnectionTasks};
use crate::piece::{PieceFiles};
use crate::types::*;
use derive_more::{Deref, DerefMut};
use itertools::Itertools;

pub use lava_torrent::torrent::v1::Torrent as TorrentFile;
use lava_torrent::tracker::TrackerResponse;
use log::{debug, info};
use portable_atomic::AtomicU64;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::select;
use tokio::time::Instant;
use tokio_util::bytes::Buf;
use crate::handshake::HandShake;
use crate::speed_estimator::SpeedEstimator;
use crate::tracker::http_tracker;

#[derive(Deref, DerefMut)]
pub struct TorrentHandle {
    inner: IoRc<Torrent>
}

impl Drop for TorrentHandle {
    fn drop(&mut self) {
        loop {
            if let Ok(mut v) = self.connected_peers.try_lock() {
                v.clear();
                break;
            }
        }
    }
}

pub struct Torrent {
    pub hash: Hash20,
    pub client: IoRc<Client>,
    pub(crate) connected_peers: Mutex<HashMap<SocketAddr, (IoRc<PeerConnection>, PeerConnectionTasks)>>,
    pub piece_size: PieceSize,
    pub last_piece_size: u32, //precalculated
    pub hashes: Vec<Hash20>,
    pub(crate) bitfield: RwLock<BitField>,
    pub(crate) to_download_bitfield: RwLock<BitField>,
    pub files: Vec<(u64, PathBuf, Mutex<fs::File>)>,
    pub trackers: Mutex<Vec<Vec<(String, Instant)>>>,
    pub peers: Mutex<VecDeque<SocketAddr>>,
    pub uploaded: AtomicU64,
    pub(crate) down_speed: Mutex<SpeedEstimator<10>>,
}

impl Torrent {
    pub async fn load_status(&self, read: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<usize> {
        let mut bitfield = self.bitfield.write().await;
        let mut to_download_bitfield = self.to_download_bitfield.write().await;
        read.read_exact(bitfield.as_raw_mut_slice()).await?;
        read.read_exact(to_download_bitfield.as_raw_mut_slice()).await?;
        Ok(bitfield.as_raw_slice().len() + to_download_bitfield.as_raw_slice().len())
    }

    pub async fn save_status(&self, write: &mut (impl AsyncWrite + Unpin)) -> anyhow::Result<usize> {
        let bitfield = self.bitfield.read().await;
        let to_download_bitfield = self.to_download_bitfield.read().await;
        write.write_all(bitfield.as_raw_slice()).await?;
        write.write_all(to_download_bitfield.as_raw_slice()).await?;
        Ok(bitfield.as_raw_slice().len() + to_download_bitfield.as_raw_slice().len())
    }

    pub async fn save_files(&self) -> anyhow::Result<()> {
        let mut open_options = OpenOptions::new();
        open_options.create(true);
        open_options.write(true);
        open_options.read(true);
        for (_, path, file) in self.files.iter() {
            let mut file_lock = file.lock().await;
            file_lock.flush().await?;
            *file_lock = open_options.open(path).await?;
            break;
        }
        Ok(())
    }

    pub async fn dspeed(&self) -> u32 {
        self.down_speed.lock().await.estimated_speed()
    }

    pub async fn new_from_file(client: IoRc<Client>, file: TorrentFile, path: PathBuf) -> anyhow::Result<TorrentHandle> {
        let mut buf = [0u8; 20];
        file.info_hash_bytes().reader().read_exact(&mut buf)?;
        let hash = Hash20 {
            v: buf
        };
        let size: u64 = file.length.as_u64();
        let mut files_mapped;

        let mut open_options = OpenOptions::new();
        open_options.create(true);
        open_options.write(true);
        open_options.read(true);

        if let Some(files) = file.files {
            files_mapped = Vec::with_capacity(files.len());
            for x in files.into_iter() {
                let mut path = path.clone();
                path.push(&x.path);
                fs::create_dir_all(path.parent().unwrap()).await?;
                let file = Mutex::new(open_options.open(&path).await?);
                files_mapped.push((x.length.as_u64(), path, file))
            }
        } else {
            let mut path = path.clone();
            path.push(&file.name);
            files_mapped = Vec::with_capacity(1);
            fs::create_dir_all(path.parent().unwrap()).await?;
            let file_mutex = Mutex::new(open_options.open(&path).await?);
            files_mapped.push((file.length.as_u64(), path, file_mutex));
        }
        let piece_size = file.piece_length.as_u32();
        let pieces = file.pieces.len();

        let mut trackers;
        if let Some(announce) = file.announce_list {
            let now = Instant::now();
            trackers = announce.into_iter().map(|x| {
                let mut tier = x.into_iter().map(|x| (x, now)).collect_vec();
                tier.shuffle(&mut thread_rng());
                tier
            }).collect_vec();
        } else {
            trackers = vec![];
        }

        let peers = Mutex::new(VecDeque::with_capacity(client.config.request_peer_limit));


        let torrent = Torrent {
            hash,
            client,
            connected_peers: Default::default(),
            piece_size,
            last_piece_size: {
                let a = (size % (piece_size as u64)) as u32;
                if a == 0 {
                    piece_size
                } else {
                    a
                }
            },
            hashes: file.pieces.iter().map(|x| {
                let mut buf = [0u8; 20];
                x.reader().read_exact(&mut buf)?;
                Ok::<_, std::io::Error>(Hash20 {
                    v: buf
                })
            }).collect::<Result<Vec<_>, std::io::Error>>()?,
            bitfield: RwLock::new(BitField::new(pieces)),
            to_download_bitfield: RwLock::new(BitField::new(pieces)),
            files: files_mapped,
            trackers: Mutex::new(trackers),
            peers,
            uploaded: Default::default(),
            down_speed: Default::default(),
        };
        Ok(TorrentHandle {
            inner: IoRc::new(torrent),
        })
    }

    pub fn new_piece(&self, idx: u32) -> Option<PieceFiles> {
        let mut abs_pos = (idx as u64) * (self.piece_size as u64);
        let mut file_idx = 0;
        if let Some(size) = self.files.get(file_idx).map(|x| x.0) {
            if abs_pos < size {
                return Some(PieceFiles {
                    files: (file_idx as u32)..,
                    start: abs_pos,
                    last: idx == self.hashes.len() as u32 - 1
                });
            } else {
                abs_pos -= size;
            }
        } else {
            panic!();
        }

        loop {
            file_idx += 1;
            if let Some(size) = self.files.get(file_idx).map(|x| x.0) {
                if abs_pos < size {
                    return Some(PieceFiles {
                        files: (file_idx as u32)..,
                        start: abs_pos,
                        last: idx == self.hashes.len() as u32 - 1
                    });
                } else {
                    abs_pos -= size;
                }
            } else {
                panic!();
            }
        }
    }

    pub async fn have(&self, idx: u32) {
        let peers = self.connected_peers.lock().await;
        for x in peers.iter() {
            x.1.0.internal_channel.send(InternalMessage::Have {
                piece: idx
            }).await.unwrap();
        }
    }

    pub async fn connect_peer(self: &IoRc<Self>, addr: SocketAddr, sock: TcpStream, incoming: Option<HandShake>, perm: OwnedSemaphorePermit) -> anyhow::Result<()> {
        let con = PeerConnection::connect(addr, self.clone(), sock, incoming, perm).await?;
        self.connected_peers.lock().await.insert(addr, con);
        Ok(())
    }

    pub async fn disconnect_peer(&self, addr: &SocketAddr) {
        self.connected_peers.lock().await.remove(&addr);
    }

    pub async fn peer(&self, addr: &SocketAddr) -> Option<IoRc<PeerConnection>> {
        self.connected_peers.lock().await.get(&addr).map(|(x, _)| x.clone())
    }

    pub async fn update_announce(&self, force: bool) -> anyhow::Result<()> {
        let mut trackers = self.trackers.lock().await;
        for x in trackers.iter_mut() {
            for i in 0..x.len() {
                if Instant::now().checked_duration_since(x[i].1).is_some() || force {
                    select! {
                        tracker_response = http_tracker(self, &x[i].0, self.client.config.request_peer_limit) => {
                            let tracker_response: anyhow::Result<TrackerResponse> = tracker_response;
                            match tracker_response {
                                Ok(tracker_response) => match tracker_response {
                                    TrackerResponse::Success {
                                        interval,
                                        peers,
                                        warning: _,
                                        min_interval: _,
                                        tracker_id: _,
                                        complete: _,
                                        incomplete: _,
                                        extra_fields: _,
                                    } => {
                                        let mut torrent_peers = self.peers.lock().await;
                                        torrent_peers.clear();
                                        for peer in peers.into_iter() {
                                            torrent_peers.push_back(peer.addr);
                                        }
                                        debug!("received peers {:#?}", torrent_peers.deref());
                                        x[i].1 = Instant::now() + Duration::from_secs(interval.as_u64());
                                    },
                                    TrackerResponse::Failure {
                                        reason
                                    } => {
                                        info!("tracker {} query failed with reason: {}", x[i].0, reason);
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    info!("tracker {} query failed with error {:#?}", x[i].0, e);
                                    continue;
                                }
                            }
                        },
                        _ = tokio::time::sleep(Duration::from_secs(20)) => {
                            info!("tracker {} timed out", x[i].0);
                            continue;
                        }
                    }
                    (&mut trackers[..=i]).rotate_right(1);
                    return Ok(());
                } else {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    pub async fn downloaded(&self) -> u64 {
        let bitfield = self.bitfield.read().await;
        let mut bytes = bitfield.count_ones() as u64;
        bytes *= self.piece_size as u64;
        if bitfield.last().map(|x| x == &true).unwrap_or(false) {
            bytes -= self.piece_size as u64;
            bytes += self.last_piece_size as u64;
        }
        bytes
    }

    pub async fn needed(&self) -> u64 {
        let bitfield = self.bitfield.read().await;
        let mut bytes = bitfield.count_zeros() as u64;
        bytes *= self.piece_size as u64;
        if !bitfield.last().map(|x| x == &true).unwrap_or(false) {
            bytes -= self.piece_size as u64;
            bytes += self.last_piece_size as u64;
        }
        bytes
    }

    // pop peer before calling this
    pub(crate) async fn try_connect_peer(self: &IoRc<Self>, perm: OwnedSemaphorePermit) -> anyhow::Result<bool> {
        let peer = self.peers.lock().await.pop_front();
        if peer.is_none() {
            return Ok(false)
        }
        let peer = peer.unwrap();
        println!("trying to connect {}", peer);
        let connect: anyhow::Result<()> = select! {
                    result = async {
                        let sock = TcpStream::connect(peer).await?;
                        self.connect_peer(peer, sock, None, perm).await?;
                        anyhow::Result::<(), anyhow::Error>::Ok(())
                    } => result,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {
                        Err(anyhow::Error::msg("timeout"))
                    }
                };

        if let Err(e) = connect {
            self.disconnect_peer(&peer).await;
            debug!("{peer} error while connecting to peer: {e:#?}");
        }
        Ok(true)
    }

    pub fn piece_count(&self) -> usize {
        self.hashes.len()
    }

    pub async fn piece_done(&self) -> usize {
        self.bitfield.read().await.count_ones()
    }
}