use std::io::Read;
use std::net::{SocketAddr};
use std::path::PathBuf;
use bitvec::macros::internal::funty::Fundamental;
use tokio::sync::RwLock;
use crate::client::Client;
use crate::peer::{InternalMessage, PeerConnection, PeerConnectionTasks};
use crate::piece::{PieceFiles};
use crate::types::*;
use derive_more::{Deref, DerefMut};

pub use lava_torrent::torrent::v1::Torrent as TorrentFile;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::bytes::Buf;
use crate::handshake::HandShake;

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
    pub files: Vec<(u64, PathBuf, Mutex<fs::File>)>
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



        let torrent = Torrent {
            hash,
            client: client,
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

    pub async fn connect_peer(self: &IoRc<Self>, addr: SocketAddr, sock: TcpStream, incoming: Option<HandShake>) -> anyhow::Result<()> {
        let con = PeerConnection::connect(addr, self.clone(), sock, incoming).await?;
        self.connected_peers.lock().await.insert(addr, con);
        Ok(())
    }

    pub async fn disconnect_peer(&self, addr: &SocketAddr) {
        if self.connected_peers.lock().await.remove(&addr).is_some() {
            self.client.drop_peer().await;
        }
    }

    pub async fn peer(&self, addr: &SocketAddr) -> Option<IoRc<PeerConnection>> {
        self.connected_peers.lock().await.get(&addr).map(|(x, _)| x.clone())
    }
}