use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::ops::AddAssign;
use std::path::PathBuf;
use anyhow::bail;
use bitvec::bitbox;
use bitvec::macros::internal::funty::Fundamental;
use tokio::sync::RwLock;
use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use crate::client::Client;
use crate::peer::{InternalMessage, PeerConnection};
use crate::piece::{PieceFiles};
use crate::types::*;
use derive_more::{Deref, DerefMut};
use itertools::Itertools;

pub use lava_torrent::torrent::v1::Torrent as TorrentFile;
use tokio::fs::OpenOptions;
use tokio_util::bytes::Buf;

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
    pub connected_peers: Mutex<HashMap<SocketAddr, IoRc<PeerConnection>>>,
    pub piece_size: PieceSize,
    pub last_piece_size: u32, //precalculated
    pub hashes: Vec<Hash20>,
    pub map: RwLock<BitBox<u8, Msb0>>,
    pub to_download_map: RwLock<BitBox<u8, Msb0>>,
    pub files: Vec<(u64, PathBuf, Mutex<fs::File>)>
}

impl Torrent {
    pub async fn new_from_file(client: &IoRc<Client>, file: TorrentFile, path: PathBuf) -> anyhow::Result<()> {
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
            client: client.clone(),
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
            map: RwLock::new(bitbox![u8, Msb0; 0u8; pieces]),
            to_download_map: RwLock::new(bitbox![u8, Msb0; 0u8; pieces]),
            files: files_mapped,
        };
        let handle = TorrentHandle {
            inner: IoRc::new(torrent),
        };
        println!("hash: {hash:#?}");
        client.torrents.lock().await.insert(hash, handle);
        Ok(())
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
            x.1.internal_channel.send(InternalMessage::Have {
                piece: idx
            }).await.unwrap();
        }
    }

    pub async fn remove_peer(&self, addr: &SocketAddr) {
        self.connected_peers.lock().await.remove(&addr);
        self.client.peer_left.lock().await.add_assign(1);
    }
}