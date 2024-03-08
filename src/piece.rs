use std::cmp::{max, min};
use std::io::{SeekFrom, Write};
use std::num::NonZeroU32;
use std::ops::RangeFrom;
use anyhow::bail;
use bitvec::bitbox;
use bitvec::boxed::BitBox;
use bitvec::macros::internal::funty::Fundamental;
use log::info;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::join;
use tokio::task::yield_now;
use tokio_util::io::SyncIoBridge;
use crate::torrent::Torrent;
use crate::types::*;

#[derive(Debug)]
pub struct PieceFiles {
    pub files: RangeFrom<u32>,
    pub start: u64,
    pub last: bool, // last piece may have different size
}

impl PieceFiles {
    // please suggest simpler algorithm
    pub async fn write(&mut self, t: &Torrent, pos: PieceSize, len: u32, data: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<()> {
        let piece_len = self.len(t);
        let mut start = self.start + pos as u64;
        let mut files = self.files.clone();
        let mut buffer = vec![0u8; 4096];
        let mut file_limit = 0;
        let mut left = len;
        let mut skip_bytes = 0;
        let mut _break = false;
        while let Some(file) = t.files.get(files.next().unwrap() as usize) && !_break {
            file_limit += file.0;
            if file_limit > self.start + piece_len as u64 {
                file_limit = self.start + piece_len as u64;
                _break = true;
            }
            if file_limit <= start {
                skip_bytes += file.0;
                continue
            }
            let mut to_read = min(4096, min((file_limit - start) as u32, left));
            while to_read != 0 {
                let read = data.read(&mut buffer[..(to_read as usize)]).await?;
                let mut file = file.2.lock().await;
                file.seek(SeekFrom::Start(start - skip_bytes)).await?;
                file.write_all(&buffer[..read]).await?;
                start += read as u64;
                left -= read as u32;
                to_read = min(4096, min((file_limit - start) as u32, left));
            }
            skip_bytes += file.0;
        }
        Ok(())
    }

    // please suggest simpler algorithm
    pub async fn write_hash(&mut self, t: &Torrent, pos: PieceSize, len: u32, data: &mut (impl AsyncRead + Unpin), hasher: &mut Sha1) -> anyhow::Result<()> {
        let piece_len = self.len(t);
        let mut start = self.start + pos as u64;
        let mut files = self.files.clone();
        let mut buffer = vec![0u8; 4096];
        let mut file_limit = 0;
        let mut left = len;
        let mut skip_bytes = 0;
        let mut _break = false;
        while let Some(file) = t.files.get(files.next().unwrap() as usize) && !_break {
            file_limit += file.0;
            if file_limit > self.start + piece_len as u64 {
                file_limit = self.start + piece_len as u64;
                _break = true;
            }
            if file_limit <= start {
                skip_bytes += file.0;
                continue
            }
            let mut to_read = min(4096, min((file_limit - start) as u32, left));
            while to_read != 0 {
                let read = data.read(&mut buffer[..(to_read as usize)]).await?;
                let mut file = file.2.lock().await;
                join!(
                    async {
                        file.seek(SeekFrom::Start(start - skip_bytes)).await?;
                        file.write_all(&buffer[..read]).await?;
                        anyhow::Result::<(), anyhow::Error>::Ok(())
                    },
                    async {
                        let mut buffer_ref = &buffer[..read];
                        let mut to_write = min(1024, buffer_ref.len());
                        while to_write != 0 {
                            hasher.update(&buffer_ref[..to_write]);
                            buffer_ref = &buffer_ref[to_write..];
                            to_write = min(1024, buffer_ref.len());
                            yield_now().await;
                        }
                    }
                ).0?;

                start += read as u64;
                left -= read as u32;
                to_read = min(4096, min((file_limit - start) as u32, left));
            }
            skip_bytes += file.0;
        }
        Ok(())
    }

    pub async fn read(&mut self, t: &Torrent, pos: u32, len: u32, target: &mut (impl AsyncWrite + Unpin)) -> anyhow::Result<usize> { //todo range check
        let piece_len = self.len(t);
        let mut start = self.start + pos as u64;
        let mut files = self.files.clone();
        let mut buffer = vec![0u8; 4096];
        let mut file_limit = 0;
        let mut left = len;
        let mut skip_bytes = 0;
        while let Some(file) = t.files.get(files.next().unwrap() as usize) {
            file_limit += file.0;
            file_limit = min(self.start + piece_len as u64, file_limit);
            if file_limit <= start {
                skip_bytes += file.0;
                continue;
            }
            let mut to_read = min(4096, min((file_limit - start) as u32, left));
            while to_read != 0 {
                {
                    let mut file = file.2.lock().await;
                    file.seek(SeekFrom::Start(start - skip_bytes)).await?;
                    file.read_exact(&mut buffer[..(to_read as usize)]).await?;
                } //early drop
                target.write_all(&buffer[..(to_read as usize)]).await?;
                start += to_read as u64;
                left -= to_read;
                to_read = min(4096, min((file_limit - start) as u32, left));
            }
            skip_bytes += file.0;
        }
        Ok((len - left) as usize)
    }

    pub async fn read_sync(&mut self, t: &Torrent, pos: u32, len: u32, target: &mut impl Write) -> anyhow::Result<usize> { //todo range check
        let piece_len = self.len(t);
        let mut start = self.start + pos as u64;
        let mut files = self.files.clone();
        let mut buffer = vec![0u8; 4096];
        let mut file_limit = 0;
        let mut left = len;
        let mut skip_bytes = 0;
        while let Some(file) = t.files.get(files.next().unwrap() as usize) {
            file_limit += file.0;
            file_limit = min(self.start + piece_len as u64, file_limit);
            if file_limit <= start {
                skip_bytes += file.0;
                continue;
            }
            let mut to_read = min(4096, min((file_limit - start) as u32, left));
            while to_read != 0 {
                {
                    let mut file = file.2.lock().await;
                    file.seek(SeekFrom::Start(start - skip_bytes)).await?;
                    file.read_exact(&mut buffer[..(to_read as usize)]).await?;
                } //early drop
                target.write_all(&buffer[..(to_read as usize)])?;
                start += to_read as u64;
                left -= to_read;
                to_read = min(4096, min((file_limit - start) as u32, left));
            }
            skip_bytes += file.0;
        }
        Ok((len - left) as usize)
    }

    pub fn len(&self, torrent: &Torrent) -> u32 {
        if self.last {
            torrent.last_piece_size
        } else {
            torrent.piece_size
        }
    }

    // we need to make this non blocking, so we yield at every 64 byte
    //todo replace read_sync
    pub async fn hash(&mut self, torrent: &Torrent) -> anyhow::Result<Hash20> {
        let mut buffer = [0u8; 1024];
        let mut position = 0;
        let mut sha1 = Sha1::new();
        let mut read = self.read_sync(torrent, position, 1024, &mut &mut buffer[0..]).await?;
        position += read as u32;
        sha1.update(&buffer[..read]);
        while read == 1024 {
            read = self.read_sync(torrent, position, 1024, &mut &mut buffer[0..]).await?;
            sha1.update(&buffer[..read]);
            position += read as u32;
        }
        Ok(Hash20 {
            v: sha1.digest().bytes()
        })
    }

}
pub struct PieceWriter<const MAP_BLOCK_SIZE: PieceSize = { 1 << 14 }> {
    pub files: PieceFiles,
    pub map: BitBox,
    pub hasher: Sha1,
    pub hasher_written: u32 // hash on fly (if ideal)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Block {
    pub(crate) begin: PieceSize,
    pub(crate) len: NonZeroU32 //niche
}

impl<const MAP_BLOCK_SIZE: PieceSize> PieceWriter<MAP_BLOCK_SIZE> {
    pub fn new(files: PieceFiles, torrent: &Torrent) -> Self {
        let size = files.len(torrent);
        Self {
            files,
            map: bitbox![0; size.div_ceil((MAP_BLOCK_SIZE)) as usize],
            hasher: Default::default(),
            hasher_written: 0,
        }
    }

    //use AsyncRead instead of buffer to reduce ram usage
    pub async fn write_block<T: AsyncRead + Unpin>(&mut self, torrent: &Torrent, offset: PieceSize, data: &mut T, len: u32) -> anyhow::Result<bool> {
        let start_block = offset.div_ceil(MAP_BLOCK_SIZE);
        if offset + len == 0 {
            return Ok(self.map.all());
        }
        let mut end_block = (offset + len - 1).div_floor(MAP_BLOCK_SIZE);
        let piece_len = self.files.len(torrent);
        if (offset + len) == piece_len {
            end_block = self.map.len() as u32 - 1;
        }
        if self.hasher_written == offset {
            self.files.write_hash(torrent, offset, len, data, &mut self.hasher).await?;
            self.hasher_written += len;
        } else {
            self.files.write(torrent, offset, len, data).await?;
        }
        for i in start_block..=end_block {
            self.map.set(i as usize, true);
        }

        Ok(self.map.all())
    }

    //for ideal situation, where all blocks arrive as requested
    //while being in order too
    pub async fn write_block_specific(&mut self, t: &Torrent, pos: PieceSize, len: u32, data: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<()> {
        Ok(())
    }

    //usize for starting position, this is to avoid sending same block request in short time
    //vec for generated blocks instead of iterator, pure lazyness
    pub fn generate_requests(&self, torrent: &Torrent, mut count: usize, start: usize) -> (Vec<Block>, usize) {
        let limit = self.map.len();
        let iter = self.map.iter().enumerate().skip(start).take(limit).filter(|x| x.1 == &false);
        let mut rt = Vec::with_capacity(count);
        let len = self.files.len(torrent);
        let mut first = None;
        let mut last = None;
        for i in iter.take(count) {
            if first == Some(i.0) {
                break;
            } else if first == None {
                first = Some(i.0);
            }
            let block_len = if self.map.len() - 1 == i.0 {
                let leftover = len % MAP_BLOCK_SIZE;
                if leftover == 0 {
                    MAP_BLOCK_SIZE
                } else {
                    leftover
                }
            } else {
                MAP_BLOCK_SIZE
            };
            rt.push(Block {
                begin: MAP_BLOCK_SIZE * i.0 as u32,
                len: NonZeroU32::new(block_len).unwrap(),
            });
            last = Some(i.0);
        }
        (rt, last.map(|x| x + 1).unwrap_or(start))
    }

    // we need to make this non blocking, so we yield at every 64 byte
    //todo replace read_sync, better hashing,(reading from file and updating hash at the same time)
    pub async fn hash(&mut self, torrent: &Torrent) -> anyhow::Result<Hash20> {
        if self.hasher_written != self.files.len(torrent) {
            println!("hash skipped {} out of {}", self.hasher_written, self.files.len(torrent));
            let mut buffer = [0u8; 1024];
            let mut read = self.files.read_sync(torrent, self.hasher_written, 1024, &mut &mut buffer[0..]).await?;
            self.hasher_written += read as u32;
            while read == 1024 {
                read = self.files.read_sync(torrent, self.hasher_written, 1024, &mut &mut buffer[0..]).await?;
                self.hasher.update(&buffer[..read]);
                self.hasher_written += read as u32;
            }
        } else {
            println!("hash skipped");
        }
        Ok(Hash20 {
            v: self.hasher.digest().bytes()
        })
    }

}