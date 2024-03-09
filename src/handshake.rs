use anyhow::bail;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::types::*;

pub struct HandShake {
    //pstr: String,
    pub(crate) info_hash: Hash20,
    pub(crate) peer_id: Hash20
}

impl HandShake {

    const PSTR: &'static [u8; 19] = b"BitTorrent protocol";
    pub async fn read_from<R: AsyncRead + Unpin>(read: &mut R) -> anyhow::Result<Self> {
        let pstrlen = read.read_u8().await? as usize;
        if pstrlen != Self::PSTR.len() {
            bail!("invalid protocol");
        }
        let mut read_buffer = [0u8; 19];
        read.read_exact(&mut read_buffer).await?;
        if read_buffer != *Self::PSTR {
            bail!("invalid protocol");
        }
        let mut reserved = [0u8; 8];
        read.read_exact(&mut reserved).await?;
        let mut info_hash = [0; 20];
        read.read_exact(&mut info_hash).await?;
        let mut peer_id = [0; 20];
        read.read_exact(&mut peer_id).await?;
        Ok(Self {
            info_hash: Hash20 {
                v: info_hash,
            },
            peer_id: Hash20 {
                v: peer_id
            },
        })
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, write: &mut W) -> anyhow::Result<()> {
        let pstr = b"BitTorrent protocol";
        write.write_u8(pstr.len() as u8).await?;
        write.write_all(pstr).await?;
        write.write_all(&[0u8; 8]).await?;
        write.write_all(&self.info_hash.v).await?;
        write.write_all(&self.peer_id.v).await?;
        Ok(())
    }
}