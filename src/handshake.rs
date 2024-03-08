use tokio::io::{AsyncRead, AsyncWrite};
use crate::types::*;

pub struct HandShake {
    pstr: String,
    info_hash: Hash20,
    peer_id: Hash20
}

impl HandShake {
    pub async fn read_from<R: AsyncRead + Unpin>(read: &mut R) -> anyhow::Result<Self> {
        let mut pstrlen = read.read_u8().await?;
        let mut pstr = vec![0u8; pstrlen as usize];
        read.read_exact(&mut pstr).await?;
        let pstr = String::from_utf8(pstr)?;
        let mut info_hash = [0; 20];
        read.read_exact(&mut info_hash).await?;
        let mut peer_id = [0; 20];
        read.read_exact(&mut peer_id).await?;
        println!("info_hash: {info_hash:#?}");
        Ok(Self {
            pstr,
            info_hash: Hash20 {
                v: info_hash,
            },
            peer_id: Hash20 {
                v: peer_id
            },
        })
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, write: &mut W) -> anyhow::Result<()> {
        write.write_all(&[self.pstr.len() as u8]).await?;
        write.write_all(self.pstr.as_bytes()).await?;
        write.write_all(&self.info_hash.v).await?;
        write.write_all(&self.peer_id.v).await?;
        Ok(())
    }
}