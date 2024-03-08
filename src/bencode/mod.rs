mod integer;

use tokio::io::{AsyncRead, AsyncWrite};
pub use integer::*;


pub trait BencodeReadFrom: Sized {
    async fn bc_read_from<R: AsyncRead>(read: &mut R) -> anyhow::Result<Self>;
}

pub trait BencodeWriteTo {
    async fn bc_write_to<W: AsyncWrite>(&self, write: &mut W) -> anyhow::Result<()>;
}
