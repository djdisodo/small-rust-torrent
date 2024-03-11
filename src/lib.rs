#![feature(int_roundings, exact_size_is_empty, try_blocks)]
#![feature(future_join)]
#![feature(let_chains)]
#![feature(extern_types)]
#[cfg(feature = "external_sha1")]
extern crate external_sha1;

pub mod piece;
pub mod client;
pub mod torrent;
pub mod peer;

mod message;

mod handshake;

mod bitfield;

mod speed_estimator;

mod tracker;
pub mod types {
    //these are here for portability reasons
    pub use std::rc::Rc as IoRc;

    pub use std::collections::{HashMap, HashSet};
    use std::fmt::{Debug, Display, Formatter};
    use std::io::Read;
    use std::rc::Rc;

    pub use tokio::fs as fs;
    pub use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};

    pub use tokio::sync::Mutex;

    pub use tokio::task::spawn_local as spawn;

    #[cfg(feature = "external_sha1")]
    pub use external_sha1::*;


    #[cfg(not(feature = "external_sha1"))]
    pub use sha1_smol::Sha1;

    pub use tokio::net;

    pub use std::rc::Weak;

    pub type PieceSize = u32;

    pub use crate::bitfield::*;

    pub use reqwest::Client as HttpClient;

    pub type RcMutex<T> = Rc<Mutex<T>>;

    #[derive(Copy, Clone, Eq, PartialEq, Hash)]
    pub struct Hash20 {
        pub v: [u8; 20]
    }

    impl Display for Hash20 {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            for x in self.v {
                write!(f, "{x:02x}")?;
            }
            Ok(())
        }
    }

    impl Debug for Hash20 {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            for x in self.v {
                write!(f, "{x:02x}")?;
            }
            Ok(())
        }
    }

    impl TryFrom<&[u8]> for Hash20 {
        type Error = ();

        fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
            if value.len() < 20 {
                Err(())
            } else {
                let mut v = [0u8; 20];
                std::io::Read::read(&mut value, &mut v).unwrap();
                Ok(Self {
                    v
                })
            }
        }
    }


}

#[macro_export]
macro_rules! trim {
    ($v:expr) => {let mut v = $v; v.shrink_to_fit(); v};
}
#[macro_export]
macro_rules! trim_mut {
    ($v:expr) => {v.shrink_to_fit()};
}
