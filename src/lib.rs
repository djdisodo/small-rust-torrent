#![feature(int_roundings, exact_size_is_empty, try_blocks)]
#![feature(future_join)]
#![feature(let_chains)]

pub mod piece;
pub mod client;
pub mod torrent;
pub mod peer;

mod message;

mod handshake;

mod bencode;

mod speed_estimator;
pub mod types {
    //these are here for portability reasons
    pub use std::rc::Rc as IoRc;

    pub use std::collections::{HashMap, HashSet};
    use std::rc::Rc;

    pub use tokio::fs as fs;
    pub use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};

    pub use tokio::sync::Mutex;

    pub use tokio::task::spawn_local as spawn;

    pub use sha1_smol::Sha1;

    pub use tokio::net;

    pub use std::rc::Weak;

    pub type PieceSize = u32;

    pub type RcMutex<T> = Rc<Mutex<T>>;

    #[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
    pub struct Hash20 {
        pub v: [u8; 20]
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

fn a() {

}