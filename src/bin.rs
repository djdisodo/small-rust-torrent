use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use log::{info, LevelFilter};
use simplelog::{Config, ConfigBuilder, SimpleLogger};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::task::LocalSet;
use smolbt::client::Client;
use smolbt::torrent::{Torrent, TorrentFile};
use smolbt::types::Hash20;

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let config = ConfigBuilder::new().set_location_level(LevelFilter::Error).build();
    SimpleLogger::init(LevelFilter::Debug, config).unwrap();
    let mut bytes = File::open("./test.torrent").await.unwrap();
    let mut data = Vec::new();
    bytes.read_to_end(&mut data).await.unwrap();
    let torrent = TorrentFile::read_from_bytes(&data).unwrap();
    let mut client = Client::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 5555)).await.unwrap();
    Torrent::new_from_file(&*client, torrent, "./testaaa/".into()).await.unwrap();
    info!("added torrent");
    let localset = LocalSet::new();
    localset.run_until(async move {
        client.listen().await.unwrap();
    }).await;
    localset.await;
}