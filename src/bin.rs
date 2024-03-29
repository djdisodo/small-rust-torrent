use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use log::{info, LevelFilter};
use simplelog::{Config, ConfigBuilder, SimpleLogger};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::task::LocalSet;
use smolbt::client::{Client, ClientConfig};
use smolbt::torrent::{Torrent, TorrentFile};

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let config = ConfigBuilder::new().set_location_level(LevelFilter::Trace).build();
    SimpleLogger::init(LevelFilter::Trace, config).unwrap();
    let mut bytes = File::open("./test.torrent").await.unwrap();
    let mut data = Vec::new();
    bytes.read_to_end(&mut data).await.unwrap();
    let torrent = TorrentFile::read_from_bytes(&data).unwrap();
    let mut cc = ClientConfig::default();
    cc.port = 10000;
    let mut client = Client::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(180,180,180,180)), 10001),
        cc
    ).await.unwrap();
    client.add_torrent(torrent, "./testaaa/".into()).await.unwrap();
    info!("added torrent");
    let localset = LocalSet::new();
    localset.run_until(async move {
        client.run().await.unwrap();
    }).await;
    localset.await;
}