use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use log::{error, info, trace};
use portable_atomic::AtomicBool;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};
use crate::handshake::HandShake;
use crate::torrent::{Torrent, TorrentHandle};
use crate::types::*;
pub struct Client {
    pub external_addr: SocketAddr,
    pub peer_table: Mutex<HashMap<Hash20, Vec<SocketAddr>>>,
    pub peer_left: Arc<Semaphore>,
    pub server: TcpListener,
    torrents: Mutex<HashMap<Hash20, TorrentHandle>>,
    pub config: ClientConfig,
    pub http_tracker_client: HttpClient,
    force_announce: AtomicBool,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub port: u16,
    pub request_pacing_step_up: u16,
    pub request_pacing_step_down: u16,
    pub max_pending_per_peer: usize,
    pub max_peers: u8,
    pub data_path: Option<PathBuf>,
    pub peer_id: Hash20,
    pub upnp: Option<Ipv4Addr>,
    pub request_peer_limit: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            port: 10000,
            request_pacing_step_up: 500,
            request_pacing_step_down: 1,
            max_pending_per_peer: 32,
            max_peers: 5,
            data_path: None,
            peer_id: Hash20 {
                v: *b"hellothisissmolbittr"
            },
            upnp: None,
            request_peer_limit: 50,
        }
    }
}

#[derive(Deref, DerefMut)]
pub struct ClientHandle {
    client: IoRc<Client>
}

const FILE_POSTFIX: &str = ".torrent";
const DATA_POSTFIX: &str = ".torrent.data";
const PATH_POSTFIX: &str = ".torrent.path";
impl Drop for ClientHandle {
    fn drop(&mut self) {
        loop {
            if let Ok(mut v) = self.client.torrents.try_lock() {
                v.clear();
                break;
            }
        }
    }
}

impl Client {

    pub async fn new(addr: IpAddr, external_addr: SocketAddr, config: ClientConfig) -> anyhow::Result<ClientHandle> {
        let client = Client {
            external_addr,
            peer_table: Default::default(),
            peer_left: Arc::from(Semaphore::new(config.max_peers as usize)),
            server: TcpListener::bind(SocketAddr::new(addr, config.port)).await?,
            torrents: Default::default(),
            config,
            http_tracker_client: Default::default(),
            force_announce: Default::default(),
        };
        let handle = ClientHandle {
            client: IoRc::new(client),
        };
        handle.load_from_data().await?;
        Ok(handle)
    }

    async fn load_from_data(self: &IoRc<Self>) -> anyhow::Result<()> {

        if let Some(data_path) = self.config.data_path.as_ref() {
            let mut directory = fs::read_dir(data_path).await?;
            let mut next = directory.next_entry().await?;
            while let Some(dir_entry) = next {
                if dir_entry.file_name().as_encoded_bytes().len() == 40 + FILE_POSTFIX.len() {
                    if dir_entry.file_name().as_encoded_bytes().ends_with(FILE_POSTFIX.as_bytes()) {
                        let mut hex = [0u8; 20];
                        hex::decode_to_slice(&dir_entry.file_name().as_encoded_bytes()[..40], &mut hex)?;
                        let data = fs::read(dir_entry.path()).await?;
                        let torrent_file = lava_torrent::torrent::v1::Torrent::read_from_bytes(data)?;

                        let path = {
                            let mut path_path = data_path.clone();
                            let mut path_filename = String::with_capacity(40 + PATH_POSTFIX.as_bytes().len());
                            path_filename.push_str(std::str::from_utf8(&dir_entry.file_name().as_encoded_bytes()[..40])?);
                            path_filename.push_str(PATH_POSTFIX);
                            path_path.push(path_filename);
                            fs::read(path_path).await?
                        };
                        let torrent = self.clone().add_torrent(torrent_file, PathBuf::from(String::from_utf8(path)?)).await?;
                        if let Ok(mut status_file) = {
                            let mut status_path = data_path.clone();
                            let mut status_filename = String::with_capacity(40 + DATA_POSTFIX.as_bytes().len());
                            status_filename.push_str(std::str::from_utf8(&dir_entry.file_name().as_encoded_bytes()[..40])?);
                            status_filename.push_str(DATA_POSTFIX);
                            status_path.push(status_filename);
                            fs::File::open(status_path).await
                        } {
                            torrent.load_status(&mut status_file).await?;
                        };
                    }
                }
                next = directory.next_entry().await?;
            }
        }

        Ok(())
    }

    pub async fn add_torrent(self: &IoRc<Self>, file: lava_torrent::torrent::v1::Torrent, path: PathBuf) -> anyhow::Result<IoRc<Torrent>> {
        if let Some(data_dir) = self.config.data_path.as_ref() {
            let mut string_buf = file.info_hash();
            string_buf.push_str(FILE_POSTFIX);
            let mut torrent_dir = data_dir.clone();
            torrent_dir.push(&string_buf);

            fs::write(torrent_dir, file.clone().encode()?).await?;
            let mut string_buf = file.info_hash();
            string_buf.push_str(PATH_POSTFIX);
            let mut path_dir = data_dir.clone();
            path_dir.push(&string_buf);

            fs::write(path_dir, path.as_os_str().as_encoded_bytes()).await?;
        }
        let torrent_handle = Torrent::new_from_file(self.clone(), file, path).await?;
        let torrent = (*torrent_handle).clone();
        self.torrents.lock().await.insert(torrent_handle.hash, torrent_handle);
        Ok(torrent)
    }

    pub async fn torrent(self: IoRc<Self>, hash: &Hash20) -> Option<IoRc<Torrent>> {
        self.torrents.lock().await.get(hash).map(|x| (*x).clone())
    }

    pub async fn remove(self: IoRc<Self>, hash: &Hash20) {
        self.torrents.lock().await.remove(hash);
    }

    pub async fn run(self: &IoRc<Self>) -> anyhow::Result<()> {
        trace!("run called");
        select!(
            r = self.listen() => r,
            r = self.save_task() => r,
            r = self.announce_task() => r,
            r = self.disconnect_slow_task() => r,
        )
    }


    async fn listen(self: &IoRc<Self>) -> anyhow::Result<()> {
        trace!("listen called");
        loop {
            let (mut sock, addr) = self.server.accept().await?;
            info!("{addr} connecting...");
            if let Ok(perm) = self.peer_left.clone().try_acquire_owned() {
                let client = self.clone();
                spawn(async move {
                    let timeout = Instant::now() + Duration::from_secs(3);
                    let result: anyhow::Result<()> = try {
                        let handshake = timeout_at(timeout, HandShake::read_from(&mut sock)).await??;
                        info!("{addr} handshake received");
                        if let Some(torrent) = client.torrents.lock().await.get(&handshake.info_hash).map(|x| (*x).clone()) {
                            timeout_at(timeout, torrent.connect_peer(addr, sock, Some(handshake), perm)).await??;
                        } else {
                            info!("{addr} torrent not found, dropping connection");
                        }
                    };
                    if let Err(e) = result {
                        error!("{addr} error while connecting: {e}");
                    }
                });
            }
        }
    }

    async fn save_task(self: &IoRc<Self>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let torrents = self.torrents.lock().await;
            for (hash, torrent) in torrents.iter() {
                if let Some(data_dir) = self.config.data_path.as_ref() {
                    let mut data_dir = data_dir.clone();
                    let mut filename = String::with_capacity(50);
                    let mut output_slice = [0u8; 40];
                    hex::encode_to_slice(hash.v, &mut output_slice)?;
                    filename.push_str(std::str::from_utf8(&output_slice)?);
                    filename.push_str(DATA_POSTFIX);
                    data_dir.push(filename);
                    let mut status_file = fs::File::create(data_dir).await?;
                    torrent.save_status(&mut status_file).await?;
                    torrent.save_files().await?;
                }
            }
        }
    }

    async fn announce_task(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            {
                let force = self.force_announce.swap(false, Ordering::Relaxed);
                let torrents = {
                    let torrents = self.torrents.lock().await;
                    torrents.iter().map(|(_, x)| (*x).clone()).collect_vec()
                };
                for x in torrents {
                    x.update_announce(force).await?;
                }
            }
            println!("peer_left: {}", self.peer_left.available_permits());
            interval.tick().await;
        }
    }

    async fn disconnect_slow_task(&self) -> anyhow::Result<()> {
        loop {
            if let Ok(mut perm) = self.peer_left.clone().try_acquire_owned() {
                let torrents = self.torrents.lock().await.iter().map(|x| (*x.1).clone()).collect_vec();
                let mut con = false;
                for x in torrents.iter() {
                    con |= x.try_connect_peer(perm).await?;
                    if let Ok(perm2) = self.peer_left.clone().try_acquire_owned() {
                        perm = perm2
                    } else {
                        break;
                    }
                }
                if !con {
                    //self.force_announce.store(true, Ordering::Relaxed);
                } else {
                    continue;
                }
            } else {
                let torrents = {
                    let torrents = self.torrents.lock().await;
                    torrents.iter().map(|(_, x)| (*x).clone()).collect_vec()
                };
                for x in torrents {
                    let mut slowest: Option<(SocketAddr, u32)> = None;
                    let peers_lock = x.connected_peers.lock().await;
                    for peer in peers_lock.iter() {
                        let est_speed: u32 = peer.1.0.status.lock().await.download_speed.estimated_speed();
                        if let Some(slowest) = slowest.as_mut() {
                            if slowest.1 > dbg!(est_speed) {
                                *slowest = (*peer.0, est_speed);
                            }
                        } else {
                            slowest = Some((*peer.0, est_speed))
                        }
                    }
                    if let Some(slowest) = slowest {
                        if dbg!(peers_lock[&slowest.0].0.connected.elapsed() > Duration::from_secs(10)) {
                            drop(peers_lock);
                            x.disconnect_peer(&slowest.0).await;
                            info!("{} disconnected due to inactive download", slowest.0);
                            continue;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}