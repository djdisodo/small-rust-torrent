use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;
use derive_more::{Deref, DerefMut};
use log::{error, info, trace};
use portable_atomic::AtomicU8;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::Notify;
use crate::handshake::HandShake;
use crate::torrent::{Torrent, TorrentHandle};
use crate::types::*;
pub struct Client {
    pub peer_table: Mutex<HashMap<Hash20, Vec<SocketAddr>>>,
    pub peer_left: AtomicU8,
    peer_drop_notify: Notify,
    pub server: TcpListener,
    torrents: Mutex<HashMap<Hash20, TorrentHandle>>,
    pub config: ClientConfig,
}


#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub request_pacing_step_up: u16,
    pub request_pacing_step_down: u16,
    pub max_pending_per_peer: usize,
    pub max_peers: u8,
    pub data_path: Option<PathBuf>,
    pub peer_id: Hash20
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_pacing_step_up: 500,
            request_pacing_step_down: 1,
            max_pending_per_peer: 32,
            max_peers: 5,
            data_path: None,
            peer_id: Hash20 {
                v: *b"hellothisissmolbittr"
            },
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

    pub async fn new(addr: SocketAddr, config: ClientConfig) -> anyhow::Result<ClientHandle> {
        let client = Client {
            peer_table: Default::default(),
            peer_left: AtomicU8::new(config.max_peers),
            peer_drop_notify: Default::default(),
            server: TcpListener::bind(addr).await?,
            torrents: Default::default(),
            config
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

            string_buf.shrink_to(40);
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
        )
    }


    async fn listen(self: &IoRc<Self>) -> anyhow::Result<()> {
        trace!("listen called");
        loop {
            if self.peer_left.load(Ordering::SeqCst) == 0 {
                self.peer_drop_notify.notified().await;
                continue;
            }
            trace!("waiting to accept");
            let (mut sock, addr) = self.server.accept().await?;
            info!("{addr} connecting...");
            let client = self.clone();
            self.peer_left.neg(Ordering::SeqCst);
            spawn(async move { select! {
                _ = async {
                    let resuglt: anyhow::Result<()> = try {
                        let handshake = HandShake::read_from(&mut sock).await?;
                        info!("{addr} handshake received");
                        if let Some(torrent) = client.torrents.lock().await.get(&handshake.info_hash).map(|x| (*x).clone()) {
                            torrent.connect_peer(addr, sock, Some(handshake)).await?;
                        } else {
                            info!("{addr} torrent not found, dropping connection");
                        }
                    };
                    result.map_err(|x| error!("{addr} error while connecting: {x}")).ok();
                } => {},
                _ = tokio::time::sleep(Duration::from_secs(20)) => {client.drop_peer().await; /*timeout*/}
            }});
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

    pub(crate) async fn drop_peer(&self) {
        self.peer_left.add(1, Ordering::SeqCst);
        self.peer_drop_notify.notify_one();
    }
}