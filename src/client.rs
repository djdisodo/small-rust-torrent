use std::io::Read;
use std::net::SocketAddr;
use derive_more::{Deref, DerefMut};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use log::{error, info};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use crate::peer::{PeerConnection, receive_handshake};
use crate::torrent::TorrentHandle;
use crate::types::*;
pub struct Client {
    pub peer_table: Mutex<HashMap<Hash20, Vec<SocketAddr>>>,
    pub peer_left: Mutex<usize>,
    pub peer_drop_notify: Notify,
    pub peer_accepting_tasks: Mutex<FuturesUnordered<JoinHandle<()>>>,
    pub server: TcpListener,
    pub peer_id: Hash20,
    pub torrents: Mutex<HashMap<Hash20, TorrentHandle>>,
    pub max_pending_per_peer: usize,
    pub stop_notify: Notify
}

#[derive(Deref, DerefMut)]
pub struct ClientHandle {
    client: IoRc<Client>
}

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

    pub async fn new(addr: SocketAddr) -> anyhow::Result<ClientHandle> {
        let client = Client {
            peer_table: Default::default(),
            peer_left: Mutex::new(5),
            peer_drop_notify: Default::default(),
            peer_accepting_tasks: Default::default(),
            server: TcpListener::bind(addr).await?,
            peer_id: {
                let mut buf = [0u8; 20];
                std::io::Read::read_exact(&mut "hellothisissmolbittr".as_bytes(), &mut buf)?;
                Hash20 {
                    v: buf
                }
            },
            torrents: Default::default(),
            max_pending_per_peer: 32,
            stop_notify: Default::default(),
        };
        Ok(ClientHandle {
            client: IoRc::new(client),
        })
    }
    pub async fn listen(self: &IoRc<Self>) -> anyhow::Result<()> {
        loop {
            if *self.peer_left.lock().await == 0 {
                self.peer_drop_notify.notified().await;
                continue;
            }
            info!("listening");
            let (mut sock, addr) = self.server.accept().await?;
            info!("peer trying to connect: {addr:#?}");
            let client = self.clone();
            let joinhandle = spawn(async move { select! {
                _ = async {
                    let result: anyhow::Result<()> = try {
                        let hash = receive_handshake(&mut sock).await?;
                        info!("received handshake");
                        if let Some(torrent) = client.torrents.lock().await.get(&hash).map(|x| (*x).clone()) {
                            let (peer, _) = PeerConnection::connect(addr, torrent.clone(), sock, true).await?;
                            torrent.connected_peers.lock().await.insert(addr, peer);
                        } else {
                            info!("peer tried to connect, torrent not found");
                        }
                    };
                    result.map_err(|x| error!("error while connecting to peer: {x:#?}")).ok();
                } => {},
                _ = client.stop_notify.notified() => {}
            }});
            //self.peer_accepting_tasks.lock().await.push(joinhandle);
        }
        Ok(())
    }

    pub async fn stop(&self) {
        self.stop_notify.notify_waiters();
    }
}