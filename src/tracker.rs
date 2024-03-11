use std::borrow::Cow;
use std::sync::atomic::Ordering;
use lava_torrent::tracker::TrackerResponse;
use serde_derive::Serialize;
use crate::torrent::Torrent;

//runs out of memory here sometimes, we nned
pub async fn http_tracker(torrent: &Torrent, url: &str, count: usize) -> anyhow::Result<TrackerResponse> {
    #[derive(Serialize)]
    struct Query<'a> {
        info_hash: &'a str,
        peer_id : &'a str,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        compact: u8,
        no_peer_id: u8,
        ip: String,
        numwant: usize
    };
    // let query = [
    //     ("info_hash", format!("{}", torrent.hash)),
    //     ("peer_id", format!("{}", torrent.client.config.peer_id)),
    //     ("port", format!("{}", torrent.client.external_addr.port())),
    //     ("uploaded", format!("{}", torrent.uploaded.load(Ordering::SeqCst))),
    //     ("downloaded", format!("{}", torrent.downloaded().await)),
    //     ("left", format!("{}", torrent.needed().await)),
    //     ("compact", "1".to_string()),
    //     ("no_peer_id", "1".to_string()),
    //     ("ip", format!("{}", torrent.client.external_addr.ip())),
    //     ("numwant", format!("{}", count)),
    // ];
    let query = unsafe {
        Query {
            info_hash: std::str::from_utf8_unchecked(&torrent.hash.v), // a bit of cheating, is there a better way?
            peer_id: std::str::from_utf8_unchecked(&torrent.client.config.peer_id.v),
            port: torrent.client.external_addr.port(),
            uploaded: torrent.uploaded.load(Ordering::SeqCst),
            downloaded: torrent.downloaded().await,
            left: torrent.needed().await,
            compact: 1,
            no_peer_id: 1,
            ip: format!("{}", torrent.client.external_addr.ip()),
            numwant: count,
        }
    };

    let bytes = torrent.client.http_tracker_client.get(url).query(&query).send().await?.bytes().await?;
    Ok(TrackerResponse::from_bytes(bytes.as_ref())?)
}