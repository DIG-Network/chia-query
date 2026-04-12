use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;

use chia_wallet_sdk::client::{
    connect_peer, create_native_tls_connector, load_ssl_cert, Network, Peer, PeerOptions,
};
use tokio_tungstenite::Connector;

use chia::protocol::Message;
use tokio::sync::mpsc;

use crate::types::ChiaQueryError;
use crate::NetworkType;

const BATCH_SIZE: usize = 10;
const MAINNET_PORT: u16 = 8444;
const TESTNET11_PORT: u16 = 58444;

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

pub fn create_tls(cert_path: &Path, key_path: &Path) -> Result<Connector, ChiaQueryError> {
    let cert_str = cert_path
        .to_str()
        .ok_or_else(|| ChiaQueryError::TlsError("cert path is not valid UTF-8".into()))?;
    let key_str = key_path
        .to_str()
        .ok_or_else(|| ChiaQueryError::TlsError("key path is not valid UTF-8".into()))?;
    let cert =
        load_ssl_cert(cert_str, key_str).map_err(|e| ChiaQueryError::TlsError(e.to_string()))?;
    create_native_tls_connector(&cert).map_err(|e| ChiaQueryError::TlsError(e.to_string()))
}

// ---------------------------------------------------------------------------
// Peer discovery + connection
// ---------------------------------------------------------------------------

/// Resolve DNS introducers, shuffle, and return the first peer that connects
/// within `timeout`.  Returns the connected [`Peer`] together with its address
/// so the pool can track it.
///
/// Discovery order (matching dig-chia-sdk FullNodePeer.ts):
///   1. `TRUSTED_FULLNODE` env var (if set and valid IPv4)
///   2. localhost (127.0.0.1)
///   3. DNS introducers (same 4 hosts used by dig-chia-sdk):
///      - dns-introducer.chia.net
///      - chia.ctrlaltdel.ch
///      - seeder.dexie.space
///      - chia.hoffmang.com
pub async fn connect_random_peer(
    network: NetworkType,
    tls: &Connector,
    timeout: Duration,
) -> Result<(Peer, SocketAddr, mpsc::Receiver<Message>), ChiaQueryError> {
    let default_port = match network {
        NetworkType::Mainnet => MAINNET_PORT,
        NetworkType::Testnet11 => TESTNET11_PORT,
    };
    let network_id = network.network_id().to_string();

    // -- Priority peers (trusted full node + localhost) ----------------------
    // Mirrors the behaviour of dig-chia-sdk FullNodePeer.ts which tries a
    // TRUSTED_FULLNODE env-var and localhost before falling back to DNS.
    let mut priority_addrs: Vec<SocketAddr> = Vec::new();

    if let Ok(trusted) = std::env::var("TRUSTED_FULLNODE") {
        if let Ok(ip) = trusted.parse::<std::net::IpAddr>() {
            priority_addrs.push(SocketAddr::new(ip, default_port));
        } else {
            log::debug!("TRUSTED_FULLNODE value is not a valid IP: {trusted}");
        }
    }

    // Always try localhost -- a local full node is the fastest option.
    priority_addrs.push(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        default_port,
    ));

    // Try priority peers first (sequentially, fast timeout).
    for addr in &priority_addrs {
        match try_connect(&network_id, tls, *addr, timeout).await {
            Ok((peer, receiver)) => return Ok((peer, *addr, receiver)),
            Err(e) => log::debug!("priority peer {addr} unavailable: {e}"),
        }
    }

    // -- DNS introducer discovery -------------------------------------------
    let net = match network {
        NetworkType::Mainnet => Network::default_mainnet(),
        NetworkType::Testnet11 => Network::default_testnet11(),
    };

    let mut addrs = net.lookup_all(timeout, BATCH_SIZE).await;

    if addrs.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }

    // Randomise so we don't always hammer the same peer.
    addrs.shuffle(&mut rand::thread_rng());
    addrs.dedup();

    // Remove any addresses we already tried above.
    addrs.retain(|a| !priority_addrs.contains(a));

    // Try batches of concurrent connection attempts.
    for chunk in addrs.chunks(BATCH_SIZE) {
        let mut futures = FuturesUnordered::new();

        for &addr in chunk {
            let tls_clone = tls.clone();
            let nid = network_id.clone();
            futures.push(async move {
                let res = tokio::time::timeout(
                    timeout,
                    connect_peer(nid, tls_clone, addr, PeerOptions::default()),
                )
                .await;
                (addr, res)
            });
        }

        while let Some((addr, result)) = futures.next().await {
            match result {
                Ok(Ok((peer, receiver))) => return Ok((peer, addr, receiver)),
                Ok(Err(e)) => log::debug!("connect to {addr} failed: {e}"),
                Err(_) => log::debug!("connect to {addr} timed out"),
            }
        }
    }

    Err(ChiaQueryError::PeerDiscoveryFailed)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn try_connect(
    network_id: &str,
    tls: &Connector,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<(Peer, mpsc::Receiver<Message>), ChiaQueryError> {
    let result = tokio::time::timeout(
        timeout,
        connect_peer(
            network_id.to_string(),
            tls.clone(),
            addr,
            PeerOptions::default(),
        ),
    )
    .await;

    match result {
        Ok(Ok((peer, receiver))) => Ok((peer, receiver)),
        Ok(Err(e)) => Err(ChiaQueryError::PeerConnection(e.to_string())),
        Err(_) => Err(ChiaQueryError::PeerConnection("timed out".into())),
    }
}
