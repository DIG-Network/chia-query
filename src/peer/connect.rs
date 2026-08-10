use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;

use chia_ssl::ChiaCertificate;
use chia_wallet_sdk::client::{
    connect_peer, create_native_tls_connector, load_ssl_cert, Network, Peer, PeerOptions,
};
use tokio_tungstenite::Connector;

use chia_protocol::Message;
use tokio::sync::mpsc;

use crate::types::ChiaQueryError;
use crate::NetworkType;

const BATCH_SIZE: usize = 10;
const MAINNET_PORT: u16 = 8444;
const TESTNET11_PORT: u16 = 58444;

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// Build a TLS connector from a FRESHLY GENERATED, in-memory Chia certificate.
///
/// The Chia peer protocol does not treat the client certificate as a credential — a
/// full node accepts any well-formed certificate — so a query client has no reason to
/// require a Chia installation just to speak to peers. Generating avoids the whole
/// class of "the certificate lives in a home directory this process cannot read or
/// write" failures (dig_ecosystem#2210).
///
/// Nothing is written to disk. The certificate lives for the life of the process, so
/// the peer-visible client identity changes on restart; that is harmless because peers
/// are tracked by address, not by certificate, and persisting one would reintroduce
/// the dependency on a writable well-known directory that this exists to remove.
pub fn create_generated_tls() -> Result<Connector, ChiaQueryError> {
    let cert = ChiaCertificate::generate().map_err(|e| ChiaQueryError::TlsError(e.to_string()))?;
    create_native_tls_connector(&cert).map_err(|e| ChiaQueryError::TlsError(e.to_string()))
}

/// Build a TLS connector from a certificate/key pair on disk, CREATING one if it is absent.
///
/// Used only when a caller explicitly supplies [`TlsIdentity::Files`](crate::TlsIdentity::Files),
/// e.g. to present a real Chia node's wallet certificate.
///
/// This writes to the filesystem, which [`create_generated_tls`] deliberately does not. When
/// either path cannot be read, `load_ssl_cert` generates a fresh certificate and PERSISTS both
/// the certificate and its private key to the two paths given. So a caller that passes paths
/// which do not yet exist gets a new key file on disk rather than an error — worth knowing,
/// because the failure this crate exists to avoid (dig_ecosystem#2210) was exactly a write to a
/// directory the process could not write to.
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
