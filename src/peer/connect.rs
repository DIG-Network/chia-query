use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};

use chia_ssl::ChiaCertificate;
use chia_wallet_sdk::client::{
    connect_peer, create_native_tls_connector, load_ssl_cert, Network, Peer, PeerOptions,
};
use tokio_tungstenite::Connector;

use chia_protocol::Message;
use tokio::sync::mpsc;

use super::ordering;
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

/// Build a TLS connector from a certificate/key pair on disk, GENERATING one if either file
/// cannot be read.
///
/// Used only when a caller explicitly supplies [`TlsIdentity::Files`](crate::TlsIdentity::Files),
/// e.g. to present a real Chia node's wallet certificate.
///
/// This writes to the filesystem, which [`create_generated_tls`] deliberately does not. When
/// EITHER path cannot be read — missing, permission-denied, or not valid UTF-8 — `load_ssl_cert`
/// generates a fresh certificate and PERSISTS both it and its private key to the two paths given.
/// Two consequences are easy to miss:
///
/// - Passing paths that do not yet exist yields a new certificate and key written to disk rather
///   than an error, *provided both writes succeed*. If a parent directory is not writable the
///   write error still surfaces as [`ChiaQueryError::TlsError`]. That distinction matters here:
///   the failure this crate exists to avoid (dig_ecosystem#2210) was exactly a write to a
///   directory the process could not write to.
/// - Because the fallback triggers when *either* file fails to read, an unreadable key beside a
///   readable certificate OVERWRITES that certificate with a newly generated one. This can
///   replace a file the caller already had, not merely create a missing one.
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

/// HOW a peer was reached, which is not the same question as whether it is reachable.
///
/// A priority peer — an operator's `TRUSTED_FULLNODE`, or a full node on this machine — is
/// *preferred* because it is fast and under the operator's own control. That makes it a good peer
/// to ASK. It does not make it an independent voice: corroboration is only worth anything when the
/// peers agreeing came from sources an attacker cannot all supply, and a co-resident process is
/// precisely a source a local attacker CAN supply. Priority orders discovery; it never confers
/// authority.
///
/// So this is reported rather than decided here. A caller doing round-robin reads wants the fast
/// peer; a caller counting agreeing opinions must count [`PeerOrigin::Discovered`] peers only. A
/// single return value cannot serve both, and conflating them is what let one local process stand
/// in for an entire "independent" peer set (dig_ecosystem#2648).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerOrigin {
    /// Reached from a configured or co-resident address tried ahead of discovery.
    Priority,
    /// Reached from a DNS introducer's address set.
    Discovered,
}

/// Resolve DNS introducers, shuffle, and return the first peer that connects
/// within `timeout`.  Returns the connected [`Peer`] together with its address
/// so the pool can track it.
///
/// Equivalent to [`connect_random_peer_excluding`] with nothing excluded; prefer that when the
/// caller already holds addresses, so a dial cannot return an address the caller must then discard.
pub async fn connect_random_peer(
    network: NetworkType,
    tls: &Connector,
    timeout: Duration,
) -> Result<(Peer, SocketAddr, mpsc::Receiver<Message>), ChiaQueryError> {
    let (peer, addr, receiver, _origin) =
        connect_random_peer_excluding(network, tls, timeout, &[]).await?;
    Ok((peer, addr, receiver))
}

/// Connect one peer whose address is NOT in `exclude`, reporting how it was reached.
///
/// Discovery order (matching dig-chia-sdk FullNodePeer.ts):
///   1. `TRUSTED_FULLNODE` env var (if set and valid IP)
///   2. localhost (127.0.0.1)
///   3. DNS introducers (same 4 hosts used by dig-chia-sdk):
///      - dns-introducer.chia.net
///      - chia.ctrlaltdel.ch
///      - seeder.dexie.space
///      - chia.hoffmang.com
///
/// `exclude` applies to the priority addresses as well as the discovered ones. That is the whole
/// point of it: the localhost dial is tried on EVERY call, so a caller filling a pool would
/// otherwise be handed the same local address as many times as it asks, and a local node that
/// answers would occupy every slot (dig_ecosystem#2648). Excluding what the caller already holds
/// makes the preference "try the local node first" instead of "try only the local node".
pub async fn connect_random_peer_excluding(
    network: NetworkType,
    tls: &Connector,
    timeout: Duration,
    exclude: &[SocketAddr],
) -> Result<(Peer, SocketAddr, mpsc::Receiver<Message>, PeerOrigin), ChiaQueryError> {
    let default_port = match network {
        NetworkType::Mainnet => MAINNET_PORT,
        NetworkType::Testnet11 => TESTNET11_PORT,
    };
    let network_id = network.network_id().to_string();

    let priority_addrs = priority_addresses(default_port, exclude);

    // Try priority peers first (sequentially, fast timeout).
    for addr in &priority_addrs {
        match try_connect(&network_id, tls, *addr, timeout).await {
            Ok((peer, receiver)) => return Ok((peer, *addr, receiver, PeerOrigin::Priority)),
            Err(e) => log::debug!("priority peer {addr} unavailable: {e}"),
        }
    }

    // -- DNS introducer discovery -------------------------------------------
    let net = match network {
        NetworkType::Mainnet => Network::default_mainnet(),
        NetworkType::Testnet11 => Network::default_testnet11(),
    };

    let discovered = net.lookup_all(timeout, BATCH_SIZE).await;

    if discovered.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }

    // Distinct, spread, then IPv6-first (§5.2) — see `ordering::candidate_order`, which also
    // carries the fix for the shuffle-then-`dedup` that made deduplication near-vacuous here.
    let mut addrs = ordering::candidate_order(&discovered);

    // Remove any addresses we already tried above, and any the caller already holds.
    addrs.retain(|a| !priority_addrs.contains(a) && !exclude.contains(a));

    if addrs.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }

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
                Ok(Ok((peer, receiver))) => {
                    return Ok((peer, addr, receiver, PeerOrigin::Discovered))
                }
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

/// The addresses tried ahead of DNS discovery, minus anything `exclude` already holds.
///
/// Mirrors dig-chia-sdk `FullNodePeer.ts`: an operator's `TRUSTED_FULLNODE`, then a full node on
/// this machine. Both are preferences about SPEED and operator control.
///
/// The exclusion is what keeps a preference from becoming a monopoly. These addresses are computed
/// on every dial, so without it a caller filling N slots is offered the same local address N times
/// — and any unprivileged local process that binds the port becomes the whole peer set
/// (dig_ecosystem#2648).
fn priority_addresses(default_port: u16, exclude: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut addrs: Vec<SocketAddr> = Vec::new();

    if let Ok(trusted) = std::env::var("TRUSTED_FULLNODE") {
        if let Ok(ip) = trusted.parse::<std::net::IpAddr>() {
            addrs.push(SocketAddr::new(ip, default_port));
        } else {
            log::debug!("TRUSTED_FULLNODE value is not a valid IP: {trusted}");
        }
    }

    addrs.push(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        default_port,
    ));

    addrs.retain(|addr| !exclude.contains(addr));
    addrs
}

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

#[cfg(test)]
mod tests {
    use super::*;

    const MAINNET: u16 = MAINNET_PORT;

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port)
    }

    /// The control: with nothing held, the local node is still preferred.
    ///
    /// Without this, an exclusion that dropped the local address unconditionally — the tempting
    /// "just delete the prepend" fix — would look identical to the correct behaviour.
    #[test]
    fn the_local_node_is_offered_when_it_is_not_already_held() {
        assert!(priority_addresses(MAINNET, &[]).contains(&localhost(MAINNET)));
    }

    /// The fix: it is offered AT MOST ONCE, because a caller that already holds it excludes it.
    #[test]
    fn the_local_node_is_not_offered_again_once_it_is_held() {
        let held = [localhost(MAINNET)];
        assert!(
            !priority_addresses(MAINNET, &held).contains(&localhost(MAINNET)),
            "a held local node must not be offered again, or one address fills the pool"
        );
    }

    /// The exclusion is by exact socket address, not by host: the same host on another port is a
    /// different peer, and refusing it would silently narrow discovery.
    #[test]
    fn exclusion_is_per_socket_address_not_per_host() {
        let held = [localhost(TESTNET11_PORT)];
        assert!(priority_addresses(MAINNET, &held).contains(&localhost(MAINNET)));
    }
}
