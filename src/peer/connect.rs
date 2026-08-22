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
pub(crate) const MAINNET_PORT: u16 = 8444;
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

    // Remove any addresses we already tried above, any the caller already holds, and any that
    // are not reachable from outside this host — see `is_host_local`.
    addrs.retain(|a| !is_host_local(a) && !priority_addrs.contains(a) && !exclude.contains(a));

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
pub(crate) fn priority_addresses(default_port: u16, exclude: &[SocketAddr]) -> Vec<SocketAddr> {
    let trusted = std::env::var("TRUSTED_FULLNODE").ok();
    priority_addresses_from(trusted.as_deref(), default_port, exclude)
}

/// [`priority_addresses`] with the operator's `TRUSTED_FULLNODE` passed in rather than read.
///
/// Split out so the SIZE of the priority list is measurable. `plurality::PRIORITY_SLOTS` sizes the
/// pool around how many slots this list can occupy, and a test that restated that number instead
/// of obtaining it from here would pin the model rather than the code — which is exactly how the
/// list grew to two entries while the pool was still sized for one.
pub(crate) fn priority_addresses_from(
    trusted: Option<&str>,
    default_port: u16,
    exclude: &[SocketAddr],
) -> Vec<SocketAddr> {
    let mut addrs: Vec<SocketAddr> = Vec::new();

    if let Some(trusted) = trusted {
        match trusted.parse::<std::net::IpAddr>() {
            Ok(ip) => addrs.push(SocketAddr::new(ip, default_port)),
            Err(_) => log::debug!("TRUSTED_FULLNODE value is not a valid IP: {trusted}"),
        }
    }

    addrs.push(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        default_port,
    ));

    addrs.retain(|addr| !exclude.contains(addr));
    addrs
}

/// Whether `addr` names something on this host or its local network rather than a peer on the
/// internet.
///
/// A DISCOVERED address that is local is refused outright, and the reason is what discovery is
/// counted for. A peer reached from a preferred address is recorded as
/// [`PeerOrigin::Priority`] and deliberately excluded from
/// [`independent_peer_count`](super::pool::PeerPool::independent_peer_count); the same node
/// arriving through the introducer set would be recorded as [`PeerOrigin::Discovered`] and counted
/// as an INDEPENDENT voice. That is the whole of dig_ecosystem#2648 reachable through the
/// discovery path: a local process that can influence what an introducer returns — or a hostile
/// introducer — supplies as many "independent" voices as the pool has slots.
///
/// Every local family is covered, not just IPv4 loopback:
///
/// - loopback (`127.0.0.0/8`, `::1`), including the IPv4-mapped spelling `::ffff:127.0.0.1`, which
///   `Ipv6Addr::is_loopback` reports as FALSE and which is therefore the form that would survive a
///   loopback check written only against the two obvious ones;
/// - RFC 1918 private IPv4 and IPv6 unique-local (`fc00::/7`);
/// - link-local (`169.254.0.0/16`, `fe80::/10`), which includes the cloud metadata address;
/// - unspecified (`0.0.0.0`, `::`).
///
/// The priority path is unaffected: it dials the loopback deliberately and records what it reaches
/// as `Priority`.
fn is_host_local(addr: &SocketAddr) -> bool {
    let ip = match addr.ip() {
        // An IPv4-mapped IPv6 address is the SAME host as its IPv4 form, so it is judged as one.
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => std::net::IpAddr::V4(v4),
            None => std::net::IpAddr::V6(v6),
        },
        v4 => v4,
    };

    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                // Unique-local, fc00::/7.
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // Link-local unicast, fe80::/10.
                || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
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

    fn sock(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().expect("a valid IP literal"), MAINNET)
    }

    /// **A DISCOVERED local address is refused, in every family it can be spelled in.**
    ///
    /// Each case is a distinct way a local address reaches the discovered set, and the list is
    /// built from the spellings a narrower check would miss rather than from round examples: the
    /// IPv4-mapped `::ffff:127.0.0.1` is not `is_loopback()` as an IPv6 address, and `fd00::1` and
    /// `fe80::1` are local without being loopback at all. A filter written against IPv4 loopback
    /// alone — the behaviour being replaced — passes only the first of these.
    #[test]
    fn every_family_of_local_address_is_refused_from_the_discovered_set() {
        for literal in [
            "127.0.0.1",
            "127.13.1.9",
            "::1",
            "::ffff:127.0.0.1",
            "10.0.0.5",
            "192.168.1.7",
            "172.16.4.2",
            "169.254.169.254",
            "fd00::1",
            "fe80::1",
            "0.0.0.0",
            "::",
        ] {
            assert!(
                is_host_local(&sock(literal)),
                "{literal} is reachable only from this host or its LAN and must never be counted \
                 as an independent voice"
            );
        }
    }

    /// The control, and it is what keeps the filter from being "refuse everything".
    ///
    /// Both families are represented, using documentation ranges that are PUBLIC: a check that
    /// over-matched — treating any IPv6 address as local, say, which `fc00::/7` and `fe80::/10`
    /// masks make easy to get wrong — would empty the discovered set and take the peer tier with
    /// it.
    #[test]
    fn a_public_address_survives_the_local_filter() {
        for literal in [
            "203.0.113.9",
            "8.8.8.8",
            "2001:db8::1",
            "2600::1",
            "::ffff:203.0.113.9",
        ] {
            assert!(
                !is_host_local(&sock(literal)),
                "{literal} is an ordinary public peer and must remain dialable"
            );
        }
    }
}
