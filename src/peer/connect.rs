use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::time::Duration;

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

/// The default peer port for `network`.
pub fn default_port(network: NetworkType) -> u16 {
    match network {
        NetworkType::Mainnet => MAINNET_PORT,
        NetworkType::Testnet11 => TESTNET11_PORT,
    }
}

/// Interpret a `TRUSTED_FULLNODE` value as a peer address on `port`.
///
/// Takes the value as a PARAMETER rather than reading the environment, so a test can exercise
/// every branch without mutating process-global state its neighbours also observe. Returns
/// `None` for an absent or unparseable value; an operator typo must not silently become some
/// other host.
pub fn parse_trusted_fullnode(value: Option<&str>, port: u16) -> Option<SocketAddr> {
    let raw = value?;
    match raw.parse::<std::net::IpAddr>() {
        Ok(ip) => Some(SocketAddr::new(ip, port)),
        Err(_) => {
            log::debug!("TRUSTED_FULLNODE value is not a valid IP: {raw}");
            None
        }
    }
}

/// The operator-configured trusted peer, if `TRUSTED_FULLNODE` names one.
///
/// An environment variable IS operator configuration, so an address it names is honoured. That
/// is the whole difference from the loopback address this module no longer dials on its own:
/// somebody asked for this one.
pub fn trusted_fullnode_from_env(network: NetworkType) -> Option<SocketAddr> {
    parse_trusted_fullnode(
        std::env::var("TRUSTED_FULLNODE").ok().as_deref(),
        default_port(network),
    )
}

/// Whether `addr` is an address a DISCOVERED peer could legitimately live at.
///
/// Introducer answers arrive over unauthenticated DNS, so this treats them as untrusted input.
/// Anything that is not a globally routable unicast address is refused: loopback (the whole of
/// `127.0.0.0/8` is on `lo`, so one process answers on all of it as distinct addresses),
/// unspecified, link-local, unique-local, RFC1918, multicast and broadcast. IPv4-mapped IPv6 is
/// unwrapped first, or `::ffff:127.0.0.1` would walk straight past the v4 checks.
///
/// Operator-named addresses are NOT subject to this — a local full node is the legitimate
/// loopback case, and somebody asked for it.
fn is_routable_peer(addr: &SocketAddr) -> bool {
    let ip = match addr.ip() {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    };
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_unspecified()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast())
        }
        IpAddr::V6(v6) => {
            let [first, second, ..] = v6.octets();
            let is_unique_local = first & 0xfe == 0xfc;
            let is_link_local = first == 0xfe && second & 0xc0 == 0x80;
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || is_unique_local
                || is_link_local)
        }
    }
}

/// `discovered`, keeping only the addresses a real internet peer could be at.
fn routable_only(discovered: Vec<SocketAddr>) -> Vec<SocketAddr> {
    discovered.into_iter().filter(is_routable_peer).collect()
}

/// Resolve candidate peer addresses from the network's DNS introducers.
///
/// The returned list is shuffled — so a caller filling several slots does not hammer whichever
/// introducer answers first — de-duplicated, and filtered to routable unicast addresses
/// ([`is_routable_peer`]). It holds DISCOVERED addresses only: no loopback and no hardcoded
/// priority address. An address nobody configured must never reach the head of this list
/// (dig_ecosystem#2648).
///
/// Fails with [`ChiaQueryError::PeerDiscoveryFailed`] when nothing usable resolves. That error
/// is meaningful and MUST NOT be cached as an empty candidate list: a single dropped DNS
/// exchange would otherwise disable the peer tier for the process's life.
pub async fn discover_addresses(
    network: NetworkType,
    timeout: Duration,
) -> Result<Vec<SocketAddr>, ChiaQueryError> {
    let net = match network {
        NetworkType::Mainnet => Network::default_mainnet(),
        NetworkType::Testnet11 => Network::default_testnet11(),
    };

    let mut addrs = routable_only(net.lookup_all(timeout, BATCH_SIZE).await);
    if addrs.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }
    addrs.shuffle(&mut rand::thread_rng());
    Ok(dedup_preserving_order(addrs))
}

/// The order a caller should dial: operator-trusted addresses first, then discovered ones,
/// each appearing at most once.
///
/// Pure and total, because it is the function the pool's admission order is asserted against.
/// A second implementation of "who comes first" is how the bias this fixes got in.
pub fn candidate_list(trusted: &[SocketAddr], discovered: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut all = trusted.to_vec();
    all.extend(discovered);
    dedup_preserving_order(all)
}

fn dedup_preserving_order(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    addrs.into_iter().filter(|a| seen.insert(*a)).collect()
}

/// Open one connection to one CHOSEN address, giving up after `timeout`.
///
/// Chooses nothing: the caller decides who to dial. Keeping selection out of the dialler is
/// what lets the pool guarantee each address occupies at most one slot.
pub async fn dial_addr(
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

/// Connect to one peer: the operator-trusted node if configured, otherwise a discovered one.
///
/// # Behaviour change (dig_ecosystem#2648)
///
/// This no longer tries `127.0.0.1` before the network. It used to, unconditionally, on every
/// call — so any unprivileged process listening on the peer port was dialled ahead of every real
/// full node, by every caller, on that host. An address NOBODY CONFIGURED must never be
/// preferred to the network; loopback is reached now only when an operator names it, via
/// `TRUSTED_FULLNODE` or [`ChiaQueryConfig::trusted_peers`](crate::ChiaQueryConfig).
///
/// Prefer [`discover_addresses`] + [`candidate_list`] + [`dial_addr`] when connecting more than
/// one peer: a helper that picks for you cannot also promise the picks are distinct.
pub async fn connect_random_peer(
    network: NetworkType,
    tls: &Connector,
    timeout: Duration,
) -> Result<(Peer, SocketAddr, mpsc::Receiver<Message>), ChiaQueryError> {
    let network_id = network.network_id().to_string();
    let trusted: Vec<SocketAddr> = trusted_fullnode_from_env(network).into_iter().collect();
    let discovered = discover_addresses(network, timeout)
        .await
        .unwrap_or_default();

    let candidates = candidate_list(&trusted, discovered);
    if candidates.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }

    for addr in candidates {
        match dial_addr(&network_id, tls, addr, timeout).await {
            Ok((peer, receiver)) => return Ok((peer, addr, receiver)),
            Err(e) => log::debug!("connect to {addr} failed: {e}"),
        }
    }

    Err(ChiaQueryError::PeerDiscoveryFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("a valid socket address")
    }

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MAINNET_PORT);

    /// #2648: loopback reaches the dial order only when an operator asked for it — and then
    /// exactly once, at the head. Asserted against the REAL composition production uses, so a
    /// prepend reintroduced anywhere in it fails here.
    #[test]
    fn loopback_is_absent_from_the_candidate_list_unless_configured() {
        let discovered = vec![addr("1.2.3.4:8444"), addr("5.6.7.8:8444")];

        // Nothing configured: the network's own answer, and nothing else.
        let unconfigured = candidate_list(&[], discovered.clone());
        assert!(
            !unconfigured.contains(&LOOPBACK),
            "loopback must not be dialled unless an operator configured it: {unconfigured:?}"
        );
        assert_eq!(unconfigured, discovered);

        // Configured: present, at the head, and once.
        let configured = candidate_list(&[LOOPBACK], discovered.clone());
        assert_eq!(configured.first(), Some(&LOOPBACK));
        assert_eq!(configured.iter().filter(|a| **a == LOOPBACK).count(), 1);

        // Offered twice — by config AND by discovery — it still occupies one place.
        let both = candidate_list(&[LOOPBACK], vec![LOOPBACK, discovered[0]]);
        assert_eq!(both, vec![LOOPBACK, discovered[0]]);
    }

    /// `TRUSTED_FULLNODE` is operator configuration and still reaches the list.
    #[test]
    fn trusted_fullnode_is_honoured_and_a_bad_value_is_not() {
        assert_eq!(
            parse_trusted_fullnode(Some("127.0.0.1"), MAINNET_PORT),
            Some(LOOPBACK)
        );
        assert_eq!(
            parse_trusted_fullnode(Some("2001:db8::1"), MAINNET_PORT),
            Some(addr("[2001:db8::1]:8444"))
        );
        assert_eq!(
            parse_trusted_fullnode(Some("not-an-ip"), MAINNET_PORT),
            None
        );
        assert_eq!(parse_trusted_fullnode(None, MAINNET_PORT), None);

        let trusted: Vec<SocketAddr> = parse_trusted_fullnode(Some("10.0.0.9"), MAINNET_PORT)
            .into_iter()
            .collect();
        assert_eq!(
            candidate_list(&trusted, vec![addr("1.2.3.4:8444")]),
            vec![addr("10.0.0.9:8444"), addr("1.2.3.4:8444")]
        );
    }

    /// SPEC "Candidate list": discovery contributes introducer-resolved addresses ONLY, and
    /// `127.0.0.1` is a candidate only when an OPERATOR named it.
    ///
    /// The introducers are reached over DNS, which the client does not authenticate: a
    /// DHCP-supplied resolver, a poisoned cache, or a user-level stub can answer with anything.
    /// On Linux the whole of `127.0.0.0/8` is on `lo`, so one unprivileged process answers on
    /// every one of those addresses as a DISTINCT `SocketAddr` — passing every admission guard
    /// in the pool and filling it alone. The routable control is what proves the filter is a
    /// filter and not a blanket refusal.
    #[test]
    fn discovery_keeps_routable_addresses_and_drops_the_rest() {
        let routable = addr("1.2.3.4:8444");
        let routable_v6 = addr("[2606:4700::1]:8444");

        for kept in [routable, routable_v6] {
            assert!(is_routable_peer(&kept), "{kept} is a real internet peer");
        }

        for dropped in [
            addr("127.0.0.1:8444"),         // loopback
            addr("127.0.0.9:8444"),         // the rest of 127.0.0.0/8, all on `lo`
            addr("[::1]:8444"),             // loopback, v6
            addr("[::ffff:127.0.0.1]:8444"), // loopback wearing a v6 address
            addr("0.0.0.0:8444"),           // unspecified
            addr("[::]:8444"),              // unspecified, v6
            addr("10.0.0.1:8444"),          // RFC1918
            addr("192.168.1.5:8444"),       // RFC1918
            addr("169.254.1.1:8444"),       // link-local
            addr("[fe80::1]:8444"),         // link-local, v6
            addr("[fc00::1]:8444"),         // unique-local, v6
            addr("224.0.0.1:8444"),         // multicast
            addr("255.255.255.255:8444"),   // broadcast
        ] {
            assert!(
                !is_routable_peer(&dropped),
                "{dropped} must never be dialled merely because DNS named it"
            );
        }
    }

    /// The filter binds where it matters: on DISCOVERED addresses, never on operator-named ones.
    /// An operator running a local node is the legitimate loopback case and must keep working.
    #[test]
    fn the_filter_applies_to_discovery_and_never_to_operator_configuration() {
        let poisoned = vec![LOOPBACK, addr("10.0.0.1:8444"), addr("1.2.3.4:8444")];

        assert_eq!(
            candidate_list(&[], routable_only(poisoned.clone())),
            vec![addr("1.2.3.4:8444")],
            "only the routable discovered address survives"
        );
        assert_eq!(
            candidate_list(&[LOOPBACK], routable_only(poisoned)),
            vec![LOOPBACK, addr("1.2.3.4:8444")],
            "an operator-named loopback address is still dialled, and still exactly once"
        );
    }

    #[test]
    fn default_port_is_per_network() {
        assert_eq!(default_port(NetworkType::Mainnet), MAINNET_PORT);
        assert_eq!(default_port(NetworkType::Testnet11), TESTNET11_PORT);
    }
}
