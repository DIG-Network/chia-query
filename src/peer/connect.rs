use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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

/// The IPv4 address an IPv6 literal carries, for THE ENCODINGS LISTED BELOW.
///
/// Four encodings are unwrapped here: IPv4-mapped `::ffff:a.b.c.d`, IPv4-compatible `::a.b.c.d`,
/// IPv4-translated `::ffff:0:a.b.c.d` (RFC 2765/6052), and the well-known NAT64 prefix
/// `64:ff9b::a.b.c.d` (RFC 6052 §2.1). Each puts a real IPv4 destination inside a literal that
/// reads as IPv6, so a check that unwraps only the first can be walked past by using any of the
/// others: `64:ff9b::7f00:1` and `::ffff:0:7f00:1` are `127.0.0.1` wearing a v6 literal, and a
/// check that reads them as ordinary global unicast admits as many of them as an introducer cares
/// to name. Whether a given host's stack translates them locally, hands them to a NAT64 gateway,
/// or fails to route them at all, none of them is an address a public full node can be reached
/// at — which is the only thing this filter needs to be right about.
///
/// It is NOT "every encoding", and that claim cannot be made honestly. RFC 6052 also permits
/// NETWORK-SPECIFIC prefixes at /32, /40, /48, /56 and /64 — any prefix a local operator chooses
/// — and those are unenumerable from an address literal alone: the same 128 bits are a network's
/// own translation prefix on one host and an ordinary global unicast address on another. They are
/// out of scope here and are refused, where they can be refused at all, by the positive
/// global-unicast rule in [`is_routable_peer`] rather than by unwrapping. The one such prefix that
/// IS fixed by an RFC — the local-use `64:ff9b:1::/48` of RFC 8215 — is refused outright there,
/// because its embedded-IPv4 offset depends on the deployed prefix length and so cannot be
/// unwrapped, and because it can never be a public full node.
///
/// `Ipv6Addr::to_ipv4` is deliberately NOT used, because it maps `::1` to `0.0.0.1` — an address
/// that is not `Ipv4Addr::is_loopback`, so unwrapping loopback through it discards the very fact
/// the check is about. Today that substitution would still be refused, by the `0.0.0.0/8` rule in
/// [`is_non_global_ipv4`] rather than by the loopback one; the two are independent and the second
/// is not a reason to rely on the first. `::` and `::1` are excluded here and refused by the v6
/// checks, which is where they belong.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(mapped) = v6.to_ipv4_mapped() {
        return Some(mapped);
    }
    if v6.is_unspecified() || v6.is_loopback() {
        return None;
    }
    let s = v6.segments();
    let last32 = Ipv4Addr::from(((s[6] as u32) << 16) | s[7] as u32);
    // ::/96 (IPv4-compatible), ::ffff:0:0:0/96 (IPv4-translated, RFC 2765) and 64:ff9b::/96
    // (RFC 6052 well-known NAT64 prefix). IPv4-mapped ::ffff:0:0/96 was already taken above.
    let is_v4_compatible = s[0..6] == [0, 0, 0, 0, 0, 0];
    // `::ffff:0:a.b.c.d` puts the `ffff` one group EARLIER than IPv4-mapped `::ffff:a.b.c.d`
    // does; the two prefixes differ by exactly that shift, which is why one is not the other.
    let is_v4_translated = s[0..4] == [0, 0, 0, 0] && s[4] == 0xffff && s[5] == 0;
    let is_nat64_wellknown = s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0];
    (is_v4_compatible || is_v4_translated || is_nat64_wellknown).then_some(last32)
}

/// Whether `v4` is in a range no peer on the public internet can be reached at.
fn is_non_global_ipv4(v4: Ipv4Addr) -> bool {
    let [a, b, c, _] = v4.octets();
    v4.is_loopback()
        || v4.is_unspecified()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_multicast()
        || v4.is_broadcast()
        || a == 0                                    // 0.0.0.0/8, "this network"
        || (a == 100 && (64..128).contains(&b))      // 100.64.0.0/10, carrier-grade NAT
        || (a == 192 && b == 0 && c == 2)            // 192.0.2.0/24, TEST-NET-1
        || (a == 198 && (18..20).contains(&b))       // 198.18.0.0/15, benchmarking
        || (a == 198 && b == 51 && c == 100)         // 198.51.100.0/24, TEST-NET-2
        || (a == 203 && b == 0 && c == 113)          // 203.0.113.0/24, TEST-NET-3
        || a >= 240 // 240.0.0.0/4, reserved
}

/// Whether `addr` is an address a DISCOVERED peer could legitimately live at.
///
/// Introducer answers arrive over unauthenticated DNS, so this treats them as untrusted input,
/// and the rule is positive: an address is dialled only when it is a globally routable unicast
/// address. Refused, therefore: loopback (the whole of `127.0.0.0/8` is on `lo`, so one process
/// answers on all of it as distinct addresses), unspecified, link-local, unique-local, RFC1918,
/// carrier-grade NAT, the documentation/benchmark/reserved ranges, multicast and broadcast — in
/// both address families, and through each of the IPv6 encodings [`embedded_ipv4`] lists as
/// carrying an IPv4 address. Encodings outside that list are not unwrapped and are refused, when
/// they can be, only by the positive rule itself; [`embedded_ipv4`] says which and why.
///
/// Operator-named addresses are NOT subject to this — a local full node is the legitimate
/// loopback case, and somebody asked for it.
fn is_routable_peer(addr: &SocketAddr) -> bool {
    let ip = match addr.ip() {
        IpAddr::V6(v6) => embedded_ipv4(v6).map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    };
    match ip {
        IpAddr::V4(v4) => !is_non_global_ipv4(v4),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            let [first, second, ..] = v6.octets();
            let is_unique_local = first & 0xfe == 0xfc;
            let is_link_local = first == 0xfe && second & 0xc0 == 0x80;
            // 100::/64 discard-only, 2001:db8::/32 documentation, 2001::/32 Teredo and
            // 2002::/16 6to4 are all reachable-looking and none of them is a full node.
            let is_discard = s[0] == 0x0100 && s[1..4] == [0, 0, 0];
            let is_documentation = s[0] == 0x2001 && s[1] == 0x0db8;
            let is_teredo = s[0] == 0x2001 && s[1] == 0;
            let is_6to4 = s[0] == 0x2002;
            // fec0::/10, the deprecated site-local range: still routed inside many networks, and
            // an introducer naming one is naming somebody's LAN.
            let is_site_local = first == 0xfe && second & 0xc0 == 0xc0;
            // 3ffe::/16, the returned 6bone allocation, and 2001:20::/28, ORCHIDv2 — neither is
            // an address any full node can be reached at.
            let is_6bone = s[0] == 0x3ffe;
            let is_orchid = s[0] == 0x2001 && (s[1] & 0xfff0) == 0x0020;
            // 64:ff9b:1::/48, RFC 8215's LOCAL-USE NAT64 prefix. Refused whole rather than
            // unwrapped: the embedded IPv4 sits at an offset that depends on the deployed prefix
            // length, so there is no fixed place to look, and a local-use translation prefix is
            // never where a public full node lives. Measured filling every slot in a five-peer
            // pool, because `embedded_ipv4` declines it and the generic unicast rule allowed it.
            let is_nat64_local_use = s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 0x0001;
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || is_unique_local
                || is_link_local
                || is_site_local
                || is_discard
                || is_documentation
                || is_teredo
                || is_6to4
                || is_6bone
                || is_orchid
                || is_nat64_local_use)
        }
    }
}

/// Resolve candidate peer addresses from the network's DNS introducers.
///
/// The returned list is shuffled — so a caller filling several slots does not hammer whichever
/// introducer answers first — de-duplicated, and filtered to routable unicast addresses (see
/// `is_routable_peer`). It holds DISCOVERED addresses only: no loopback and no hardcoded
/// priority address. An address nobody configured must never reach the head of this list
/// (dig_ecosystem#2648).
///
/// # Two filter applications, on two paths, both required
///
/// Routability is enforced here AND in [`candidate_list`], and they are not copies of one guard.
/// This is the PUBLIC resolver an external consumer building its own pool calls directly, so
/// removing the filter here hands that consumer an unfiltered list. `candidate_list` is the
/// composition every dial order in this crate is built by, including one seeded from a
/// caller-supplied [`AddressSource`](super::pool::AddressSource) that never reaches this
/// function at all. Each is reachable without the other and each is separately observable; do
/// not "simplify" them together.
///
/// A consumer building its own pool wants this plus [`candidate_list`] plus [`dial_addr`] —
/// resolve a list, compose the dial order, dial a CHOSEN address — rather than
/// [`connect_random_peer`], which chooses for you and therefore cannot promise its picks are
/// distinct.
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

    finish_discovery(net.lookup_all(timeout, BATCH_SIZE).await)
}

/// Everything [`discover_addresses`] does to a resolved list, split out so all of it is reachable
/// without a resolver.
///
/// The network call is the one line of that function a test cannot execute, so it is the only
/// line left in it. Filtering, the failed-resolution error and de-duplication are decisions, and
/// a decision that only ever runs against real DNS is a decision no test protects.
fn finish_discovery(resolved: Vec<SocketAddr>) -> Result<Vec<SocketAddr>, ChiaQueryError> {
    let mut addrs = routable_only(resolved);
    if addrs.is_empty() {
        return Err(ChiaQueryError::PeerDiscoveryFailed);
    }
    addrs.shuffle(&mut rand::thread_rng());
    Ok(dedup_preserving_order(addrs))
}

/// `discovered`, keeping only the addresses a real internet peer could be at.
fn routable_only(discovered: Vec<SocketAddr>) -> Vec<SocketAddr> {
    discovered.into_iter().filter(is_routable_peer).collect()
}

/// The order a caller should dial: operator-trusted addresses first, then the ROUTABLE
/// discovered ones, each appearing at most once.
///
/// Routability is enforced here because this is the function every dial order in this crate is
/// built by — see [`discover_addresses`] for why the resolver filters as well, and why that is
/// defence in depth rather than a duplicated guard. The pool composes its candidates through
/// this function, so an address that arrives from a caller-supplied
/// [`AddressSource`](super::pool::AddressSource) — a seam that is `pub`, and that never touches
/// DNS discovery — is filtered on exactly the same terms as an introducer answer. A filter that
/// sat in discovery alone would leave the whole public path unguarded, which is the shape of the
/// original collapse (dig_ecosystem#2648).
///
/// `trusted` is deliberately exempt: an operator naming their own local full node is the
/// legitimate loopback case, and somebody asked for it.
///
/// Pure and total, because it is the function the pool's admission order is asserted against.
/// A second implementation of "who comes first" is how the bias this fixes got in.
pub fn candidate_list(trusted: &[SocketAddr], discovered: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut all = trusted.to_vec();
    all.extend(discovered.into_iter().filter(is_routable_peer));
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
            addr("127.0.0.1:8444"),          // loopback
            addr("127.0.0.9:8444"),          // the rest of 127.0.0.0/8, all on `lo`
            addr("[::1]:8444"),              // loopback, v6
            addr("[::ffff:127.0.0.1]:8444"), // loopback wearing a v6 address
            addr("0.0.0.0:8444"),            // unspecified
            addr("[::]:8444"),               // unspecified, v6
            addr("10.0.0.1:8444"),           // RFC1918
            addr("192.168.1.5:8444"),        // RFC1918
            addr("169.254.1.1:8444"),        // link-local
            addr("[fe80::1]:8444"),          // link-local, v6
            addr("[fc00::1]:8444"),          // unique-local, v6
            addr("224.0.0.1:8444"),          // multicast
            addr("255.255.255.255:8444"),    // broadcast
        ] {
            assert!(
                !is_routable_peer(&dropped),
                "{dropped} must never be dialled merely because DNS named it"
            );
        }
    }

    /// The OTHER encodings that carry an IPv4 address into an IPv6 literal.
    ///
    /// `::ffff:127.0.0.1` was already unwrapped; its siblings were not, so the same loopback
    /// address written three other ways walked straight past every v4 check. The rationale is
    /// not about how any one host routes them — a locally-originated translated packet lands on
    /// the TRANSLATOR's loopback, not on yours. It is simply that none of these literals can be a
    /// public full node, while an introducer can name as many of them as it likes and the pool
    /// will count each as a distinct peer.
    ///
    /// The globally-routable member of each family is the control: unwrapping must be a filter
    /// on what is embedded, not a blanket refusal of the encoding.
    #[test]
    fn an_ipv4_address_wearing_an_ipv6_encoding_is_judged_by_the_ipv4_inside_it() {
        for dropped in [
            addr("[::7f00:1]:8444"),        // ::/96 IPv4-compatible loopback
            addr("[::a00:1]:8444"),         // ::/96 IPv4-compatible RFC1918
            addr("[64:ff9b::7f00:1]:8444"), // NAT64 well-known prefix over loopback
            addr("[64:ff9b::a00:1]:8444"),  // NAT64 well-known prefix over RFC1918
            addr("[::ffff:0:7f00:1]:8444"), // IPv4-translated (RFC 2765) over loopback
            addr("[::ffff:0:a00:1]:8444"),  // IPv4-translated over RFC1918
        ] {
            assert!(
                !is_routable_peer(&dropped),
                "{dropped} carries a non-routable IPv4 address and must be refused"
            );
        }

        assert!(
            is_routable_peer(&addr("[64:ff9b::102:304]:8444")),
            "NAT64 over a real internet address is a legitimate peer on an IPv6-only host"
        );
        assert!(
            is_routable_peer(&addr("[::ffff:1.2.3.4]:8444")),
            "an IPv4-mapped routable address is still a real internet peer"
        );
        assert!(
            is_routable_peer(&addr("[::ffff:0:102:304]:8444")),
            "IPv4-translated over a real internet address is still a real internet peer"
        );
    }

    /// The ranges an introducer can name that are neither loopback nor an IPv4 in disguise, and
    /// that the generic global-unicast rule let through.
    ///
    /// `64:ff9b:1::/48` is the sharp one and was MEASURED filling all five slots of a five-peer
    /// pool: `embedded_ipv4` declines it, because RFC 8215 leaves the embedded IPv4's offset up
    /// to the deployed prefix length, and every remaining v6 check passed it. Refusing the whole
    /// /48 is the only answer available, and it is the right one — a local-use translation prefix
    /// is not where a public full node lives.
    ///
    /// `64:ff9b::102:304` is the control on the very next prefix: refusing the local-use /48 must
    /// not take the well-known /96 with it.
    #[test]
    fn local_use_and_retired_ipv6_ranges_are_not_globally_routable() {
        for dropped in [
            addr("[64:ff9b:1::7f00:1]:8444"), // RFC 8215 local-use NAT64, over loopback
            addr("[64:ff9b:1::1]:8444"),      // RFC 8215 local-use NAT64, no readable v4
            addr("[64:ff9b:1:ffff::2]:8444"), // elsewhere in the same /48
            addr("[fec0::1]:8444"),           // deprecated site-local
            addr("[feff::1]:8444"),           // the top of fec0::/10
            addr("[3ffe::1]:8444"),           // returned 6bone allocation
            addr("[2001:20::1]:8444"),        // ORCHIDv2
            addr("[2001:2f:ffff::1]:8444"),   // the top of 2001:20::/28
        ] {
            assert!(
                !is_routable_peer(&dropped),
                "{dropped} is not an address a public full node can be reached at"
            );
        }

        for kept in [
            addr("[64:ff9b::102:304]:8444"), // well-known NAT64 over a real internet address
            addr("[2001:30::1]:8444"),       // just past ORCHIDv2
            addr("[3ffd::1]:8444"),          // adjacent to the 6bone range, and unaffected
            addr("[2606:4700::1]:8444"),     // an ordinary global unicast address
        ] {
            assert!(is_routable_peer(&kept), "{kept} is a real internet peer");
        }
    }

    /// The non-global ranges beyond the obvious ones. Each is reachable-looking, none is a full
    /// node, and the doc comment claims a POSITIVE rule — only globally routable unicast is
    /// dialled — which is false while any of these passes.
    #[test]
    fn reserved_and_documentation_ranges_are_not_globally_routable() {
        for dropped in [
            addr("0.1.2.3:8444"),           // 0.0.0.0/8, "this network"
            addr("100.64.0.1:8444"),        // carrier-grade NAT
            addr("192.0.2.1:8444"),         // TEST-NET-1
            addr("198.18.0.1:8444"),        // benchmarking
            addr("198.51.100.1:8444"),      // TEST-NET-2
            addr("203.0.113.1:8444"),       // TEST-NET-3
            addr("240.0.0.1:8444"),         // reserved
            addr("[2001:db8::1]:8444"),     // documentation
            addr("[100::1]:8444"),          // discard-only
            addr("[2001::1]:8444"),         // Teredo
            addr("[2002:102:304::1]:8444"), // 6to4
        ] {
            assert!(
                !is_routable_peer(&dropped),
                "{dropped} is not a globally routable unicast address"
            );
        }

        // Controls, so the widened refusal stays a filter: the neighbours of two of those
        // prefixes are ordinary internet addresses.
        for kept in [
            addr("100.63.255.255:8444"),
            addr("100.128.0.1:8444"),
            addr("223.255.255.254:8444"), // the last unicast address below multicast
            addr("[2001:db9::1]:8444"),
            addr("[2003::1]:8444"),
        ] {
            assert!(is_routable_peer(&kept), "{kept} is a real internet peer");
        }
    }

    /// The filter binds where it matters: on DISCOVERED addresses, never on operator-named ones,
    /// and it binds inside `candidate_list` — the one function every dial order is built by.
    ///
    /// An operator running a local node is the legitimate loopback case and must keep working.
    #[test]
    fn the_filter_applies_to_discovery_and_never_to_operator_configuration() {
        let poisoned = vec![LOOPBACK, addr("10.0.0.1:8444"), addr("1.2.3.4:8444")];

        assert_eq!(
            candidate_list(&[], poisoned.clone()),
            vec![addr("1.2.3.4:8444")],
            "only the routable discovered address survives"
        );
        assert_eq!(
            candidate_list(&[LOOPBACK], poisoned),
            vec![LOOPBACK, addr("1.2.3.4:8444")],
            "an operator-named loopback address is still dialled, and still exactly once"
        );
        assert_eq!(
            candidate_list(&[addr("10.0.0.9:8444")], vec![addr("10.0.0.9:8444")]),
            vec![addr("10.0.0.9:8444")],
            "a private address an operator named is kept; the same address discovered is not"
        );
    }

    /// The PUBLIC resolver filters too, and a resolution that yields nothing dialable is a
    /// FAILURE rather than an empty success.
    ///
    /// This is the seam a consumer building its own pool over this crate calls directly
    /// (dig-node#213), so it has to be safe by default: handing back an unfiltered list would
    /// give that consumer the very loopback bias it exists to escape. The distinction between
    /// failure and emptiness is equally load-bearing — the pool caches a successful resolution
    /// for its life and retries a failed one, so "resolved only junk" reported as a success
    /// would disable the peer tier permanently.
    #[test]
    fn a_resolution_is_filtered_and_an_unusable_one_is_a_failure() {
        let real = addr("1.2.3.4:8444");
        let kept = finish_discovery(vec![
            addr("127.0.0.1:8444"),
            real,
            addr("10.0.0.1:8444"),
            real,
        ])
        .expect("a resolution containing a real peer succeeds");
        assert_eq!(kept, vec![real], "filtered, and de-duplicated");

        assert!(
            matches!(
                finish_discovery(vec![addr("127.0.0.1:8444"), addr("192.168.0.1:8444")]),
                Err(ChiaQueryError::PeerDiscoveryFailed)
            ),
            "a resolution of nothing dialable must not be cacheable as a success"
        );
        assert!(matches!(
            finish_discovery(Vec::new()),
            Err(ChiaQueryError::PeerDiscoveryFailed)
        ));
    }

    #[test]
    fn default_port_is_per_network() {
        assert_eq!(default_port(NetworkType::Mainnet), MAINNET_PORT);
        assert_eq!(default_port(NetworkType::Testnet11), TESTNET11_PORT);
    }
}
