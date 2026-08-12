//! A pool of held full-node peer connections, admitting each ADDRESS at most once.
//!
//! # Why distinctness is the pool's job
//!
//! The pool used to fill its slots by making `max_peers` independent calls to
//! [`connect::connect_random_peer`], each of which redid discovery and each of which tried
//! `127.0.0.1:8444` first. On a host where any unprivileged process was listening there, all
//! five slots became five sockets to that ONE process — while the pool reported itself full
//! (dig_ecosystem#2648). Downstream that number is read as chain truth:
//! `router::get_blockchain_state` reports `synced` from [`PeerPoolInner::peak_height`].
//!
//! A helper that returns one already-chosen peer puts both the selection decision and the
//! distinctness decision somewhere the pool cannot see them. So the pool resolves a candidate
//! ADDRESS LIST once and dials from it itself ([`PeerPoolInner::fill`]), skipping any address it
//! already holds. Loopback is a member only when the OPERATOR asked for it — never because
//! something happened to be listening.
//!
//! # Peaks are claims, not facts
//!
//! Each member records its own [`PeakClaim`], exactly as that peer announced it. The shared
//! [`PeerPoolInner::peak_height`] remains the maximum across members, unchanged, because
//! `router` depends on it; the per-member claims are what a caller needs to tell "five peers
//! agree" from "one peer said it five times".

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chia_protocol::{Bytes32, Message, NewPeakWallet, ProtocolMessageTypes};
use chia_traits::Streamable;
use tokio::sync::{mpsc, RwLock};

use chia_wallet_sdk::client::Peer;
use tokio_tungstenite::Connector;

use crate::types::ChiaQueryError;
use crate::NetworkType;

use super::connect;

// ---------------------------------------------------------------------------
// Peak claims
// ---------------------------------------------------------------------------

/// One peer's UNVERIFIED claim about the tip, as announced in its `NewPeakWallet`.
///
/// A claim, never a fact: nothing here has been checked against the chain, and a hostile peer
/// can assert any height it likes. Treat it as evidence from one source and corroborate across
/// [`PeerPoolInner::peer_members`] before believing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeakClaim {
    /// The height the peer claims as the tip.
    pub height: u32,
    /// The header hash the peer claims for that height.
    pub header_hash: Bytes32,
    /// The accumulated chain weight the peer claims.
    pub weight: u128,
}

// ---------------------------------------------------------------------------
// Dialer seam
// ---------------------------------------------------------------------------

/// The future returned by [`PeerDialer::dial`].
///
/// Boxed so the trait stays object-safe without pulling in an async-trait dependency; the pool
/// holds its dialer behind `dyn` so a test can supply one whose answers it controls.
pub type DialFuture<P> =
    Pin<Box<dyn Future<Output = Result<(P, mpsc::Receiver<Message>), ChiaQueryError>> + Send>>;

/// Opens one connection to one CHOSEN address.
///
/// The pool decides who to dial; the dialer only carries it out. That split is the fix: a
/// dialer that also chose could not promise the pool's members are distinct.
pub trait PeerDialer<P>: Send + Sync {
    /// Connect to `addr`, yielding the peer handle and its inbound message channel.
    fn dial(&self, addr: SocketAddr) -> DialFuture<P>;
}

/// The production dialer: a real TLS WebSocket connection to a full node.
pub struct NetworkDialer {
    network_id: String,
    tls: Connector,
    timeout: Duration,
}

impl NetworkDialer {
    /// A dialer for `network`, presenting `tls` and giving up after `timeout`.
    pub fn new(network: NetworkType, tls: Connector, timeout: Duration) -> Self {
        Self {
            network_id: network.network_id().to_string(),
            tls,
            timeout,
        }
    }
}

impl PeerDialer<Peer> for NetworkDialer {
    fn dial(&self, addr: SocketAddr) -> DialFuture<Peer> {
        let network_id = self.network_id.clone();
        let tls = self.tls.clone();
        let timeout = self.timeout;
        Box::pin(async move { connect::dial_addr(&network_id, &tls, addr, timeout).await })
    }
}

// ---------------------------------------------------------------------------
// Members
// ---------------------------------------------------------------------------

/// One held connection, identified by its address.
///
/// Distinctness in this pool IS address distinctness: two members at one address are two
/// sockets to one process, which is the collapse the pool exists to prevent.
#[derive(Clone)]
pub struct PeerMember<P> {
    /// The peer's address, and its identity within the pool.
    pub addr: SocketAddr,
    /// The peer handle itself.
    pub peer: P,
    peak: Arc<RwLock<Option<PeakClaim>>>,
}

impl<P> PeerMember<P> {
    /// THIS peer's own latest claim about the tip, or `None` if it has not announced one yet.
    pub async fn peak(&self) -> Option<PeakClaim> {
        *self.peak.read().await
    }
}

// ---------------------------------------------------------------------------
// PeerRequirement
// ---------------------------------------------------------------------------

/// Whether at least one peer must connect for the pool to be considered usable.
///
/// A client that can fall back to the coinset HTTP tier is still useful with zero
/// peers, so failing construction on peer discovery would deny a keyless reader over a
/// peer-tier problem it does not need (dig_ecosystem#2210).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRequirement {
    /// Peer discovery failing is fatal.
    Required,
    /// An empty pool is acceptable; it refills in the background.
    Optional,
}

// ---------------------------------------------------------------------------
// PeerPool
// ---------------------------------------------------------------------------

/// The pool over the concrete peer type. `PeerPoolInner`'s type parameter exists so the
/// admission rule can be tested with a peer stand-in; the rule itself keys on `SocketAddr`
/// alone and never touches `P`.
pub type PeerPool = PeerPoolInner<Peer>;

/// A pool of held peer connections at DISTINCT addresses.
pub struct PeerPoolInner<P: Clone> {
    members: RwLock<Vec<PeerMember<P>>>,
    next_idx: AtomicUsize,
    /// How many members the pool tries to hold.
    target: usize,
    /// Addresses the OPERATOR named, dialled ahead of anything discovered.
    trusted: Vec<SocketAddr>,
    network: NetworkType,
    connect_timeout: Duration,
    /// The candidate list, resolved ONCE and then fixed for the pool's life.
    ///
    /// Fixed on purpose: re-resolving per fill would let a resolver that is fast or always-up
    /// reappear at the head of every future list, which is the bias in a slower disguise.
    addresses: RwLock<Option<Vec<SocketAddr>>>,
    /// Addresses ejected since they were last admitted, deprioritised on the next fill.
    ///
    /// A peer is ejected because a request to it failed, and the candidate list is fixed, so
    /// without this the very next refill would re-dial the address that just failed — every
    /// time, forever, letting one broken peer hold a slot permanently. Deprioritised, never
    /// banned: once the untried candidates are exhausted a second pass reconsiders them, because
    /// a peer that failed earlier may be the only one left.
    ejected: RwLock<HashSet<SocketAddr>>,
    dialer: Arc<dyn PeerDialer<P>>,
    /// Highest peak height claimed by ANY member. Kept as a shared `AtomicU32` updated with
    /// `fetch_max` because `router::get_blockchain_state` reads it directly.
    peak_height: Arc<AtomicU32>,
}

impl<P: Clone + Send + Sync + 'static> PeerPoolInner<P> {
    /// A pool that dials through `dialer` toward `target` members.
    ///
    /// `addresses` pre-seeds the candidate list; `None` means resolve it from DNS introducers on
    /// the first fill.
    pub fn with_dialer(
        dialer: Arc<dyn PeerDialer<P>>,
        target: usize,
        trusted: Vec<SocketAddr>,
        network: NetworkType,
        connect_timeout: Duration,
        addresses: Option<Vec<SocketAddr>>,
    ) -> Self {
        Self {
            members: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            target,
            trusted,
            network,
            connect_timeout,
            addresses: RwLock::new(addresses),
            ejected: RwLock::new(HashSet::new()),
            dialer,
            peak_height: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Latest peak height claimed across all connected peers, or 0 before any claim.
    ///
    /// This is a MAXIMUM over unverified claims, so with a single member it is that member's
    /// word alone. Corroborate via [`peer_members`](Self::peer_members) before treating it as
    /// settled.
    pub fn peak_height(&self) -> u32 {
        self.peak_height.load(Ordering::Relaxed)
    }

    /// Every current member, cloned.
    ///
    /// The way to tell "several peers agree" from "one peer said it several times": members are
    /// address-distinct by construction, so their [`PeakClaim`]s are independent claims.
    pub async fn peer_members(&self) -> Vec<PeerMember<P>> {
        self.members.read().await.clone()
    }

    /// Round-robin select a peer, or `None` when the pool is empty.
    ///
    /// This is LOAD BALANCING across a now-distinct member set — that distinctness is the whole
    /// fix. It is not a sampler: a quorum MUST NOT be built from repeated calls to this, because
    /// round-robin over N members returns the same N peers in a fixed cycle. Use
    /// [`peer_members`](Self::peer_members).
    pub async fn select_peer(&self) -> Option<(P, SocketAddr)> {
        let members = self.members.read().await;
        if members.is_empty() {
            return None;
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % members.len();
        let member = &members[idx];
        Some((member.peer.clone(), member.addr))
    }

    /// Drop the member at `addr`. The freed slot refills on the next
    /// [`try_refill`](Self::try_refill).
    pub async fn eject_peer(&self, addr: SocketAddr) {
        self.members.write().await.retain(|m| m.addr != addr);
        self.ejected.write().await.insert(addr);
        log::debug!(
            "peer {addr} ejected from pool; will refill on next request (network={:?})",
            self.network,
        );
    }

    /// Whether the pool has at least one usable peer.
    pub async fn has_peers(&self) -> bool {
        !self.members.read().await.is_empty()
    }

    /// How many members the pool currently holds.
    pub async fn len(&self) -> usize {
        self.members.read().await.len()
    }

    /// Whether the pool holds no members.
    pub async fn is_empty(&self) -> bool {
        self.members.read().await.is_empty()
    }

    /// If the pool is under target, dial toward it.
    pub async fn try_refill(&self) {
        if self.len().await >= self.target {
            return;
        }
        self.fill().await;
    }

    /// Dial toward `target`, admitting each address AT MOST ONCE.
    ///
    /// Returns the number of members held afterwards. Falling short is ordinary and is reported
    /// by that number rather than by an error: a pool with three members is degraded, not
    /// broken, and the caller decides what a shortfall means.
    pub async fn fill(&self) -> usize {
        let mut held: HashSet<SocketAddr> =
            self.members.read().await.iter().map(|m| m.addr).collect();
        if held.len() >= self.target {
            // Resolving a candidate list the pool has no room for would spend a DNS round trip
            // to learn nothing.
            return held.len();
        }

        let candidates = self.candidate_addresses().await;
        let ejected = self.ejected.read().await.clone();

        // Fresh candidates first, then previously-ejected ones — a peer that just failed a
        // request must not immediately reclaim the slot it was ejected from while an untried
        // address is available.
        let (fresh, retry): (Vec<_>, Vec<_>) = candidates
            .iter()
            .copied()
            .partition(|a| !ejected.contains(a));

        for addr in fresh.into_iter().chain(retry) {
            if held.len() >= self.target {
                break;
            }
            if held.contains(&addr) {
                continue;
            }
            if self.try_admit(addr, &mut held).await {
                self.ejected.write().await.remove(&addr);
            }
        }

        self.len().await
    }

    /// Dial `addr` and admit it if the pool still has room and does not already hold it.
    ///
    /// Returns whether it was admitted. `held` is marked either way: a dialled address is not
    /// retried within one fill, whatever the outcome.
    async fn try_admit(&self, addr: SocketAddr, held: &mut HashSet<SocketAddr>) -> bool {
        let (peer, receiver) = match self.dialer.dial(addr).await {
            Ok(connection) => connection,
            Err(e) => {
                log::debug!("connect to {addr} failed: {e}");
                return false;
            }
        };
        held.insert(addr);

        let peak = Arc::new(RwLock::new(None));
        {
            let mut members = self.members.write().await;
            // Re-checked under the write lock: a concurrent fill may have admitted this address
            // between the read in `fill` and here, and two members at one address is exactly the
            // duplicate this exists to prevent.
            if members.iter().any(|m| m.addr == addr) || members.len() >= self.target {
                // `peer` and `receiver` drop here WITHOUT a handler being spawned. That drop is
                // load-bearing: a rejected duplicate whose handler still ran would go on feeding
                // the shared peak, so a refused connection would still be reporting its number.
                return false;
            }
            members.push(PeerMember {
                addr,
                peer,
                peak: Arc::clone(&peak),
            });
        }
        self.spawn_receiver_handler(peak, receiver);
        true
    }

    /// The dial order, resolved once and then reused.
    async fn candidate_addresses(&self) -> Vec<SocketAddr> {
        if let Some(addresses) = self.addresses.read().await.as_ref() {
            return addresses.clone();
        }

        let discovered = connect::discover_addresses(self.network, self.connect_timeout)
            .await
            .unwrap_or_else(|e| {
                log::warn!("peer discovery failed: {e}");
                Vec::new()
            });
        let candidates = connect::candidate_list(&self.trusted, discovered);

        let mut slot = self.addresses.write().await;
        // A concurrent fill may have resolved first; either list is valid, so keep whichever
        // landed to preserve the resolve-once guarantee.
        slot.get_or_insert(candidates).clone()
    }

    /// Read a member's inbound messages, recording its own peak claim and folding it into the
    /// shared maximum. Spawned only for an ADMITTED member.
    fn spawn_receiver_handler(
        &self,
        peak_slot: Arc<RwLock<Option<PeakClaim>>>,
        mut receiver: mpsc::Receiver<Message>,
    ) {
        let shared_peak = Arc::clone(&self.peak_height);
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if msg.msg_type != ProtocolMessageTypes::NewPeakWallet {
                    continue;
                }
                let Ok(new_peak) = NewPeakWallet::from_bytes(&msg.data) else {
                    continue;
                };
                *peak_slot.write().await = Some(PeakClaim {
                    height: new_peak.height,
                    header_hash: new_peak.header_hash,
                    weight: new_peak.weight,
                });
                let prev = shared_peak.fetch_max(new_peak.height, Ordering::Relaxed);
                if new_peak.height > prev {
                    log::debug!("new peak claimed by peer: {}", new_peak.height);
                }
            }
        });
    }
}

impl PeerPool {
    /// Spin up the pool, dialling toward `max_peers` DISTINCT full-node addresses.
    ///
    /// `trusted_peers` are dialled ahead of anything discovered, joined by `TRUSTED_FULLNODE`
    /// when set. Because admission is address-distinct, a configured peer occupies AT MOST ONE
    /// slot by construction — that, not a special case, is what stops a local node crowding out
    /// the pool.
    ///
    /// Under [`PeerRequirement::Required`] at least one peer must connect, otherwise
    /// [`ChiaQueryError::PeerDiscoveryFailed`]; under [`PeerRequirement::Optional`] an empty
    /// pool is returned and refills later.
    pub async fn new(
        network: NetworkType,
        tls: Connector,
        max_peers: usize,
        trusted_peers: Vec<SocketAddr>,
        requirement: PeerRequirement,
        connect_timeout: Duration,
    ) -> Result<Self, ChiaQueryError> {
        let mut trusted = trusted_peers;
        if let Some(from_env) = connect::trusted_fullnode_from_env(network) {
            trusted.push(from_env);
        }

        let dialer = Arc::new(NetworkDialer::new(network, tls, connect_timeout));
        let pool = Self::with_dialer(dialer, max_peers, trusted, network, connect_timeout, None);
        pool.fill().await;

        if pool.is_empty().await {
            if requirement == PeerRequirement::Required {
                return Err(ChiaQueryError::PeerDiscoveryFailed);
            }
            log::warn!("no peers connected; serving from the coinset fallback until one does");
        }

        Ok(pool)
    }
}

#[cfg(test)]
mod tests;
