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
use std::time::{Duration, Instant};

use chia_protocol::{Bytes32, Message, NewPeakWallet, ProtocolMessageTypes};
use chia_traits::Streamable;
use tokio::sync::{mpsc, Mutex, RwLock};

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
// Address-source seam
// ---------------------------------------------------------------------------

/// The future returned by [`AddressSource::discover`].
pub type DiscoverFuture =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, ChiaQueryError>> + Send>>;

/// Where the pool's discovered candidate addresses come from.
///
/// Behind a trait so the pool's own seeding path — composition of trusted with discovered, the
/// resolve-once cache, and the retry after a failure — is reachable offline. Those are the parts
/// no fixed address list can exercise, and the parts a wrong answer is most expensive in.
///
/// Failure MUST be distinguishable from an empty answer: the pool caches a SUCCESSFUL resolution
/// for its life and retries a failed one.
pub trait AddressSource: Send + Sync {
    /// Resolve peer addresses, or report why not.
    fn discover(&self) -> DiscoverFuture;
}

/// The production source: the network's DNS introducers.
pub struct IntroducerAddresses {
    network: NetworkType,
    timeout: Duration,
}

impl IntroducerAddresses {
    /// Resolve `network`'s introducers, giving up after `timeout`.
    pub fn new(network: NetworkType, timeout: Duration) -> Self {
        Self { network, timeout }
    }
}

impl AddressSource for IntroducerAddresses {
    fn discover(&self) -> DiscoverFuture {
        let (network, timeout) = (self.network, self.timeout);
        Box::pin(async move { connect::discover_addresses(network, timeout).await })
    }
}

// ---------------------------------------------------------------------------
// Fill cost
// ---------------------------------------------------------------------------

/// How many addresses one fill dials AT ONCE.
///
/// Ten, because that is the batch width introducer resolution already uses
/// ([`connect`]); a serial sweep at an 8-second connect timeout costs 8 seconds per
/// unreachable address, and the mainnet introducers currently answer with over a hundred.
const DIAL_BATCH: usize = 10;

/// The most addresses ONE fill will dial, however long the candidate list is.
///
/// Without a cap a host that cannot reach `target` distinct peers re-walks the entire candidate
/// list on every fill forever, because the pool is permanently under target — the very condition
/// address-distinct admission creates. Two batches bound the worst case at roughly twice the
/// connect timeout.
///
/// The budget bounds ONE fill's cost; it must never bound which addresses are reachable at all.
/// That distinction is what [`PeerPoolInner::dial_cursor`] exists for: successive fills advance
/// through the candidate list rather than re-dialling its first twenty forever.
const MAX_DIALS_PER_FILL: usize = 2 * DIAL_BATCH;

/// How long after a fill that fell SHORT before another sweep is worth attempting.
///
/// A fill that could not reach `target` just proved the network cannot currently supply it.
/// Repeating it per request turns every read into a dial sweep and points up to a hundred
/// outbound handshakes per read at real full nodes, which is behaviour worth being banned for.
///
/// It gates a SHORT pool only, never an EMPTY one. Short is a diversity problem and can wait a
/// minute; empty means every read is served by the one centralized HTTP endpoint the peer tier
/// exists to avoid depending on, and waiting cannot improve it.
const REFILL_COOLDOWN: Duration = Duration::from_secs(60);

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
    discovery: Arc<dyn AddressSource>,
    /// The candidate list, resolved ONCE — on SUCCESS — and then fixed for the pool's life.
    ///
    /// Fixed on purpose: re-resolving per fill would let a resolver that is fast or always-up
    /// reappear at the head of every future list, which is the bias in a slower disguise.
    ///
    /// A FAILED resolution is never stored. Caching one would let a single dropped DNS exchange
    /// at boot — a DHCP or VPN race, a captive portal, an off-path attacker — downgrade the
    /// client to the one centralized HTTP fallback permanently, with no retry and no TTL.
    addresses: RwLock<Option<Vec<SocketAddr>>>,
    /// Addresses ejected since they were last admitted, deprioritised on the next fill.
    ///
    /// A peer is ejected because a request to it failed, and the candidate list is fixed, so
    /// without this the very next refill would re-dial the address that just failed — every
    /// time, forever, letting one broken peer hold a slot permanently. Deprioritised, never
    /// banned: once the untried candidates are exhausted a second pass reconsiders them, because
    /// a peer that failed earlier may be the only one left.
    ejected: RwLock<HashSet<SocketAddr>>,
    /// Where the NEXT fill starts in the candidate list.
    ///
    /// The candidate list is fixed for the pool's life and one fill dials at most
    /// `MAX_DIALS_PER_FILL` of it, so without a cursor every fill considers the same prefix — and
    /// on a host that cannot reach any of those twenty, the other hundred are never dialled again
    /// for the process's life while each read re-dials the identical twenty that just failed.
    /// Advancing by the window makes every candidate reachable across successive fills without
    /// widening what one fill costs.
    dial_cursor: AtomicUsize,
    /// Held for the duration of a [`try_refill`](PeerPoolInner::try_refill) sweep.
    ///
    /// Single-flight, not mutual exclusion: a caller that cannot take it returns at once rather
    /// than queueing, so K concurrent reads over a short pool cost ONE sweep and not K.
    refilling: Mutex<()>,
    /// When a fill last ended under `target`, gating the next sweep by `REFILL_COOLDOWN`.
    last_short_fill: RwLock<Option<Instant>>,
    dialer: Arc<dyn PeerDialer<P>>,
    /// Highest peak height claimed by ANY member. Kept as a shared `AtomicU32` updated with
    /// `fetch_max` because `router::get_blockchain_state` reads it directly.
    peak_height: Arc<AtomicU32>,
}

impl<P: Clone + Send + Sync + 'static> PeerPoolInner<P> {
    /// A pool that dials through `dialer` toward `target` members, over addresses from
    /// `discovery` behind the operator-named `trusted` ones.
    pub fn with_dialer(
        dialer: Arc<dyn PeerDialer<P>>,
        discovery: Arc<dyn AddressSource>,
        target: usize,
        trusted: Vec<SocketAddr>,
        network: NetworkType,
    ) -> Self {
        Self {
            members: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            target,
            trusted,
            network,
            discovery,
            addresses: RwLock::new(None),
            ejected: RwLock::new(HashSet::new()),
            dial_cursor: AtomicUsize::new(0),
            refilling: Mutex::new(()),
            last_short_fill: RwLock::new(None),
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
    /// Members are address-distinct by construction, which is what stops one peer being counted
    /// several times over. Address-distinctness is NECESSARY for independent claims and it is
    /// NOT SUFFICIENT: one operator can hold several addresses, and seeders list any node that
    /// answers, so five distinct addresses may be five processes, one process, or one operator.
    /// The guarantee also assumes an honest dialer — admission keys on the address the pool
    /// REQUESTED, and [`with_dialer`](Self::with_dialer) lets a caller supply the connection.
    ///
    /// So this is the raw evidence a corroborating caller needs, not a quorum. Weighing how
    /// independent these claims actually are is that caller's job.
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

    /// A peer to serve a request with, refilling the pool around the answer.
    ///
    /// A fill is a bounded but real sweep of outbound TLS dials, so where it sits relative to
    /// selection decides what a read costs. With a usable member in hand the sweep buys
    /// DIVERSITY, which the caller waiting on a read does not need right now — so it runs BEHIND
    /// the answer, detached. With nothing to serve it buys AVAILABILITY and is the only thing
    /// that can help, so it runs IN FRONT and the caller waits for it.
    ///
    /// The decision lives here rather than at the call site because it is a property of the
    /// pool's own cost model, and because a caller cannot be asked to rediscover it.
    /// [`try_refill`](Self::try_refill) is single-flight either way, which is what keeps the
    /// detached case from accumulating sweeps.
    pub async fn select_refilling(self: &Arc<Self>) -> Option<(P, SocketAddr)> {
        if let Some(picked) = self.select_peer().await {
            let pool = Arc::clone(self);
            tokio::spawn(async move { pool.try_refill().await });
            return Some(picked);
        }

        self.try_refill().await;
        self.select_peer().await
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

    /// Dial toward `target` if the pool is under it — AT MOST ONE sweep at a time, and, for a
    /// pool that is merely SHORT, at most one per `REFILL_COOLDOWN` (60s).
    ///
    /// This is the entry point every request path uses, and both bounds are about the request
    /// path rather than about correctness. A pool that cannot reach `target` distinct peers is
    /// under target PERMANENTLY, so an ungated refill re-walks the candidate list on every read,
    /// once per concurrent read.
    ///
    /// An EMPTY pool is exempt from the cooldown. Between "short" and "empty" the cooldown is
    /// trading diversity for handshake volume in the first case and the entire peer tier for one
    /// centralized HTTP endpoint in the second — and a pool that fell short at construction, or
    /// that was drained to zero by ejections, would otherwise be blacked out for a minute at
    /// exactly the moment it has nothing to serve with.
    ///
    /// Whether there is ROOM is not decided here: [`fill`](Self::fill) decides it, from the
    /// member set, under concurrency. Repeating that check here would put the same policy in two
    /// places where either alone would satisfy any test of it.
    pub async fn try_refill(&self) {
        let Ok(_in_flight) = self.refilling.try_lock() else {
            return;
        };
        if !self.is_empty().await {
            if let Some(last_short) = *self.last_short_fill.read().await {
                if last_short.elapsed() < REFILL_COOLDOWN {
                    return;
                }
            }
        }

        if self.fill().await < self.target {
            *self.last_short_fill.write().await = Some(Instant::now());
        }
    }

    /// Dial toward `target`, admitting each address AT MOST ONCE.
    ///
    /// Dials in CONCURRENT batches of `DIAL_BATCH` (10) and considers at most
    /// `MAX_DIALS_PER_FILL` (20) candidates, so a long list costs a bounded amount of time and a
    /// bounded number of handshakes; the next fill resumes where this one stopped, so the bound
    /// on one fill never becomes a bound on which candidates are ever tried.
    ///
    /// Occupancy is re-read from the member set at the top of every batch, and that ONE reading
    /// serves both jobs: whether there is still room, and which addresses must not be dialled
    /// again. Deriving it from this fill's own admissions instead would make a sibling fill's
    /// progress invisible — two concurrent fills would each dial a full second batch at real
    /// full nodes while the pool was already at target.
    ///
    /// Returns the number of members held afterwards. Falling short is ordinary and is reported
    /// by that number rather than by an error: a pool with three members is degraded, not
    /// broken, and the caller decides what a shortfall means.
    pub async fn fill(&self) -> usize {
        let mut window: Option<Vec<SocketAddr>> = None;
        let mut batch_start = 0;

        loop {
            let held: HashSet<SocketAddr> =
                self.members.read().await.iter().map(|m| m.addr).collect();
            if held.len() >= self.target {
                break;
            }
            // Resolved lazily, so a pool with no room never spends a DNS round trip to learn
            // that. The window is fixed on the first pass: re-deriving it mid-fill would let
            // this fill's own admissions shift the addresses it has yet to try.
            let window = match &window {
                Some(w) => w,
                None => window.insert(self.dial_window().await),
            };
            let Some(chunk) = window.get(batch_start..) else {
                break;
            };
            let chunk = &chunk[..chunk.len().min(DIAL_BATCH)];
            if chunk.is_empty() {
                break;
            }
            batch_start += chunk.len();

            let batch: Vec<SocketAddr> = chunk
                .iter()
                .copied()
                .filter(|addr| !held.contains(addr))
                .collect();
            if batch.is_empty() {
                continue;
            }

            // Concurrently, not in sequence. A batch of ten unreachable addresses at the default
            // eight-second connect timeout is eight seconds dialled this way and eighty dialled
            // serially — and this runs on the request path, so a silent regression to serial is
            // the difference between a bounded fill and a read that appears to hang.
            let connections = futures_util::future::join_all(
                batch
                    .iter()
                    .map(|addr| async move { (*addr, self.dialer.dial(*addr).await) }),
            )
            .await;

            for (addr, outcome) in connections {
                match outcome {
                    Ok(connection) => {
                        if self.try_admit(addr, connection).await {
                            self.ejected.write().await.remove(&addr);
                        }
                    }
                    Err(e) => log::debug!("connect to {addr} failed: {e}"),
                }
            }
        }

        self.len().await
    }

    /// The addresses THIS fill may dial: at most `MAX_DIALS_PER_FILL` of the candidate list,
    /// starting where the previous fill stopped, previously-ejected addresses last.
    ///
    /// Two orderings compose here and they answer different questions. Ejection decides
    /// PRIORITY: a peer that just failed a request must not reclaim its slot while an untried
    /// address is available. The cursor decides REACH: the candidate list outlives any one fill's
    /// budget, so each fill takes the next window and wraps, which is what makes every candidate
    /// eventually dialable rather than only the first twenty.
    async fn dial_window(&self) -> Vec<SocketAddr> {
        let candidates = self.candidate_addresses().await;
        if candidates.is_empty() {
            return Vec::new();
        }
        let ejected = self.ejected.read().await.clone();
        let (fresh, retry): (Vec<_>, Vec<_>) = candidates
            .iter()
            .copied()
            .partition(|a| !ejected.contains(a));
        let order: Vec<SocketAddr> = fresh.into_iter().chain(retry).collect();

        let width = order.len().min(MAX_DIALS_PER_FILL);
        let start = self.dial_cursor.fetch_add(width, Ordering::Relaxed) % order.len();
        order
            .iter()
            .cycle()
            .skip(start)
            .take(width)
            .copied()
            .collect()
    }

    /// Admit an already-open connection to `addr`, unless the pool now holds that address or is
    /// now at target.
    ///
    /// This is the only guard that can be trusted to decide the question, because it is the only
    /// one that asks it while HOLDING the members write lock. Everything upstream — the fill
    /// loop's occupancy read, the pre-dial skip — is an optimisation that spares a handshake;
    /// this one is what makes the answer true.
    async fn try_admit(
        &self,
        addr: SocketAddr,
        (peer, receiver): (P, mpsc::Receiver<Message>),
    ) -> bool {
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

    /// The dial order: operator-named addresses, then discovered ones — resolved once on
    /// SUCCESS and reused, re-attempted after a failure.
    ///
    /// The asymmetry is the point. A successful resolution is fixed for the pool's life so a
    /// fast resolver cannot keep reappearing at the head of the list. A FAILED one is not stored
    /// at all: discovery failing is a transient network condition, and remembering it would
    /// leave the pool with an empty candidate list forever — no TTL, no invalidation, no retry —
    /// having permanently traded a peer tier for one centralized HTTP endpoint.
    ///
    /// Operator-named addresses are still returned when discovery fails: they need no resolver.
    async fn candidate_addresses(&self) -> Vec<SocketAddr> {
        if let Some(addresses) = self.addresses.read().await.as_ref() {
            return addresses.clone();
        }

        let Ok(discovered) = self.discovery.discover().await.inspect_err(|e| {
            log::warn!("peer discovery failed, retrying on a later fill: {e}");
        }) else {
            return connect::candidate_list(&self.trusted, Vec::new());
        };

        let candidates = connect::candidate_list(&self.trusted, discovered);
        if candidates.len() <= self.trusted.len() {
            // Discovery answered, but nothing it named survived the routability filter — a
            // poisoned resolver, or an introducer with nothing but private addresses. That is a
            // failed resolution wearing a success, and caching it would fix the pool on the
            // operator's own addresses for the process's life with no retry.
            log::warn!("peer discovery contributed no routable address, retrying on a later fill");
            return candidates;
        }

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
        let discovery = Arc::new(IntroducerAddresses::new(network, connect_timeout));
        let pool = Self::with_dialer(dialer, discovery, max_peers, trusted, network);
        // Through `try_refill` rather than `fill` so a construction that falls short arms the
        // cooldown, and the first request does not immediately repeat the sweep that just failed.
        pool.try_refill().await;

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
