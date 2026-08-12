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
use tokio::sync::{mpsc, Mutex, RwLock};
// `tokio`'s clock, not `std`'s, and the difference is testability: the two throttles below are
// TIME policies, and `std::time::Instant` does not respond to `tokio::time::pause`. Outside a
// paused runtime it reads the same real clock `std` does, so nothing about production changes.
use tokio::time::Instant;

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

// # The pool's dial-cost policy, in one place
//
// Everything below is one policy expressed as four bounds. They were previously emergent from
// five separate knobs across three files, which is how three successive rounds of locally-correct
// fixes each severed a neighbour: no reader could see what the loop as a whole was allowed to
// cost. The bounds are named here, asserted by name in `pool::tests`, and restated in SPEC.
//
// - **DP-1, per fill.** One [`PeerPoolInner::fill`] considers at most `MAX_DIALS_PER_FILL`
//   candidates, dials each at most once, and holds at most `DIAL_BATCH` open at a time.
// - **DP-2, single-flight.** At most one fill is in flight per pool, in EVERY state including
//   empty. Enforced by [`PeerPoolInner::refilling`].
// - **DP-3, per address.** No candidate is dialled more than once per `REFILL_COOLDOWN` at steady
//   state. This holds by construction once fills are further apart than the time it takes
//   `dial_cursor` to wrap, and is a consequence of DP-1 and DP-4 rather than a separate check.
// - **DP-4, aggregate.** Sustained outbound dials stay at or under `MAX_DIALS_PER_FILL` per
//   minute per pool in every state. A CONTINUOUSLY EMPTY pool may burst to at most six fills —
//   120 dials — in its first minute, then decays to that steady rate. At target it dials nothing;
//   short, it is bounded by `REFILL_COOLDOWN`.
//
// DP-4 is the only one of the four that the intervals cannot deliver, and an earlier version of
// this note claimed the opposite. Both intervals are chosen from the pool's CURRENT state, and
// the empty backoff resets on admission — so a pool that admits a peer and loses it again
// re-enters the empty state owing nothing. Nothing bounds how often a pool may change state, so
// nothing bounded the dial rate: churning between one member and none measured **6000 dials a
// minute** at real mainnet full nodes, fifty times this ceiling
// (`churn_between_admission_and_ejection_cannot_outrun_the_dial_ceiling`).
//
// So DP-4 is enforced on the dials themselves, by [`DialBudget`], and the two intervals are
// demoted to what they alone can do. They are not a duplicate of it: the intervals decide WHEN a
// fill is offered and thereby SPREAD the burst — without them a pool would spend its whole
// minute's allowance in the first hundred milliseconds and have nothing left when a transient
// outage cleared. The budget decides HOW MANY dials any fill may spend and bounds the TOTAL. One
// shapes distribution, one caps volume, and only the second is immune to a state transition.

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
/// exists to avoid depending on, and waiting cannot improve it. An empty pool is instead gated by
/// [`EMPTY_RETRY_FLOOR`], which starts far shorter and grows toward this value.
const REFILL_COOLDOWN: Duration = Duration::from_secs(60);

/// The shortest interval between two fills of an EMPTY pool, and the value that interval starts
/// at and resets to.
///
/// One second, because the common empty pool is TRANSIENT — a boot before the network is up, a
/// DHCP or VPN race, the last member ejected by a failed request — and it recovers on the very
/// next dial. Blacking that out for `REFILL_COOLDOWN` trades the whole peer tier for one
/// centralized HTTP endpoint over a condition that would have cleared in a second.
///
/// The PERMANENTLY empty pool is the other half, and it is the expensive one: a host that can
/// reach no full node at all would, at a flat one-second floor, sustain 1200 outbound TLS
/// handshakes a minute at real mainnet nodes forever. So the interval doubles after every fill
/// that admits nothing — 1, 2, 4, 8, 16, 32, then capped at `REFILL_COOLDOWN` — and the FIRST
/// admission resets it. Six fills land in the first minute, twenty dials a minute thereafter.
const EMPTY_RETRY_FLOOR: Duration = Duration::from_secs(1);

/// What one outbound dial costs against the pool's aggregate allowance.
///
/// `REFILL_COOLDOWN / MAX_DIALS_PER_FILL` — three seconds — so the sustained rate DP-4 names is
/// exactly one full fill's worth of dials per cooldown, and a change to either constant carries
/// through instead of silently drifting from it
/// (`the_sustained_dial_rate_is_one_fill_per_cooldown` pins the relation).
const DIAL_COST: Duration =
    Duration::from_secs(REFILL_COOLDOWN.as_secs() / MAX_DIALS_PER_FILL as u64);

/// The most dials a pool may save up while it is quiet, and so the most any one minute may spend
/// above the sustained rate.
///
/// A hundred, which with the three-second cost puts DP-4's first-minute ceiling at 120: the
/// hundred banked plus the twenty accrued while they are spent. Banking matters because the
/// expensive states are transient — a boot before the network is up, a drain to empty — and a
/// pool that had been idle for an hour should be allowed to spend hard to recover, once.
const DIAL_BURST: u32 = 100;

/// The pool's aggregate dial allowance: DP-4, enforced by counting dials rather than timing fills.
///
/// Credit accrues with elapsed time and is spent per dial, so no sequence of admissions,
/// ejections or state changes can manufacture allowance — the quantity being counted is the only
/// one a transition leaves alone. It is deliberately NOT consulted as a second veto before
/// `fill`: it hands out a smaller allowance, and an exhausted allowance ends the fill. There is
/// one gate on whether to fill (`try_refill`) and one bound on what a fill may spend, not two
/// places that each independently say no.
struct DialBudget {
    /// Accrued and unspent, capped at `DIAL_COST * DIAL_BURST`.
    credit: Duration,
    /// When `credit` was last brought up to date.
    updated: Instant,
}

impl DialBudget {
    /// A budget that starts full, so a pool's first fill is never throttled by its own youth.
    fn full() -> Self {
        Self {
            credit: DIAL_COST * DIAL_BURST,
            updated: Instant::now(),
        }
    }

    /// Accrue, then hand out at most `wanted` dials' worth of allowance, spending what it hands.
    ///
    /// Returns how many dials the caller may make — possibly zero.
    fn take(&mut self, wanted: usize) -> usize {
        let now = Instant::now();
        self.credit =
            (self.credit + now.saturating_duration_since(self.updated)).min(DIAL_COST * DIAL_BURST);
        self.updated = now;

        let affordable = (self.credit.as_nanos() / DIAL_COST.as_nanos()) as usize;
        let allowed = wanted.min(affordable);
        // Saturating, though `allowed <= affordable` makes it exact: `Duration`'s `-` PANICS on
        // underflow, and this runs inside a fill on the request path and in a detached task. A
        // future edit that got the relation wrong should overspend the budget and be caught by
        // the ceiling test, not take the pool down with an arithmetic panic.
        self.credit = self.credit.saturating_sub(DIAL_COST * allowed as u32);
        allowed
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
    ///
    /// Two caveats, neither a defect, both easy to over-read. The reach property is SEQUENTIAL:
    /// concurrent fills take adjacent wrapping windows, which overlap whenever the candidate list
    /// is shorter than two windows, and `try_admit` rather than the cursor is what keeps the
    /// result distinct. And `dial_window` re-partitions the list by the ejected set on every
    /// fill, so the index carried here points into a DIFFERENT permutation each time; "every
    /// candidate is eventually dialled" weakens as that set churns, and is a strong tendency
    /// rather than a guarantee.
    dial_cursor: AtomicUsize,
    /// Held for the duration of a [`try_refill`](PeerPoolInner::try_refill) sweep.
    ///
    /// Single-flight, not mutual exclusion: a caller that cannot take it returns at once rather
    /// than queueing, so K concurrent reads over a short pool cost ONE sweep and not K.
    refilling: Mutex<()>,
    /// When a fill last ended under `target`, gating the next sweep by `REFILL_COOLDOWN`.
    last_short_fill: RwLock<Option<Instant>>,
    /// When a fill of an EMPTY pool last admitted nothing, and how long the next one must wait.
    ///
    /// `None` means no such fill has happened since the last admission, so the next fill is
    /// permitted at once. The interval doubles per empty fill and is capped at `REFILL_COOLDOWN`;
    /// see [`EMPTY_RETRY_FLOOR`] for why it is a backoff rather than a flat floor.
    ///
    /// One field in one place: the alternative — a floor here and a counter somewhere else — is
    /// the split-policy shape this pool has already been bitten by.
    empty_backoff: RwLock<Option<(Instant, Duration)>>,
    /// DP-4's aggregate ceiling. See [`DialBudget`] for why the two intervals above cannot be it.
    dial_budget: Mutex<DialBudget>,
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
            empty_backoff: RwLock::new(None),
            dial_budget: Mutex::new(DialBudget::full()),
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

    /// Start a refill WITHOUT waiting for it, for a caller that has somewhere else to get its
    /// answer.
    ///
    /// [`try_refill`](Self::try_refill) is single-flight, so calling this on every read of an
    /// empty pool costs one sweep and not one per read; the spawned task returns immediately when
    /// a sweep is already running or the state's interval has not elapsed.
    pub fn try_refill_detached(self: &Arc<Self>) {
        let pool = Arc::clone(self);
        tokio::spawn(async move { pool.try_refill().await });
    }

    /// A peer to serve a request with, refilling the pool around the answer.
    ///
    /// A fill is a bounded but real sweep of outbound TLS dials, so where it sits relative to
    /// selection decides what a read costs. With a usable member in hand the sweep buys
    /// DIVERSITY, which the caller waiting on a read does not need right now — so it runs BEHIND
    /// the answer, detached. With nothing to serve it buys AVAILABILITY, so it runs IN FRONT and
    /// the caller waits for it.
    ///
    /// That second case is the pool's answer for a caller that has NO other source. A caller that
    /// does have one must not reach this function while empty: waiting here buys a
    /// low-probability decentralized answer on precisely the read whose fill is least likely to
    /// succeed, at up to two connect-timeout sweeps, when the fallback would have answered in
    /// milliseconds — and the detached fill makes the NEXT read peer-served anyway. The pool
    /// cannot see whether its caller has a fallback, so that decision is `router`'s: it
    /// short-circuits to the fallback and calls
    /// [`try_refill_detached`](Self::try_refill_detached) instead.
    pub async fn select_refilling(self: &Arc<Self>) -> Option<(P, SocketAddr)> {
        if let Some(picked) = self.select_peer().await {
            self.try_refill_detached();
            return Some(picked);
        }

        self.try_refill().await;
        self.select_peer().await
    }

    /// Drop the member at `addr`. The freed slot refills on the next
    /// [`try_refill`](Self::try_refill).
    pub async fn eject_peer(&self, addr: SocketAddr) {
        let became_empty = {
            let mut members = self.members.write().await;
            members.retain(|m| m.addr != addr);
            members.is_empty()
        };
        self.ejected.write().await.insert(addr);
        if became_empty {
            // A clock started while the pool was merely SHORT says nothing about a pool that now
            // holds nothing, so it is discarded rather than carried into the empty state — where
            // `empty_backoff` is the interval that applies.
            *self.last_short_fill.write().await = None;
        }
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

    /// Dial toward `target` if the pool is under it — AT MOST ONE sweep at a time (DP-2), and no
    /// more often than the state's own interval allows.
    ///
    /// Two intervals, because the two states are different problems. A pool that is merely SHORT
    /// has something to serve with and is short of DIVERSITY, so it waits the full
    /// `REFILL_COOLDOWN`. A pool that is EMPTY has nothing to serve with and is short of
    /// AVAILABILITY, so it retries from `EMPTY_RETRY_FLOOR` — one second — and backs off from
    /// there only as repeated fills go on admitting nothing. Between them they hold DP-4: at most
    /// 120 dials in a continuously-empty pool's first minute, and 20 a minute after that.
    ///
    /// Both intervals gate THIS call site and nothing else. Whether there is ROOM is likewise not
    /// decided here: [`fill`](Self::fill) decides it, from the member set, under concurrency.
    /// Repeating either check elsewhere would put one policy in two places where either alone
    /// would satisfy any test of it.
    pub async fn try_refill(&self) {
        let Ok(_in_flight) = self.refilling.try_lock() else {
            return;
        };
        if self.is_empty().await {
            if let Some((last_empty, floor)) = *self.empty_backoff.read().await {
                if last_empty.elapsed() < floor {
                    return;
                }
            }
        } else if let Some(last_short) = *self.last_short_fill.read().await {
            if last_short.elapsed() < REFILL_COOLDOWN {
                return;
            }
        }

        let held = self.fill().await;
        if held < self.target {
            *self.last_short_fill.write().await = Some(Instant::now());
        }
        self.record_empty_outcome(held).await;
    }

    /// Advance or clear the empty-pool backoff, given how many members the fill left behind.
    ///
    /// Recorded AFTER the fill rather than before it, so the interval is measured from the end of
    /// one sweep to the start of the next and a slow sweep cannot overlap the following one.
    async fn record_empty_outcome(&self, held: usize) {
        let mut backoff = self.empty_backoff.write().await;
        if held > 0 {
            // Any admission at all means the network can supply a peer, so the evidence the
            // backoff was accumulating is stale. Reset rather than decay: a pool that flaps
            // between one member and none must not degrade to a minute's silence.
            *backoff = None;
            return;
        }
        let next = match *backoff {
            Some((_, current)) => (current * 2).min(REFILL_COOLDOWN),
            None => EMPTY_RETRY_FLOOR,
        };
        *backoff = Some((Instant::now(), next));
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

            // DP-4, charged on the dials themselves. An exhausted allowance ENDS the fill rather
            // than shrinking the next batch: a pool out of credit should stop, not trickle a dial
            // per batch through a loop that keeps re-reading occupancy.
            let allowed = self.dial_budget.lock().await.take(batch.len());
            let batch = &batch[..allowed];
            if batch.is_empty() {
                break;
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
                        self.try_admit(addr, connection).await;
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
            // Cleared while the members write lock is still held, so "admitted" and "no longer
            // deprioritised" become true together. Done after the guard dropped, a concurrent
            // `dial_window` could read the member as admitted and the ejection as still standing,
            // and deprioritise an address the pool is currently holding for one whole fill.
            self.ejected.write().await.remove(&addr);
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
        // Against the trusted list AS THE COMPOSER SEES IT, not `self.trusted.len()`:
        // `candidate_list` de-duplicates, so an operator who named the same node twice would make
        // a genuinely useful discovery answer look like it contributed nothing.
        let trusted_only = connect::candidate_list(&self.trusted, Vec::new()).len();
        if candidates.len() <= trusted_only {
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
        // matching interval — the short cooldown, or the empty-pool backoff at its one-second
        // floor — instead of leaving the first request free to repeat the sweep that just failed.
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
