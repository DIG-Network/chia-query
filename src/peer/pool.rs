use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use chia_protocol::{CoinStateUpdate, Message, NewPeakWallet, ProtocolMessageTypes};
use chia_traits::Streamable;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, RwLock};

use chia_wallet_sdk::client::Peer;
use tokio_tungstenite::Connector;

use crate::types::ChiaQueryError;
use crate::NetworkType;

use super::connect;
use super::frames::{
    FrameFanout, FrameSource, FrameSubscription, PoolFrame, SessionEndReason, SessionId,
};
use super::plurality::{CORROBORATION_FLOOR, PEAK_LAG_EVICTION, PEER_LIFETIME, PRIORITY_SLOTS};

/// How many dial rounds [`PeerPool::fill_toward_capacity`] may spend reaching capacity.
///
/// DERIVED, not chosen, for the same reason [`default_max_peers`](super::plurality::default_max_peers)
/// is: the priority addresses are tried SEQUENTIALLY and a round admits at most one of them, so
/// [`PRIORITY_SLOTS`] rounds can be consumed before a single dial reaches discovery at all. Two
/// more are then owed - one that reaches discovery with every priority address excluded, and one
/// for the ordinary attrition of a round whose dials did not all land.
///
/// A literal `3` was wrong on exactly the host this rule exists for: an operator with
/// `TRUSTED_FULLNODE` who also runs a node spends round one on the trusted address and round two
/// on the loopback, leaving ONE round for discovery and no slack. If that round admitted fewer
/// than [`CORROBORATION_FLOOR`] independent peers the pool could never arm, and there was no
/// fourth round in which to try. Deriving it means a third priority address widens the budget
/// instead of silently consuming it.
const FILL_ROUNDS: usize = PRIORITY_SLOTS + 2;

/// How many dials a round opens per slot it is trying to fill.
///
/// The pool dials WIDER than it can hold and keeps the most credible answers, because a dial is
/// the only point at which peers can be compared at all: once a connection is admitted the slot is
/// spent, and the alternative to comparing candidates is admitting whichever ones happened to
/// answer. Discovery returns an introducer's address set in shuffled order, so "happened to answer"
/// is the pool's entire selection policy without this.
///
/// **Two, and deliberately no more.** The retained peers are chosen by handshake latency, and
/// latency is partly a measure of network PROXIMITY — so a strong selection pressure would
/// concentrate the pool on peers near this host, which is the population a local or regional
/// adversary is likeliest to hold. Choosing 8 of 16 is a mild preference for peers that answer;
/// choosing 8 of 100 would be a proximity filter wearing a credibility label, and NC-12 rests on
/// the held peers being independent of each other. The factor is a bound on that pressure, which
/// is why it is small rather than as large as the discovery set allows.
///
/// It is NOT part of the capacity derivation. Capacity remains
/// [`default_max_peers`](super::plurality::default_max_peers), derived from
/// [`QUORUM_SAMPLE`](super::plurality::QUORUM_SAMPLE); this widens the CANDIDATE set only, so the
/// number of independent voices the pool ends up holding is unchanged and only their identity
/// differs.
const DIAL_OVERSUBSCRIPTION: usize = 2;

/// How many dials to open when `wanted` slots remain.
///
/// A free function rather than an inline multiplication so the property it carries — that the
/// candidate set is strictly wider than the slots, whenever there are slots at all — is stated
/// where it can be tested.
fn dials_for(wanted: usize) -> usize {
    wanted.saturating_mul(DIAL_OVERSUBSCRIPTION)
}

/// A dial that succeeded, waiting to be judged against its round's other successes.
///
/// Generic over the connection it carries so the ranking below can be exercised on a fixture of
/// plain numbers. The alternative — proving the ordering only through `fill_toward_capacity` —
/// cannot be done without a live network, which is how a selection policy ends up asserted rather
/// than measured.
struct DialCandidate<T> {
    origin: connect::PeerOrigin,
    /// Time from opening the dial to a usable connection: the one behavioural signal a peer has
    /// actually produced by the moment the pool must decide whether to keep it.
    handshake: Duration,
    connection: T,
}

/// Keep the `slots` most credible candidates of a round, discarding the rest.
///
/// Ordering, in this order:
///
/// 1. **[`Priority`](connect::PeerOrigin::Priority) first, unconditionally.** A priority entry is
///    the operator's own or co-resident node; it is admitted because the operator said so, and it
///    must not be crowded out by a stranger that answered a millisecond sooner. It is not a voice
///    (#42), so keeping it costs the independent set nothing.
/// 2. **Then by ascending handshake time.** A peer that completed a handshake quickly has DONE
///    something; a peer that took most of the connect timeout to answer is the one most likely to
///    be slow again, and the pool has no other evidence about either at this moment.
///
/// The sort is STABLE, so candidates that tie keep the order the round produced them in — which is
/// discovery's shuffle, not an address ordering. A tie broken by address would let an adversary
/// pick addresses that sort early.
///
/// Discarding a candidate DROPS its connection, which closes it. That is the cost of dialling
/// wide and it is paid on purpose: a handshake spent learning that a peer is slow is cheaper than
/// a pool slot held for [`PEER_LIFETIME`] by one.
fn most_credible<T>(mut candidates: Vec<DialCandidate<T>>, slots: usize) -> Vec<DialCandidate<T>> {
    candidates.sort_by_key(|c| (c.origin != connect::PeerOrigin::Priority, c.handshake));
    candidates.truncate(slots);
    candidates
}

// ---------------------------------------------------------------------------
// Pool entry
// ---------------------------------------------------------------------------

struct PeerEntry {
    peer: Peer,
    address: SocketAddr,
    /// The session this connection publishes its frames under.
    ///
    /// Held so an ejection can name a CONNECTION rather than an address: a peer whose session
    /// ended is removed only if the entry at that address is still the one that ended, never
    /// its freshly dialled replacement.
    session: SessionId,
    /// How this peer was reached. Held so a caller counting independent opinions can tell a
    /// preferred local node from a discovered one - see [`connect::PeerOrigin`].
    origin: connect::PeerOrigin,
    /// The highest peak THIS peer has announced, or 0 before it has announced any.
    ///
    /// The pool's shared `peak_height` is a `fetch_max` across every session, so it answers "how
    /// high is the chain" and can never say which peer is behind. Lag is a per-peer fact and needs
    /// a per-peer number.
    ///
    /// An `Arc<AtomicU32>` because the only writer is the entry's receiver-handler task, which
    /// runs detached and cannot take the pool's write lock. It is cloned into that task at
    /// admission and dies with the entry, so nothing accumulates.
    last_peak: Arc<AtomicU32>,
    /// When this connection entered the pool, so it can be rotated out on a TIMER.
    ///
    /// The pool's other eviction is failure-driven, and failure is not the risk this guards: a set
    /// of peers that all keep answering is exactly the set an attacker only has to capture once
    /// (NC-12). Age is the only signal that separates the two.
    admitted_at: Instant,
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

/// Whether the pool holds enough independent voices for a corroborated read.
///
/// A two-variant answer rather than a bare count, so a caller cannot accidentally proceed with
/// "some" corroboration: the insufficient case names what it has AND what it needed, which is the
/// information a log line or a user-facing message actually requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorroborationReadiness {
    /// Enough independent peers, besides the one answering, to corroborate.
    Armed { corroborators: usize },
    /// Too few. The read must be REFUSED, never attempted with fewer voices.
    Insufficient {
        corroborators: usize,
        required: usize,
    },
}

pub struct PeerPool {
    entries: RwLock<Vec<PeerEntry>>,
    next_idx: AtomicUsize,
    max_peers: usize,
    tls: Connector,
    network: NetworkType,
    connect_timeout: Duration,
    /// Latest peak height observed from any connected peer's NewPeakWallet
    /// messages.  Updated in the background by receiver handler tasks.
    peak_height: Arc<AtomicU32>,
    /// Fans every inbound frame out to the pool's subscribers.
    ///
    /// The atomic above answers "how high is the chain"; this carries the frames THEMSELVES, which
    /// is what a consumer following coin states needs and what its absence forced into a second
    /// dialled session (dig_ecosystem#2761).
    fanout: Arc<FrameFanout>,
    /// Sessions that have STOPPED, waiting to be ejected on the next maintenance pass.
    ///
    /// A receiver handler runs in its own task and cannot take the pool's write lock without
    /// holding a reference to the pool, so it records the death here and lets
    /// [`maintain`](Self::maintain) act on it. Without this the pool's only removals are
    /// failure-driven - a dead connection stays in `entries`, counted as a held peer and
    /// offered to `select_peer`, until a request happens to pick it or `PEER_LIFETIME`
    /// elapses.
    dead_sessions: Arc<StdMutex<Vec<FrameSource>>>,
}

impl PeerPool {
    /// Spin up the pool by connecting to `max_peers` random full-node peers
    /// concurrently.  Under [`PeerRequirement::Required`] at least one peer must
    /// succeed, otherwise we return [`ChiaQueryError::PeerDiscoveryFailed`]; under
    /// [`PeerRequirement::Optional`] an empty pool is returned and refills later.
    pub async fn new(
        network: NetworkType,
        tls: Connector,
        max_peers: usize,
        requirement: PeerRequirement,
        connect_timeout: Duration,
    ) -> Result<Self, ChiaQueryError> {
        let pool = Self {
            entries: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls,
            network,
            connect_timeout,
            peak_height: Arc::new(AtomicU32::new(0)),
            fanout: Arc::new(FrameFanout::new()),
            dead_sessions: Arc::new(StdMutex::new(Vec::new())),
        };

        pool.fill_toward_capacity().await;

        if !pool.has_peers().await {
            if requirement == PeerRequirement::Required {
                return Err(ChiaQueryError::PeerDiscoveryFailed);
            }
            log::warn!("no peers connected; serving from the coinset fallback until one does");
        }

        Ok(pool)
    }

    /// Dial toward capacity in ROUNDS, each excluding what the earlier rounds admitted.
    ///
    /// One round of `max_peers` concurrent dials is not enough, and the reason is the priority
    /// path: every dial offers `TRUSTED_FULLNODE` and the loopback ahead of discovery, and
    /// concurrent dials know nothing of each other, so on a machine running a full node ALL of
    /// them return the same local address and every one but the first is discarded as a duplicate.
    /// A single round therefore leaves a pool of ONE peer on exactly the machines most likely to
    /// have several available - and one peer is a pool that can never corroborate anything.
    ///
    /// Each round dials [`DIAL_OVERSUBSCRIPTION`] times the slots it is trying to fill and keeps
    /// the most credible answers ([`most_credible`]). Capacity is untouched by this — the pool
    /// still holds [`default_max_peers`](super::plurality::default_max_peers) — so it changes WHICH
    /// peers occupy the slots, never how many independent voices stand in them.
    ///
    /// A later round excludes what the earlier ones admitted, so the priority addresses are no
    /// longer offered and the dial falls through to discovery. Rounds stop as soon as one admits
    /// nothing: a round that admitted nothing is evidence that dialling again would not help
    /// either, and that bound is what keeps a host with no reachable peers from spending
    /// `FILL_ROUNDS` whole timeouts on the same answer.
    async fn fill_toward_capacity(&self) {
        for _ in 0..FILL_ROUNDS {
            let held: Vec<SocketAddr> = {
                let entries = self.entries.read().await;
                entries.iter().map(|e| e.address).collect()
            };
            let wanted = self.max_peers.saturating_sub(held.len());
            if wanted == 0 {
                return;
            }

            let mut dials = FuturesUnordered::new();
            for _ in 0..dials_for(wanted) {
                let tls = self.tls.clone();
                let held = held.clone();
                let network = self.network;
                let timeout = self.connect_timeout;
                dials.push(async move {
                    let started = Instant::now();
                    connect::connect_random_peer_excluding(network, &tls, timeout, &held)
                        .await
                        .map(|connection| (connection, started.elapsed()))
                });
            }

            // The whole round is collected before anything is admitted. Admitting eagerly, as
            // this did, spends the slots on whichever dials returned first and leaves nothing to
            // compare - the round has to be complete before "most credible" means anything. It is
            // bounded by `connect_timeout`, since that is what every dial resolves within.
            let mut candidates = Vec::new();
            while let Some(result) = dials.next().await {
                match result {
                    Ok((connection, handshake)) => candidates.push(DialCandidate {
                        origin: connection.3,
                        handshake,
                        connection,
                    }),
                    Err(e) => log::debug!("peer connect failed: {e}"),
                }
            }

            let offered = candidates.len();
            let mut admitted = 0usize;
            for candidate in most_credible(candidates, wanted) {
                let (peer, addr, receiver, origin) = candidate.connection;
                if self.admit_and_follow(peer, addr, receiver, origin).await {
                    admitted += 1;
                }
            }
            if offered > admitted {
                log::debug!(
                    "dial round kept {admitted} of {offered} candidates for {wanted} slots"
                );
            }

            if admitted == 0 {
                return;
            }
        }
    }

    /// Admit a connection and, if it was admitted, start following its frames.
    ///
    /// The ONE path by which a session becomes visible to subscribers, so the ordering it holds
    /// holds everywhere: the session is identified, then admitted, then ANNOUNCED, and only then
    /// does its task begin publishing. Announcing before admission would name a session for a
    /// duplicate that was discarded; announcing after the task started would race its first frame.
    async fn admit_and_follow(
        &self,
        peer: Peer,
        address: SocketAddr,
        receiver: mpsc::Receiver<Message>,
        origin: connect::PeerOrigin,
    ) -> bool {
        let source = self.fanout.allocate_session(address);
        let last_peak = Arc::new(AtomicU32::new(0));
        if !self
            .admit(
                peer,
                address,
                origin,
                source.session,
                Arc::clone(&last_peak),
            )
            .await
        {
            return false;
        }
        self.fanout.open_session(source).await;
        self.spawn_receiver_handler(source, receiver, last_peak);
        true
    }

    /// Latest peak height observed across all connected peers.
    /// Returns 0 if no peak has been received yet.
    pub fn peak_height(&self) -> u32 {
        self.peak_height.load(Ordering::Relaxed)
    }

    /// Round-robin select a peer from the pool.
    /// Returns `None` when the pool is empty.
    pub async fn select_peer(&self) -> Option<(Peer, SocketAddr)> {
        let entries = self.entries.read().await;
        if entries.is_empty() {
            return None;
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % entries.len();
        let entry = &entries[idx];
        Some((entry.peer.clone(), entry.address))
    }

    /// How the pool reached the peer currently held at `address`, or `None` if it holds none.
    ///
    /// Exists so a consumer that PINS one session can describe it honestly. A
    /// [`Priority`](connect::PeerOrigin::Priority) peer is an operator's own or co-resident node
    /// and a [`Discovered`](connect::PeerOrigin::Discovered) one is an anonymous introducer result;
    /// a component that reports a trust level for its answers must be able to tell which it is
    /// talking to, rather than repeating what its own configuration claimed.
    ///
    /// This does NOT confer independence. Counting independent voices stays with
    /// [`independent_peer_count`](Self::independent_peer_count) and
    /// [`select_corroborating_peers`](Self::select_corroborating_peers), both of which already
    /// exclude `Priority` peers, and reading a single origin is not a substitute for either.
    pub async fn origin_of(&self, address: SocketAddr) -> Option<connect::PeerOrigin> {
        self.entries
            .read()
            .await
            .iter()
            .find(|e| e.address == address)
            .map(|e| e.origin)
    }

    /// Every peer that could CORROBORATE an answer already given by the peer at `asked`.
    ///
    /// A corroborating peer must be two things at once, and neither alone is enough:
    ///
    /// - **A different address than `asked`.** Asking the same connection twice returns the same
    ///   opinion twice, which reads as agreement while being one voice.
    /// - **[`PeerOrigin::Discovered`](connect::PeerOrigin).** A peer reached from a preferred
    ///   address - an operator's node, or one on this machine - is an excellent peer to READ from
    ///   and is not evidence about the chain independent of this host, exactly as
    ///   [`independent_peer_count`](Self::independent_peer_count) records.
    ///
    /// They are returned ALL AT ONCE, and there is deliberately no singular form of this. Asking
    /// corroborators one at a time lets the first responder settle a claim about the chain, which
    /// is exactly the power a hostile peer has (dig_ecosystem#2462) - and a single corroborator
    /// cannot reach `CORROBORATION_FLOOR` at all, so a caller that took one would be building an
    /// answer it is not allowed to report as corroborated.
    ///
    /// Returns an empty vector when the pool holds nobody who qualifies, which is the honest
    /// answer that there is nobody to corroborate with.
    pub async fn select_corroborating_peers(&self, asked: SocketAddr) -> Vec<(Peer, SocketAddr)> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| Self::is_corroborator(e, asked))
            .map(|e| (e.peer.clone(), e.address))
            .collect()
    }

    /// Remove a peer from the pool and asynchronously connect a replacement.
    pub async fn eject_peer(&self, addr: SocketAddr) {
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.address != addr);
        }
        log::debug!(
            "peer ejected from pool; will refill on next request (network={:?})",
            self.network,
        );
    }

    /// Whether the pool has at least one usable peer.
    pub async fn has_peers(&self) -> bool {
        !self.entries.read().await.is_empty()
    }

    /// How many peers the pool HOLDS right now.
    ///
    /// This is a live count of the connections currently in the pool, not
    /// [`max_peers`](Self::new)'s target: a pool that is still filling reports what it has, and
    /// reports the target only once it has reached it. A caller showing this number to a user is
    /// stating a fact about the machine, so a configured intention must never stand in for it.
    ///
    /// A peer is removed by [`eject_peer`](Self::eject_peer), which runs when a request to it
    /// FAILS. So the count is of peers held and believed usable; a connection that has died
    /// silently is still counted until something tries to use it. That is the same liveness
    /// standard [`has_peers`](Self::has_peers) has always answered by, made countable.
    pub async fn peer_count(&self) -> usize {
        self.entries.read().await.len()
    }

    /// How many peers the pool holds that are INDEPENDENT opinions.
    ///
    /// [`peer_count`](Self::peer_count) answers "how many connections do I have"; this answers
    /// "how many of them could corroborate each other". They differ by the peers reached from a
    /// preferred address - an operator's trusted node or one on this machine - which are excellent
    /// peers to READ from and are not evidence about the chain independent of this host. A caller
    /// deciding whether enough separate sources agree MUST use this number, because counting a
    /// co-resident node as an independent voice is the thing that made a single local process able
    /// to look like a full peer set (dig_ecosystem#2648).
    pub async fn independent_peer_count(&self) -> usize {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| e.origin == connect::PeerOrigin::Discovered)
            .count()
    }

    /// Whether a peer entry qualifies as a corroborator: not the answering peer, and discovered.
    fn is_corroborator(entry: &PeerEntry, asked: SocketAddr) -> bool {
        entry.address != asked && entry.origin == connect::PeerOrigin::Discovered
    }

    /// Whether the pool can honestly attempt a CORROBORATED read of an answer given by `asked`.
    ///
    /// The count is of the peers that WILL be asked to corroborate - precisely the set
    /// [`select_corroborating_peers`](Self::select_corroborating_peers) returns for the same
    /// address, because both use [`is_corroborator`](Self::is_corroborator) to decide the set.
    ///
    /// **It takes the answering address rather than subtracting one blindly.** An earlier version
    /// charged the asker's slot against the independent set whatever the asker was, so a read from
    /// the operator's own node - which is not in that set at all - silently spent an independent
    /// voice it had never occupied. On the host this crate is sized for, two genuinely independent
    /// peers agreeing with a co-resident node were downgraded to `Uncorroborated*` and pushed on
    /// to the centralized coinset tier, which is the opposite of what NC-12 asks for.
    ///
    /// **This refuses; it never degrades.** Corroborating against however many peers happen to be
    /// present turns a four-voice quorum into a two-voice one that still reports itself
    /// corroborated, and no consumer downstream can tell those apart. A caller handed
    /// [`CorroborationReadiness::Insufficient`] must decline the read, not proceed with fewer
    /// voices.
    pub async fn corroboration_readiness(&self, asked: SocketAddr) -> CorroborationReadiness {
        let corroborators = self
            .entries
            .read()
            .await
            .iter()
            .filter(|e| Self::is_corroborator(e, asked))
            .count();
        if corroborators >= CORROBORATION_FLOOR {
            CorroborationReadiness::Armed { corroborators }
        } else {
            CorroborationReadiness::Insufficient {
                corroborators,
                required: CORROBORATION_FLOOR,
            }
        }
    }

    /// Rotate out the OLDEST discovered peer that has outlived [`PEER_LIFETIME`], if any.
    ///
    /// Returns the address ejected, so a caller can log or refill deliberately. This is NC-12's
    /// cycling half and it is driven by AGE alone: a peer that has answered every request is
    /// exactly the peer this removes, because a set that never fails is a set an attacker only has
    /// to capture once. The pool's other eviction, [`eject_peer`](Self::eject_peer), fires on
    /// request FAILURE and cannot substitute for this - a captured peer does not fail.
    ///
    /// Only [`PeerOrigin::Discovered`](connect::PeerOrigin) entries are rotated. A priority entry
    /// is the operator's own node or one on this machine; cycling it would re-dial the same
    /// address, spending a handshake to change nothing.
    ///
    /// One per call, so cycling can never empty the pool in a single sweep.
    pub async fn cycle_expired_peers(&self) -> Option<SocketAddr> {
        let mut entries = self.entries.write().await;
        let now = Instant::now();

        let oldest = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.origin == connect::PeerOrigin::Discovered
                    && now.duration_since(e.admitted_at) >= PEER_LIFETIME
            })
            .min_by_key(|(_, e)| e.admitted_at)
            .map(|(idx, e)| (idx, e.address));

        let (idx, address) = oldest?;
        entries.remove(idx);
        log::debug!("peer {address} rotated out after {PEER_LIFETIME:?} (NC-12 cycling)");
        Some(address)
    }

    /// Evict the discovered peers that have fallen too far behind the chain the pool sees.
    ///
    /// Returns the addresses removed. This is the pool's FOURTH eviction reason, and a
    /// connected-but-lagging peer escapes all three of the others: it fails no request, so
    /// [`eject_peer`](Self::eject_peer) never fires; its session is alive, so
    /// [`eject_dead_sessions`](Self::eject_dead_sessions) never sees it; and it may have minutes
    /// of [`PEER_LIFETIME`] left, so [`cycle_expired_peers`](Self::cycle_expired_peers) is not due.
    /// Until now it therefore counted as an armed corroborating voice in
    /// [`corroboration_readiness`](Self::corroboration_readiness) while answering about an older
    /// chain — a denominator that overstated how many CURRENT voices the pool held (#40).
    ///
    /// # The reference peak is a MEDIAN, never a maximum, and that is the security of it
    ///
    /// Lag is measured against [`reference_peak`], the median of what the held peers have
    /// announced. A maximum would hand the bar to whichever peer claims the highest number, so one
    /// hostile peer announcing an inflated peak would evict every honest peer in the pool and
    /// leave itself — turning an eviction written for NC-12 into the cleanest possible attack on
    /// it. A median moves UPWARD — the direction that evicts honest peers — only if MORE THAN HALF
    /// the held peers agree to move it: 5 of 8, 3 of 4, 2 of 3, measured. A pool whose majority is
    /// hostile has already lost, so no eviction policy can rescue it.
    ///
    /// Moving it DOWNWARD is cheaper — at an even size exactly half suffices, 4 of 8 — and that
    /// asymmetry is deliberate rather than a gap. Lag is `reference - peak > PEAK_LAG_EVICTION`, so
    /// a lower reference can only evict FEWER peers; the worst a colluding half achieves is the
    /// no-eviction status quo, which it could reach more cheaply by announcing the true peak. The
    /// bar is guarded in the only direction where crossing it costs anything.
    ///
    /// A peer that has announced NOTHING yet is never evicted here. Silence is not lag: a
    /// just-admitted peer has had no chance to speak, and its `last_peak` of 0 would otherwise read
    /// as the furthest-behind peer in the pool. Session death and age own that peer's fate.
    ///
    /// # Only DISCOVERED peers — both as CANDIDATES and as VOTERS
    ///
    /// `Priority` entries are exempt from being evicted, and they are also excluded from the
    /// `announced` set the reference is taken over. **These are two separate properties and the
    /// second is the one that has security content** (#42): exempting a peer decides only its own
    /// fate, while letting it vote hands it a say over everyone else's.
    ///
    /// A priority entry is the operator's own or co-resident node — precisely the source a local
    /// attacker can supply (dig_ecosystem#2648) — so `independent_peer_count` already refuses to
    /// count it as a voice. Counting its announced peak in the median granted it back, through the
    /// side door, the authority the origin was introduced to deny: with `PRIORITY_SLOTS` such
    /// entries the median rests on them whenever the pool holds one discovered peer, and two
    /// fabricated heights evict it, taking the independent count to zero.
    ///
    /// The consequence is stated on the empty case below: once only discovered peaks vote, a pool
    /// holding no independent voice has no reference and evicts NOTHING.
    ///
    /// # No floor exemption
    ///
    /// Discovered entries are exactly the set whose count this defect corrupts, and re-dialling a
    /// priority address would only reach the same address.
    ///
    /// Eviction is NOT capped to keep the pool at [`CORROBORATION_FLOOR`]. Keeping a lagging peer
    /// so the arithmetic still reaches the floor would preserve precisely the inflated denominator
    /// this exists to remove, and would do it in the marginal case where it matters most.
    /// `corroboration_readiness` REFUSES rather than degrading, so falling under the floor makes
    /// the read decline and fall back — the fail-safe direction — and
    /// [`maintain`](Self::maintain) refills immediately afterwards.
    pub async fn evict_lagging_peers(&self) -> Vec<SocketAddr> {
        let mut entries = self.entries.write().await;

        let announced: Vec<u32> = entries
            .iter()
            .filter(|e| e.origin == connect::PeerOrigin::Discovered)
            .map(|e| e.last_peak.load(Ordering::Relaxed))
            .filter(|peak| *peak > 0)
            .collect();

        // No independent voice has spoken, so there is no bar to judge anyone against, and the
        // pool evicts NOTHING. Stated rather than left to the arithmetic: this is now reachable
        // in ordinary operation - a host that connected its priority addresses before discovery
        // holds entries that all announce and none of which may vote - and the alternative
        // readings are both worse. Evicting on a bar set by non-voices is the defect above;
        // evicting everyone against a reference of zero would destroy the pool on the first
        // maintenance pass after start-up.
        let Some(reference) = reference_peak(&announced) else {
            return Vec::new();
        };

        let mut evicted = Vec::new();
        entries.retain(|entry| {
            let peak = entry.last_peak.load(Ordering::Relaxed);
            let lagging = entry.origin == connect::PeerOrigin::Discovered
                && peak > 0
                && reference.saturating_sub(peak) > PEAK_LAG_EVICTION;
            if lagging {
                log::debug!(
                    "peer {} evicted: peak {peak} trails the pool's reference {reference} by more \
                     than {PEAK_LAG_EVICTION} blocks",
                    entry.address
                );
                evicted.push(entry.address);
            }
            !lagging
        });
        evicted
    }

    /// One maintenance pass: drop what the pool no longer truly holds, then refill toward capacity.
    ///
    /// Cycling before refilling is deliberate. Refilling first would find the pool at capacity and
    /// do nothing, so the rotation would leave a permanently smaller pool. Lag eviction runs in
    /// the same window and for the same reason.
    pub async fn maintain(&self) {
        self.eject_dead_sessions().await;
        self.evict_lagging_peers().await;
        self.cycle_expired_peers().await;
        self.try_refill().await;
    }

    /// Remove the peers whose sessions have ENDED.
    ///
    /// Session death is the pool's third eviction reason and it is neither of the other two: a
    /// disconnected or protocol-violating peer has not failed a request, so
    /// [`eject_peer`](Self::eject_peer) never fires for it, and it need not be old, so cycling may
    /// be minutes away. Until it is removed the pool counts it as held and `select_peer` keeps
    /// offering it - a peer count that overstates what the pool can actually reach.
    ///
    /// Matched on address AND session, so a replacement already dialled to the same address is
    /// never removed by its predecessor's death.
    async fn eject_dead_sessions(&self) {
        let dead = std::mem::take(&mut *self.dead_sessions_guard());
        if dead.is_empty() {
            return;
        }

        let mut entries = self.entries.write().await;
        entries.retain(|entry| {
            let died = dead
                .iter()
                .any(|d| d.address == entry.address && d.session == entry.session);
            if died {
                log::debug!("peer {} ejected: its session ended", entry.address);
            }
            !died
        });
    }

    /// The dead-session list, recovering from a poisoned lock rather than panicking.
    ///
    /// A panic in one handler task must not take the pool's maintenance down with it, and the list
    /// is a plain `Vec` with no invariant a poisoned lock could have left half-applied.
    fn dead_sessions_guard(&self) -> std::sync::MutexGuard<'_, Vec<FrameSource>> {
        self.dead_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Admit a connection, or reject it, deciding under the WRITE lock.
    ///
    /// Returns whether it was admitted. Rejected because the pool is full, or because its address
    /// is already held - a pool of N connections to one address reports itself healthy while being
    /// a single point of both failure and deceit (dig_ecosystem#2648).
    ///
    /// **Both checks are made while HOLDING the write lock, and that placement is the whole
    /// correctness of this.** Dials run concurrently, so any check made before acquiring the lock -
    /// under the read lock, or by the caller - is a time-of-check/time-of-use gap: two fills of the
    /// same address each observe it absent, then each pushes, and the duplicate is admitted by
    /// exactly the code written to prevent it. The check and the push must be one critical section.
    async fn admit(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
        session: SessionId,
        last_peak: Arc<AtomicU32>,
    ) -> bool {
        let mut entries = self.entries.write().await;

        if entries.len() >= self.max_peers {
            log::debug!("peer {address} not admitted: pool is at capacity");
            return false;
        }
        if entries.iter().any(|e| e.address == address) {
            log::debug!("peer {address} not admitted: already held");
            return false;
        }

        entries.push(PeerEntry {
            peer,
            address,
            origin,
            session,
            last_peak,
            admitted_at: Instant::now(),
        });
        log::debug!("peer admitted: {address} ({origin:?})");
        true
    }

    /// If the pool is under capacity, try to connect one new peer.
    /// Also spawns a background task to handle its inbound `NewPeakWallet`
    /// messages.
    pub async fn try_refill(&self) {
        let held: Vec<SocketAddr> = {
            let entries = self.entries.read().await;
            if entries.len() >= self.max_peers {
                return;
            }
            entries.iter().map(|e| e.address).collect()
        };

        // `held` is a hint to the dial, not the guard: it saves dialling an address already in the
        // pool (the local one is offered on every call), and it may be stale the moment it is read.
        // `admit` re-decides under the write lock, which is where the invariant actually holds.
        match connect::connect_random_peer_excluding(
            self.network,
            &self.tls,
            self.connect_timeout,
            &held,
        )
        .await
        {
            Ok((peer, addr, receiver, origin)) => {
                if self.admit_and_follow(peer, addr, receiver, origin).await {
                    log::debug!("replacement peer connected: {addr}");
                }
            }
            Err(e) => log::warn!("replacement peer connect failed: {e}"),
        }
    }

    // -----------------------------------------------------------------------
    // Receiver helpers (handle NewPeakWallet from peers)
    // -----------------------------------------------------------------------

    /// Spawn a background task that reads inbound messages from one peer session, updating the
    /// shared peak height and fanning every recognised frame out to the pool's subscribers.
    ///
    /// `source` identifies the session. Every frame this task emits carries it, so a subscriber
    /// can tell one held peer's frames from another's - which is what lets it follow the peer it
    /// chose and eject one whose frames it rejected.
    ///
    /// **`pub(crate)`, so the one-path claim on [`admit_and_follow`](Self::admit_and_follow) holds
    /// across the crate boundary too.** A caller outside the crate could otherwise supply its own
    /// [`FrameSource`] and publish frames under a session the pool never allocated - attribution
    /// that is unforgeable in-crate becomes forgeable the moment the constructor is exported.
    ///
    /// **The task never ends quietly.** Both ways a session can stop - the transport closing, and
    /// a message this crate cannot decode - publish a [`PoolFrame::SessionEnded`] and record the
    /// death for [`eject_dead_sessions`](Self::eject_dead_sessions). Returning silently would
    /// leave a subscriber unable to distinguish a peer that stopped from a chain that is quiet,
    /// and would leave the pool holding a connection nothing will ever read from.
    pub(crate) fn spawn_receiver_handler(
        &self,
        source: FrameSource,
        mut receiver: mpsc::Receiver<Message>,
        last_peak: Arc<AtomicU32>,
    ) {
        let peak = Arc::clone(&self.peak_height);
        let fanout = Arc::clone(&self.fanout);
        let dead_sessions = Arc::clone(&self.dead_sessions);

        tokio::spawn(async move {
            let reason = loop {
                let Some(msg) = receiver.recv().await else {
                    break SessionEndReason::Disconnected;
                };

                match msg.msg_type {
                    ProtocolMessageTypes::NewPeakWallet => {
                        let Ok(new_peak) = NewPeakWallet::from_bytes(&msg.data) else {
                            log::warn!(
                                "peer {} sent an undecodable NewPeakWallet; ending the session",
                                source.address
                            );
                            break SessionEndReason::UndecodableFrame;
                        };
                        last_peak.fetch_max(new_peak.height, Ordering::Relaxed);
                        let prev = peak.fetch_max(new_peak.height, Ordering::Relaxed);
                        if new_peak.height > prev {
                            log::debug!(
                                "new peak from peer {}: {}",
                                source.address,
                                new_peak.height
                            );
                        }
                        fanout
                            .publish(
                                source,
                                PoolFrame::Peak {
                                    height: new_peak.height,
                                    header_hash: new_peak.header_hash,
                                },
                            )
                            .await;
                    }
                    ProtocolMessageTypes::CoinStateUpdate => {
                        let Ok(update) = CoinStateUpdate::from_bytes(&msg.data) else {
                            log::warn!(
                                "peer {} sent an undecodable CoinStateUpdate; ending the session",
                                source.address
                            );
                            break SessionEndReason::UndecodableFrame;
                        };
                        fanout.publish(source, coin_states_frame(update)).await;
                    }
                    _ => {}
                }
            };

            dead_sessions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(source);
            fanout
                .publish(source, PoolFrame::SessionEnded { reason })
                .await;
        });
    }

    /// Subscribe to this pool's frames, with room for `capacity` unread ones.
    ///
    /// Falling further behind than `capacity` ENDS the subscription - see
    /// [`FrameSubscription`](super::frames::FrameSubscription) for why a gap is not an option.
    pub async fn subscribe_frames(&self, capacity: usize) -> FrameSubscription {
        self.fanout.subscribe(capacity).await
    }
}

/// The peak the pool treats as the chain's, given what its held peers have announced.
///
/// The LOWER MEDIAN of `announced`, and `None` when nobody has announced anything.
///
/// A median rather than a maximum because this number decides which peers are evicted for lag, and
/// a maximum is settable by a single voice: one peer claiming an absurd height would make every
/// honest peer look hopelessly behind. Raising a median requires more than half the held peers to
/// agree, which is the plurality NC-12 already rests on. Lowering it takes only half at an even
/// size, which is harmless: a lower reference evicts fewer peers, never more.
///
/// The LOWER median on an even-sized set — the smaller of the two middles — so the bar is the more
/// forgiving of the two candidates. Eviction is destructive and a redial is not free, so where the
/// pool is genuinely split between two heights the tie is resolved toward keeping peers.
fn reference_peak(announced: &[u32]) -> Option<u32> {
    if announced.is_empty() {
        return None;
    }
    let mut sorted = announced.to_vec();
    sorted.sort_unstable();
    Some(sorted[(sorted.len() - 1) / 2])
}

/// Translate a decoded `CoinStateUpdate` into the frame its subscribers see.
///
/// The destructuring is the guard, not ceremony. A field added upstream to `CoinStateUpdate` must
/// be named here or this stops compiling; the old field-access form (`update.height`, ...) accepted
/// a new field silently, which is how WU2 shipped a frame with `peak_hash` missing and left
/// subscribers pairing a new height with a stale hash. Never reintroduce `..`.
fn coin_states_frame(update: CoinStateUpdate) -> PoolFrame {
    let CoinStateUpdate {
        height,
        fork_height,
        peak_hash,
        items,
    } = update;
    PoolFrame::CoinStates {
        height,
        fork_height,
        peak_hash,
        items,
    }
}

/// Construction and admission reachable from OTHER modules' tests.
///
/// [`PeerPool::new`] dials the network, so a test of anything built ON the pool - the backend's
/// absence corroboration, for one - cannot use it. These wrap the private internals rather than
/// widening them, so production code keeps exactly one admission path.
#[cfg(test)]
impl PeerPool {
    pub(crate) fn for_tests(max_peers: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            next_idx: AtomicUsize::new(0),
            max_peers,
            tls: connect::create_generated_tls().expect("generate a TLS identity"),
            network: NetworkType::Mainnet,
            connect_timeout: Duration::from_millis(1),
            peak_height: Arc::new(AtomicU32::new(0)),
            fanout: Arc::new(FrameFanout::new()),
            dead_sessions: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub(crate) async fn admit_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
    ) -> bool {
        self.admitted(peer, address, origin).await
    }

    /// Admit a connection under a freshly allocated session, reporting only whether it was taken.
    ///
    /// Production admits through [`admit_and_follow`](Self::admit_and_follow), which also starts
    /// the session; a test that only cares about the pool's membership uses this so it does not
    /// have to invent a receiver it will never feed.
    pub(crate) async fn admitted(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
    ) -> bool {
        let session = self.fanout.allocate_session(address).session;
        self.admit(peer, address, origin, session, Arc::new(AtomicU32::new(0)))
            .await
    }

    /// Admit a connection that has already announced `peak`.
    ///
    /// A peer's peak is otherwise only reachable by feeding a real `NewPeakWallet` down a live
    /// session, which a membership test has no reason to build. Wrapping the private field rather
    /// than widening it keeps production code on exactly one admission path.
    pub(crate) async fn admit_at_peak_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
        peak: u32,
    ) -> bool {
        let session = self.fanout.allocate_session(address).session;
        self.admit(
            peer,
            address,
            origin,
            session,
            Arc::new(AtomicU32::new(peak)),
        )
        .await
    }

    /// The peak this pool has recorded for `address`, if it holds it.
    pub(crate) async fn recorded_peak_for_tests(&self, address: SocketAddr) -> Option<u32> {
        self.entries
            .read()
            .await
            .iter()
            .find(|e| e.address == address)
            .map(|e| e.last_peak.load(Ordering::Relaxed))
    }

    /// Admit a connection AND follow `receiver`, exactly as a real dial would.
    /// Run only the lag half of [`maintain`](Self::maintain), which does not dial.
    pub(crate) async fn evict_lagging_peers_for_tests(&self) -> Vec<SocketAddr> {
        self.evict_lagging_peers().await
    }

    pub(crate) async fn admit_and_follow_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        receiver: mpsc::Receiver<Message>,
        origin: connect::PeerOrigin,
    ) -> bool {
        self.admit_and_follow(peer, address, receiver, origin).await
    }

    /// Run only the dead-session half of [`maintain`](Self::maintain), which does not dial.
    pub(crate) async fn eject_dead_sessions_for_tests(&self) {
        self.eject_dead_sessions().await;
    }

    /// The addresses currently held, in pool order.
    pub(crate) async fn held_addresses_for_tests(&self) -> Vec<SocketAddr> {
        self.entries
            .read()
            .await
            .iter()
            .map(|e| e.address)
            .collect()
    }

    /// Admit a connection as if it had entered the pool at `admitted_at`.
    ///
    /// Age is otherwise only reachable by waiting, and a test that waits five minutes is a test
    /// nobody runs. Wrapping the private field rather than widening it keeps production code on
    /// exactly one admission path.
    pub(crate) async fn admit_at_for_tests(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
        admitted_at: Instant,
    ) -> bool {
        if !self.admitted(peer, address, origin).await {
            return false;
        }
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.address == address) {
            entry.admitted_at = admitted_at;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::connect::{create_generated_tls, PeerOrigin};
    use crate::peer::plurality::{default_max_peers, QUORUM_SAMPLE};
    use crate::peer::test_support::{address, address_v6, loopback_peer};

    use super::PeerPool as _Pool;
    fn empty_pool(max_peers: usize) -> PeerPool {
        _Pool::for_tests(max_peers)
    }

    /// **The defect, and the one shape that separates a locked re-check from a TOCTOU dedupe.**
    ///
    /// Eight fills of the SAME address are admitted CONCURRENTLY, which is how the pool fills in
    /// production: `PeerPool::new` races `max_peers` dials with no knowledge of each other, and each
    /// may return the same priority address. A dedupe that reads the entry list before taking the
    /// write lock passes a sequential test and fails this one - every task observes the address
    /// absent, then every task pushes.
    ///
    /// `max_peers` is 8, not 1, deliberately: a capacity of one would make the pool reject the
    /// duplicates for being FULL rather than for being duplicates, and would stay green with the
    /// distinctness check deleted entirely.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_address_cannot_fill_the_pool_however_many_fills_race() {
        let pool = Arc::new(empty_pool(8));
        let peer = loopback_peer().await;
        let occupied = address(1);

        let mut fills = Vec::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admitted(peer, occupied, PeerOrigin::Priority).await
            }));
        }

        let admitted = futures_util::future::join_all(fills)
            .await
            .into_iter()
            .filter(|r| *r.as_ref().expect("the admission task must not panic"))
            .count();

        assert_eq!(
            admitted, 1,
            "exactly one fill of an address may be admitted"
        );
        assert_eq!(
            pool.peer_count().await,
            1,
            "eight concurrent fills of one address must leave one connection, not eight"
        );
    }

    /// The control that keeps the test above honest: concurrency itself must not cost admissions.
    ///
    /// Without this, an `admit` that rejected everything after the first - or that lost racing
    /// pushes - would satisfy the distinctness test while breaking the pool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_addresses_all_fill_concurrently() {
        let pool = Arc::new(empty_pool(8));
        let peer = loopback_peer().await;

        let mut fills = Vec::new();
        for octet in 1..=8u8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admitted(peer, address(octet), PeerOrigin::Discovered)
                    .await
            }));
        }
        futures_util::future::join_all(fills).await;

        assert_eq!(
            pool.peer_count().await,
            8,
            "eight distinct addresses must all be admitted"
        );
    }

    /// Capacity is enforced in the same critical section, so racing fills cannot overshoot it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_fills_never_exceed_max_peers() {
        let pool = Arc::new(empty_pool(3));
        let peer = loopback_peer().await;

        let mut fills = Vec::new();
        for octet in 1..=10u8 {
            let pool = Arc::clone(&pool);
            let peer = peer.clone();
            fills.push(tokio::spawn(async move {
                pool.admitted(peer, address(octet), PeerOrigin::Discovered)
                    .await
            }));
        }
        futures_util::future::join_all(fills).await;

        assert_eq!(pool.peer_count().await, 3, "max_peers is a hard ceiling");
    }

    /// **A preferred peer is not a corroborating one.**
    ///
    /// Two `Discovered` peers sit beside one `Priority` peer, so the two counts differ by exactly
    /// the priority entry. A single-origin fixture cannot show that: all-priority or all-discovered
    /// both make the two counts move together, which an implementation returning `peer_count` for
    /// both would satisfy.
    #[tokio::test]
    async fn a_preferred_peer_is_held_but_not_counted_as_an_independent_opinion() {
        let pool = empty_pool(5);
        let peer = loopback_peer().await;

        assert!(
            pool.admitted(peer.clone(), address(1), PeerOrigin::Priority)
                .await
        );
        assert!(
            pool.admitted(peer.clone(), address(2), PeerOrigin::Discovered)
                .await
        );
        assert!(
            pool.admitted(peer, address(3), PeerOrigin::Discovered)
                .await
        );

        assert_eq!(pool.peer_count().await, 3, "three connections are held");
        assert_eq!(
            pool.independent_peer_count().await,
            2,
            "the co-resident peer is held and read from, but is not an independent voice"
        );
    }

    /// An ejected address is admissible again - distinctness must not become a permanent ban.
    #[tokio::test]
    async fn an_ejected_address_can_be_admitted_again() {
        let pool = empty_pool(5);
        let peer = loopback_peer().await;
        let addr = address(1);

        assert!(
            pool.admitted(peer.clone(), addr, PeerOrigin::Discovered)
                .await
        );
        assert!(
            !pool
                .admitted(peer.clone(), addr, PeerOrigin::Discovered)
                .await,
            "still held, so still a duplicate"
        );

        pool.eject_peer(addr).await;

        assert!(
            pool.admitted(peer, addr, PeerOrigin::Discovered).await,
            "a re-dialled peer must be admissible after ejection"
        );
        assert_eq!(pool.peer_count().await, 1);
    }

    /// `max_peers: 0` attempts no connection at all, so the pool is deterministically
    /// empty offline - an exact, network-free fixture for the empty-pool branch.
    async fn pool_with_no_connection_attempts(
        requirement: PeerRequirement,
    ) -> Result<PeerPool, ChiaQueryError> {
        PeerPool::new(
            NetworkType::Mainnet,
            create_generated_tls().expect("generate a TLS identity"),
            0,
            requirement,
            Duration::from_millis(1),
        )
        .await
    }

    /// The control: an empty pool is still fatal when nothing can serve in its place.
    #[tokio::test]
    async fn empty_pool_is_fatal_when_peers_are_required() {
        assert!(matches!(
            pool_with_no_connection_attempts(PeerRequirement::Required).await,
            Err(ChiaQueryError::PeerDiscoveryFailed)
        ));
    }

    /// The fix: with a fallback able to serve, an empty pool must not deny the client.
    #[tokio::test]
    async fn empty_pool_is_tolerated_when_peers_are_optional() {
        let pool = pool_with_no_connection_attempts(PeerRequirement::Optional)
            .await
            .expect("an optional peer pool must construct with zero peers");
        assert!(!pool.has_peers().await);
    }

    /// **The count is what is HELD, never what was asked for.**
    ///
    /// Built by hand rather than through [`PeerPool::new`] so `max_peers` can be a realistic 5
    /// while the pool provably holds nothing - the one shape that separates a measurement from a
    /// configured intention. A `peer_count` that returned `max_peers` would satisfy every
    /// assertion reachable through the offline constructor, whose `max_peers` is necessarily 0,
    /// and would then report "5 peers" on a machine holding none.
    #[tokio::test]
    async fn an_unfilled_pool_counts_what_it_holds_not_the_target_it_was_given() {
        let pool = empty_pool(5);

        assert_eq!(
            pool.peer_count().await,
            0,
            "held is 0 while the target is 5"
        );
        assert!(!pool.has_peers().await);
    }

    /// **NC-12 cycling: an over-age peer is rotated out by the TIMER, with nothing having failed.**
    ///
    /// The distinguishing fixture is the pair. One entry was admitted well past `PEER_LIFETIME`
    /// ago, one moments ago, and BOTH are healthy - no request is made, so `eject_peer`, the
    /// pool's only other eviction, is never reached. A cycler that fired on failure, or one that
    /// swept indiscriminately, changes the outcome here: the first leaves both peers, the second
    /// removes both.
    #[tokio::test]
    async fn an_over_age_peer_is_rotated_out_with_no_request_having_failed() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        let stale = address(1);
        let fresh = address(2);
        let long_past = Instant::now() - (PEER_LIFETIME + Duration::from_secs(100));

        assert!(
            pool.admit_at_for_tests(peer.clone(), stale, PeerOrigin::Discovered, long_past)
                .await
        );
        assert!(
            pool.admit_at_for_tests(peer, fresh, PeerOrigin::Discovered, Instant::now())
                .await
        );

        let rotated = pool.cycle_expired_peers().await;

        assert_eq!(
            rotated,
            Some(stale),
            "the peer past its lifetime must be rotated out on age alone"
        );
        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![fresh],
            "cycling must remove the over-age peer and keep the fresh one"
        );
    }

    /// The control: a pool of healthy, recent peers is left ALONE.
    ///
    /// Without it, a cycler that ejected the oldest entry unconditionally - no lifetime check at
    /// all - would satisfy the test above while churning the pool on every pass.
    #[tokio::test]
    async fn peers_within_their_lifetime_are_not_rotated() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=3u8 {
            assert!(
                pool.admit_at_for_tests(
                    peer.clone(),
                    address(octet),
                    PeerOrigin::Discovered,
                    Instant::now() - (PEER_LIFETIME - Duration::from_secs(30)),
                )
                .await
            );
        }

        assert_eq!(
            pool.cycle_expired_peers().await,
            None,
            "a peer inside its lifetime must not be rotated"
        );
        assert_eq!(pool.peer_count().await, 3);
    }

    /// A priority entry is not rotated: cycling it would re-dial the same address.
    #[tokio::test]
    async fn an_over_age_priority_peer_is_not_rotated() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;
        let long_past = Instant::now() - (PEER_LIFETIME + Duration::from_secs(100));

        assert!(
            pool.admit_at_for_tests(peer, address(1), PeerOrigin::Priority, long_past)
                .await
        );

        assert_eq!(pool.cycle_expired_peers().await, None);
        assert_eq!(pool.peer_count().await, 1);
    }

    /// **The sizing property: one priority entry must not cost the quorum.**
    ///
    /// A pool filled to the SHIPPED default - one priority entry, which is the ordinary case
    /// because the dialler tries the loopback first, and discovered peers for the rest - must
    /// still hold more independent voices than a sample needs. At the previous default of 5 this
    /// fixture yields four, and the assertion fails.
    #[tokio::test]
    async fn one_priority_entry_does_not_cost_the_quorum() {
        let pool = empty_pool(default_max_peers());
        let peer = loopback_peer().await;

        assert!(
            pool.admitted(peer.clone(), address(1), PeerOrigin::Priority)
                .await
        );
        for octet in 2..=(default_max_peers() as u8) {
            assert!(
                pool.admitted(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(pool.peer_count().await, default_max_peers());
        // A whole sample, plus the one session a subscriber is following and which therefore
        // cannot corroborate itself.
        let owed = QUORUM_SAMPLE + 1;
        let independent = pool.independent_peer_count().await;
        assert!(
            independent >= owed,
            "a full pool holding one priority entry still owes a whole sample plus the session being followed; it holds {independent}"
        );
    }

    /// **Below the floor the pool REFUSES rather than corroborating with fewer voices.**
    ///
    /// Two discovered peers means exactly one corroborator once the answering peer is set aside -
    /// one short. The wrong implementation is not an error, it is a *degradation*: proceeding on
    /// that single second opinion and still calling the result corroborated. So the assertion is
    /// on the refusal AND on the count it reports, which a bare boolean could not distinguish from
    /// an empty pool.
    #[tokio::test]
    async fn a_pool_below_the_corroboration_floor_refuses_rather_than_degrading() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=2u8 {
            assert!(
                pool.admitted(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(
            pool.corroboration_readiness(address(1)).await,
            CorroborationReadiness::Insufficient {
                corroborators: 1,
                required: CORROBORATION_FLOOR,
            }
        );
    }

    /// The control: at the floor exactly, corroboration ARMS.
    ///
    /// Pins the bound from the other side - a gate that refused everything would satisfy the test
    /// above on its own.
    #[tokio::test]
    async fn a_pool_at_the_corroboration_floor_arms() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=(CORROBORATION_FLOOR as u8 + 1) {
            assert!(
                pool.admitted(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }

        assert_eq!(
            pool.corroboration_readiness(address(1)).await,
            CorroborationReadiness::Armed {
                corroborators: CORROBORATION_FLOOR
            }
        );
    }

    /// A preferred peer is not a corroborator, so it cannot arm the gate.
    ///
    /// Same peer COUNT as the arming control above, different origins - the one fixture shape that
    /// separates "enough connections" from "enough independent voices" (dig_ecosystem#2648).
    #[tokio::test]
    async fn priority_peers_cannot_arm_the_corroboration_gate() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        assert!(
            pool.admitted(peer.clone(), address(1), PeerOrigin::Discovered)
                .await
        );
        for octet in 2..=(CORROBORATION_FLOOR as u8 + 1) {
            assert!(
                pool.admitted(peer.clone(), address(octet), PeerOrigin::Priority)
                    .await
            );
        }

        assert!(matches!(
            pool.corroboration_readiness(address(1)).await,
            CorroborationReadiness::Insufficient { .. }
        ));
    }

    /// **A PRIORITY peer answering does not spend an independent voice it never occupied.**
    ///
    /// The fixture varies exactly one thing against
    /// [`a_pool_at_the_corroboration_floor_arms`]: WHO was asked. The independent set is a floor's
    /// worth on its own, and the answer comes from a preferred peer that is not in that set - so
    /// charging the asker's slot against it, as a blind `- 1` does, reports a pool with two
    /// genuine corroborators as having one.
    ///
    /// That is not a missed opportunity, it is a downgrade with a destination: the answer becomes
    /// `Uncorroborated*` and the router settles it against the centralized coinset tier
    /// (`router.rs`), substituting one HTTPS source for the untrusted plurality NC-12 asks for. On
    /// a host with `TRUSTED_FULLNODE` or a co-resident node - the configuration this pool is sized
    /// for - that is the ordinary path, not an edge case.
    #[tokio::test]
    async fn a_priority_peer_answering_does_not_consume_an_independent_slot() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        for octet in 1..=(CORROBORATION_FLOOR as u8) {
            assert!(
                pool.admitted(peer.clone(), address(octet), PeerOrigin::Discovered)
                    .await
            );
        }
        let preferred = address(200);
        assert!(
            pool.admitted(peer.clone(), preferred, PeerOrigin::Priority)
                .await
        );

        assert_eq!(
            pool.corroboration_readiness(preferred).await,
            CorroborationReadiness::Armed {
                corroborators: CORROBORATION_FLOOR
            },
            "a preferred peer is not an independent voice, so answering from one cannot cost the              independent set a member"
        );
    }

    #[tokio::test]
    async fn corroboration_readiness_and_select_use_the_same_predicate() {
        let pool = empty_pool(7);
        let peer = loopback_peer().await;

        // Build a pool with mixed origins: Priority, Discovered, and the asker itself.
        let asked = address(100);
        let priority = address(200);
        let discovered_1 = address(1);
        let discovered_2 = address(2);

        assert!(
            pool.admitted(peer.clone(), asked, PeerOrigin::Discovered)
                .await
        );
        assert!(
            pool.admitted(peer.clone(), priority, PeerOrigin::Priority)
                .await
        );
        assert!(
            pool.admitted(peer.clone(), discovered_1, PeerOrigin::Discovered)
                .await
        );
        assert!(
            pool.admitted(peer.clone(), discovered_2, PeerOrigin::Discovered)
                .await
        );

        // Both should see exactly 2 corroborators: discovered_1 and discovered_2.
        // Not `asked` (excluded by address), not `priority` (excluded by origin).
        let readiness = pool.corroboration_readiness(asked).await;
        let selected = pool.select_corroborating_peers(asked).await;

        let readiness_count = match readiness {
            CorroborationReadiness::Armed { corroborators } => corroborators,
            CorroborationReadiness::Insufficient { corroborators, .. } => corroborators,
        };

        assert_eq!(
            readiness_count,
            selected.len(),
            "corroboration_readiness and select_corroborating_peers must use the same predicate"
        );
        assert_eq!(
            readiness_count, 2,
            "both should count exactly the two Discovered peers that are not the asker"
        );
    }

    /// **`FILL_ROUNDS` is DERIVED from the dialler, so a new priority address cannot starve it.**
    ///
    /// Network-free arithmetic, in the shape of the pool-sizing derivations: the priority
    /// addresses are tried sequentially and a round admits at most one of them, so `PRIORITY_SLOTS`
    /// rounds can pass before any dial reaches discovery. What is left must still be enough for a
    /// discovery round AND one of attrition.
    ///
    /// The literal `3` this replaced satisfied that only while `PRIORITY_SLOTS` was 1. At 2 it
    /// left exactly one discovery round with no slack, on precisely the host the rounds exist for.
    #[test]
    fn fill_rounds_leaves_a_discovery_round_and_one_of_attrition() {
        let for_discovery = FILL_ROUNDS - PRIORITY_SLOTS;

        assert_eq!(
            for_discovery, 2,
            "FILL_ROUNDS ({FILL_ROUNDS}) minus the {PRIORITY_SLOTS} rounds the priority              addresses can consume must leave a discovery round and one of attrition"
        );
        assert_eq!(
            FILL_ROUNDS,
            PRIORITY_SLOTS + 2,
            "FILL_ROUNDS stays coupled to PRIORITY_SLOTS + 2"
        );
        // This assertion and the one above state the same mathematical fact: FILL_ROUNDS - PRIORITY_SLOTS == 2
        // and FILL_ROUNDS == PRIORITY_SLOTS + 2 are equivalent. Both are present because changing either
        // one invalidates FILL_ROUNDS' budget, but they do not add independent verification.
    }
    // -----------------------------------------------------------------------
    // Session lifecycle: attribution, loud endings, and the ejection they drive
    // -----------------------------------------------------------------------

    use chia_protocol::{Bytes32, Coin, CoinState};

    use super::super::frames::SourcedFrame;

    /// A well-formed `NewPeakWallet` message at `height`.
    fn peak_message(height: u32) -> Message {
        let peak = NewPeakWallet::new(Bytes32::new([height as u8; 32]), height, 0, 0);
        Message {
            msg_type: ProtocolMessageTypes::NewPeakWallet,
            id: None,
            data: peak.to_bytes().expect("encode a peak").into(),
        }
    }

    /// A `NewPeakWallet` message whose BODY cannot be decoded.
    ///
    /// The type byte is honest and the payload is one byte, far short of the
    /// `Bytes32 + u32 + u128 + u32` the body requires - which is what any peer can send at will,
    /// costing it nothing.
    fn undecodable_peak_message() -> Message {
        Message {
            msg_type: ProtocolMessageTypes::NewPeakWallet,
            id: None,
            data: vec![0x00].into(),
        }
    }

    /// Drain the frames that have arrived, waiting briefly for the handler task to run.
    ///
    /// The handler is a separate task, so a bare `try_recv` races it. This yields until the
    /// expected number of frames has arrived or the budget runs out, and returns whatever it has -
    /// so a test asserting on the CONTENT fails on its own assertion rather than on a timeout.
    async fn drain_at_least(
        subscription: &mut FrameSubscription,
        wanted: usize,
    ) -> Vec<SourcedFrame> {
        let mut seen = Vec::new();
        for _ in 0..200 {
            while let Ok(frame) = subscription.try_recv() {
                seen.push(frame);
            }
            if seen.len() >= wanted {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        seen
    }

    /// Admit a peer at `addr` and follow a channel the test itself feeds.
    async fn followed_session(
        pool: &PeerPool,
        addr: SocketAddr,
    ) -> (mpsc::Sender<Message>, FrameSource) {
        let (sender, receiver) = mpsc::channel(8);
        let peer = loopback_peer().await;
        let before = pool.held_addresses_for_tests().await.len();
        assert!(
            pool.admit_and_follow_for_tests(peer, addr, receiver, PeerOrigin::Discovered)
                .await,
            "the fixture peer must be admitted"
        );
        assert_eq!(pool.held_addresses_for_tests().await.len(), before + 1);

        let source = pool
            .entries
            .read()
            .await
            .iter()
            .find(|e| e.address == addr)
            .map(|e| FrameSource {
                address: e.address,
                session: e.session,
            })
            .expect("the admitted entry");
        (sender, source)
    }

    /// **A frame carries the address of the peer that sent it, all the way from the socket.**
    ///
    /// TWO sessions are followed and each is fed a peak of its own. A handler that published
    /// without attribution - or that attributed every frame to one session - gives both frames the
    /// same source and fails here; a one-session fixture cannot tell those apart from correct
    /// behaviour.
    ///
    /// This is the property whose absence let any held peer's `CoinStateUpdate` reach a subscriber
    /// as if it came from the peer that subscriber had chosen to follow.
    #[tokio::test]
    async fn a_frame_reaching_a_subscriber_names_the_session_it_came_from() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;

        let (first, first_source) = followed_session(&pool, address(1)).await;
        let (second, second_source) = followed_session(&pool, address(2)).await;

        first.send(peak_message(100)).await.expect("send");
        second.send(peak_message(200)).await.expect("send");

        let seen = drain_at_least(&mut subscription, 4).await;

        let peaks: Vec<(SocketAddr, u32)> = seen
            .iter()
            .filter_map(|f| match f.frame {
                PoolFrame::Peak { height, .. } => Some((f.source.address, height)),
                _ => None,
            })
            .collect();

        assert!(
            peaks.contains(&(address(1), 100)),
            "peer 1's peak must arrive under peer 1's address: {peaks:?}"
        );
        assert!(
            peaks.contains(&(address(2), 200)),
            "peer 2's peak must arrive under peer 2's address: {peaks:?}"
        );
        assert_ne!(
            first_source.session, second_source.session,
            "two sessions must not share an identity"
        );
    }

    /// **An undecodable frame ENDS the session; it is never skipped.**
    ///
    /// The fixture is ordered so that skipping is distinguishable from ending: a valid peak, then
    /// an undecodable one, then a second valid peak. An implementation that ignores what it cannot
    /// decode - the `if let Ok(..)` this replaces - delivers BOTH peaks and no ending, which is a
    /// subscriber missing an update it will never learn it missed.
    #[tokio::test]
    async fn an_undecodable_frame_ends_the_session_rather_than_being_skipped() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;
        let (sender, source) = followed_session(&pool, address(1)).await;

        sender.send(peak_message(100)).await.expect("send");
        sender.send(undecodable_peak_message()).await.expect("send");
        sender.send(peak_message(101)).await.expect("send");

        let seen = drain_at_least(&mut subscription, 3).await;
        let frames: Vec<&PoolFrame> = seen
            .iter()
            .filter(|f| f.source == source)
            .map(|f| &f.frame)
            .collect();

        assert!(
            frames
                .iter()
                .any(|f| matches!(f, PoolFrame::Peak { height: 100, .. })),
            "the frames before the bad one are still delivered: {frames:?}"
        );
        assert!(
            frames.contains(&&PoolFrame::SessionEnded {
                reason: SessionEndReason::UndecodableFrame
            }),
            "an undecodable frame must END the session, loudly: {frames:?}"
        );
        assert!(
            !frames
                .iter()
                .any(|f| matches!(f, PoolFrame::Peak { height: 101, .. })),
            "nothing after the undecodable frame belongs to this session: {frames:?}"
        );
    }

    /// The control: a session fed only VALID frames is not ended.
    ///
    /// Without it, a handler that ended every session on its first message would satisfy the test
    /// above.
    #[tokio::test]
    async fn a_session_fed_only_valid_frames_stays_open() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;
        let (sender, source) = followed_session(&pool, address(1)).await;

        sender.send(peak_message(100)).await.expect("send");
        sender.send(peak_message(101)).await.expect("send");

        let seen = drain_at_least(&mut subscription, 3).await;
        let frames: Vec<&PoolFrame> = seen
            .iter()
            .filter(|f| f.source == source)
            .map(|f| &f.frame)
            .collect();

        assert!(
            !frames
                .iter()
                .any(|f| matches!(f, PoolFrame::SessionEnded { .. })),
            "a well-behaved session must stay open: {frames:?}"
        );
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f, PoolFrame::Peak { .. }))
                .count(),
            2,
            "both valid peaks must be delivered: {frames:?}"
        );
    }

    /// A transport that closes ends the session loudly rather than silently.
    #[tokio::test]
    async fn a_closed_transport_ends_the_session_loudly() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;
        let (sender, source) = followed_session(&pool, address(1)).await;

        drop(sender);

        let seen = drain_at_least(&mut subscription, 2).await;
        assert!(
            seen.iter().any(|f| f.source == source
                && f.frame
                    == PoolFrame::SessionEnded {
                        reason: SessionEndReason::Disconnected
                    }),
            "a dropped transport must be announced, not left as silence: {seen:?}"
        );
    }

    /// **A peer whose session ended is EJECTED, without waiting for a failed request or the
    /// rotation timer.**
    ///
    /// Two peers are held and only ONE dies, which is the fixture shape that separates ejecting
    /// the right peer from ejecting on any death: a pass that removed both, or removed the wrong
    /// one, fails here. The surviving peer is the control and is never fed anything, so nothing
    /// about it changes except that its neighbour died.
    #[tokio::test]
    async fn a_peer_whose_session_ended_is_ejected_without_waiting_for_a_failure() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;

        let (dying, dying_source) = followed_session(&pool, address(1)).await;
        let (_surviving, _) = followed_session(&pool, address(2)).await;

        drop(dying);

        // Wait for the death to be announced, which is published after it is recorded.
        let seen = drain_at_least(&mut subscription, 3).await;
        assert!(
            seen.iter()
                .any(|f| f.source == dying_source
                    && matches!(f.frame, PoolFrame::SessionEnded { .. })),
            "the fixture depends on the session actually ending: {seen:?}"
        );

        assert_eq!(
            pool.held_addresses_for_tests().await.len(),
            2,
            "a dead session is still HELD until maintenance runs - which is the gap being closed"
        );

        pool.eject_dead_sessions_for_tests().await;

        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![address(2)],
            "exactly the peer whose session ended is removed"
        );
    }

    /// A replacement dialled to the same address is not removed by its predecessor's death.
    ///
    /// The interleaving is a real one and it is the ONLY one that can exhibit this: a request to
    /// the dead connection fails, so `eject_peer` removes it by ADDRESS before maintenance runs;
    /// a refill then re-dials that same address; and only afterwards does maintenance drain the
    /// death that is still recorded against it. An ejection matching on address alone - the
    /// obvious implementation - removes the live replacement there and leaves the pool short, with
    /// nothing anywhere reporting a problem.
    ///
    /// Draining the dead list BEFORE re-admitting cannot show this, because the drain empties the
    /// list and the second pass then has nothing to match with. That ordering was this test's
    /// first shape and it passed against address-only matching, which is to say it proved nothing.
    #[tokio::test]
    async fn a_replacement_at_the_same_address_survives_its_predecessors_death() {
        let pool = empty_pool(4);
        let mut subscription = pool.subscribe_frames(32).await;

        let (dying, dying_source) = followed_session(&pool, address(1)).await;
        drop(dying);

        let seen = drain_at_least(&mut subscription, 2).await;
        assert!(
            seen.iter()
                .any(|f| f.source == dying_source
                    && matches!(f.frame, PoolFrame::SessionEnded { .. })),
            "the fixture depends on the session actually ending: {seen:?}"
        );

        // A request to the dead connection fails first, which is how it leaves `entries` while its
        // death is still recorded.
        pool.eject_peer(address(1)).await;
        assert!(pool.held_addresses_for_tests().await.is_empty());

        let (_replacement, replacement_source) = followed_session(&pool, address(1)).await;
        assert_ne!(replacement_source.session, dying_source.session);

        // Maintenance now drains a death recorded against an address the replacement holds.
        pool.eject_dead_sessions_for_tests().await;

        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![address(1)],
            "the replacement session must survive its predecessor's death"
        );
    }

    /// **Every field of a `CoinStateUpdate` reaches its frame carrying its OWN value.**
    ///
    /// The destructuring in [`coin_states_frame`] makes a DROPPED field a compile error, but a
    /// field crossed with another (`fork_height: height`) still compiles and still ships a frame.
    /// So every value here is distinct — `height` differs from `fork_height`, `peak_hash` from any
    /// other byte pattern in the fixture, and `items` is non-empty — because a fixture that reuses
    /// a value across two fields cannot tell a faithful translation from a swapped one.
    ///
    /// The update is round-tripped through the wire first, so this covers the same decode-then-
    /// translate pair the session loop runs, not a hand-built struct the loop never sees.
    #[test]
    fn a_coin_state_update_transfers_every_field_into_its_frame() {
        let items = vec![CoinState {
            coin: Coin::new(Bytes32::new([7; 32]), Bytes32::new([8; 32]), 9),
            created_height: Some(150),
            spent_height: None,
        }];
        let peak_hash = Bytes32::new([0xBB; 32]);
        let update = CoinStateUpdate::new(200, 199, peak_hash, items.clone());
        let decoded = CoinStateUpdate::from_bytes(
            &update.to_bytes().expect("a CoinStateUpdate is streamable"),
        )
        .expect("its own bytes decode");

        let PoolFrame::CoinStates {
            height: got_height,
            fork_height: got_fork_height,
            peak_hash: got_peak_hash,
            items: got_items,
        } = coin_states_frame(decoded)
        else {
            panic!("a CoinStateUpdate must translate to a CoinStates frame");
        };

        assert_eq!(got_height, 200, "the peak height must be the update's own");
        assert_eq!(
            got_fork_height, 199,
            "fork_height is the reorg depth; crossing it with height reports a rewind that did not happen"
        );
        assert_eq!(
            got_peak_hash, peak_hash,
            "the header hash must travel with the height it belongs to"
        );
        assert_eq!(
            got_items, items,
            "the coin states are the payload; a frame without them tells subscribers nothing about their coins"
        );
    }

    // -----------------------------------------------------------------------
    // Lag eviction: the peer that answers fine, about an older chain (#40)
    // -----------------------------------------------------------------------

    /// The reference height every lag fixture is written against. Arbitrary, but far enough above
    /// `PEAK_LAG_EVICTION` that "behind by more than the bar" never has to saturate at zero.
    const CURRENT: u32 = 9_196_851;

    /// A pool holding `peaks` as discovered peers at `address(i + 1)`, one per entry.
    async fn pool_at_peaks(max_peers: usize, peaks: &[u32]) -> PeerPool {
        let pool = empty_pool(max_peers);
        for (i, peak) in peaks.iter().enumerate() {
            assert!(
                pool.admit_at_peak_for_tests(
                    loopback_peer().await,
                    address(i as u8 + 1),
                    PeerOrigin::Discovered,
                    *peak,
                )
                .await,
                "fixture peer {i} must be admitted"
            );
        }
        pool
    }

    /// **Proves (#40):** a peer that stays connected and merely falls behind is EVICTED, with no
    /// request having failed, no session having ended, and no lifetime having elapsed — the three
    /// removals it previously escaped.
    ///
    /// **The fixture varies exactly ONE actor.** Three peers stay current and one falls behind, so
    /// a sweep that removed everything, or an eviction that fired on something other than lag,
    /// changes the outcome and fails. An all-lagging fixture could not see this at all: the median
    /// would move down with the pool and nobody would be behind it.
    ///
    /// The readiness assertion is the point of the ticket rather than a bonus — the harm was an
    /// `Armed { corroborators }` that counted a peer answering about an older chain.
    #[tokio::test]
    async fn a_lagging_peer_is_evicted_though_nothing_failed_and_no_session_ended() {
        let lagging = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = pool_at_peaks(5, &[CURRENT, CURRENT, CURRENT, lagging]).await;

        let asked = address(9);
        assert_eq!(
            pool.corroboration_readiness(asked).await,
            CorroborationReadiness::Armed { corroborators: 4 },
            "the lagging peer is counted as a current voice until the eviction runs"
        );

        assert_eq!(
            pool.evict_lagging_peers_for_tests().await,
            vec![address(4)],
            "only the peer behind the reference is evicted"
        );
        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![address(1), address(2), address(3)],
            "the three current peers are untouched"
        );
        assert_eq!(
            pool.corroboration_readiness(asked).await,
            CorroborationReadiness::Armed { corroborators: 3 },
            "the corroboration denominator now counts only peers on the current chain"
        );
    }

    /// **Proves:** the eviction bar is `PEAK_LAG_EVICTION`, pinned from BOTH sides.
    ///
    /// A bound tested only from below can only confirm itself: an implementation that evicted
    /// everything, or that used `>=` where the doc says "more than", passes a one-sided test. So
    /// exactly-at-the-bar must be KEPT and one block further must be EVICTED, and the two peers
    /// sit in the SAME pool so no other difference between the fixtures can explain the split.
    #[tokio::test]
    async fn the_eviction_bar_keeps_a_peer_at_it_and_drops_the_one_past_it() {
        let at_bar = CURRENT - PEAK_LAG_EVICTION;
        let past_bar = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = pool_at_peaks(5, &[CURRENT, CURRENT, CURRENT, at_bar, past_bar]).await;

        assert_eq!(
            pool.evict_lagging_peers_for_tests().await,
            vec![address(5)],
            "at the bar is current; one block past it is not"
        );
        assert!(
            pool.held_addresses_for_tests().await.contains(&address(4)),
            "a peer exactly at the tolerance must survive"
        );
    }

    /// **Proves (NC-12):** ONE peer claiming an inflated peak cannot evict the honest majority.
    ///
    /// This is the security property of the whole eviction, and it is the assertion that fails if
    /// the reference peak is ever changed from a median to a maximum: against a maximum the three
    /// honest peers are a million blocks behind the liar and all three are evicted, leaving the
    /// pool holding nothing but the hostile peer. A peer claim about the chain is not evidence
    /// (NC-12), and a bar one voice can set is a bar one voice controls.
    #[tokio::test]
    async fn one_peer_claiming_an_inflated_peak_cannot_evict_the_honest_majority() {
        let pool = pool_at_peaks(5, &[CURRENT, CURRENT, CURRENT, CURRENT + 1_000_000]).await;

        assert!(
            pool.evict_lagging_peers_for_tests().await.is_empty(),
            "a single inflated claim must not make the honest peers look behind"
        );
        assert_eq!(
            pool.held_addresses_for_tests().await,
            vec![address(1), address(2), address(3), address(4)],
            "every honest peer is still held"
        );
    }

    /// **Proves:** silence is not lag. A peer admitted moments ago has announced nothing, and its
    /// recorded peak of zero must not read as the furthest-behind entry in the pool.
    ///
    /// Without this the eviction would remove every peer on the maintenance pass that follows its
    /// own admission — the pool destroying its refill as fast as it dials it.
    #[tokio::test]
    async fn a_peer_that_has_announced_nothing_is_never_evicted_as_lagging() {
        let pool = pool_at_peaks(5, &[CURRENT, CURRENT, 0]).await;

        assert!(
            pool.evict_lagging_peers_for_tests().await.is_empty(),
            "a peer that has not spoken has not fallen behind"
        );
        assert!(
            pool.held_addresses_for_tests().await.contains(&address(3)),
            "the silent peer is still held"
        );
    }

    /// **Proves:** a lagging PRIORITY peer is not evicted, for the same reason cycling spares one —
    /// re-dialling reaches the same address, and it is not an independent voice whose count this
    /// eviction exists to correct.
    ///
    /// The discovered lagger in the same pool, at the SAME peak, IS evicted — so the test cannot
    /// pass by evicting nothing, and origin is the only difference that can explain the split.
    #[tokio::test]
    async fn a_lagging_priority_peer_is_kept_while_a_lagging_discovered_peer_goes() {
        let behind = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = empty_pool(5);
        for (i, peak) in [CURRENT, CURRENT, CURRENT, behind].iter().enumerate() {
            assert!(
                pool.admit_at_peak_for_tests(
                    loopback_peer().await,
                    address(i as u8 + 1),
                    PeerOrigin::Discovered,
                    *peak,
                )
                .await
            );
        }
        assert!(
            pool.admit_at_peak_for_tests(
                loopback_peer().await,
                address(5),
                PeerOrigin::Priority,
                behind,
            )
            .await
        );

        assert_eq!(
            pool.evict_lagging_peers_for_tests().await,
            vec![address(4)],
            "the discovered lagger goes and the priority one at the same peak stays"
        );
    }

    /// **Proves (§5.2):** the eviction is address-family agnostic — an IPv6 lagger is removed on
    /// the same terms as an IPv4 one, and a current IPv6 peer is kept.
    ///
    /// The peer tier is IPv6-first and the fleet that motivated this held a `2806:2f0::/32` peer. A
    /// policy proven only against the documentation-range IPv4 addresses every other fixture uses
    /// is proven against half the network; that is the same defect with a smaller number.
    #[tokio::test]
    async fn lag_eviction_treats_an_ipv6_peer_exactly_as_it_treats_an_ipv4_one() {
        let behind = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = empty_pool(5);
        for (addr, peak) in [
            (address_v6(1), CURRENT),
            (address(1), CURRENT),
            (address_v6(2), behind),
        ] {
            assert!(
                pool.admit_at_peak_for_tests(
                    loopback_peer().await,
                    addr,
                    PeerOrigin::Discovered,
                    peak,
                )
                .await
            );
        }

        assert_eq!(
            pool.evict_lagging_peers_for_tests().await,
            vec![address_v6(2)],
            "an IPv6 lagger is evicted"
        );
        assert!(
            pool.held_addresses_for_tests()
                .await
                .contains(&address_v6(1)),
            "a current IPv6 peer is kept"
        );
    }

    /// **Proves the WIRING**, not the policy: `maintain` — the one pass a request actually drives —
    /// runs the lag eviction. A unit test of `evict_lagging_peers` alone cannot see whether
    /// anything calls it, and an eviction nothing calls is indistinguishable from no eviction.
    ///
    /// `maintain` ends with `try_refill`, which dials, so the call is bounded by a timeout and the
    /// assertion is made afterwards regardless of how the refill went. Eviction runs BEFORE the
    /// refill, so it has already happened either way — the timeout can cost the test the refill,
    /// never its subject.
    #[tokio::test]
    async fn maintain_runs_the_lag_eviction() {
        let lagging = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = pool_at_peaks(4, &[CURRENT, CURRENT, CURRENT, lagging]).await;

        let _ = tokio::time::timeout(Duration::from_secs(20), pool.maintain()).await;

        assert!(
            !pool.held_addresses_for_tests().await.contains(&address(4)),
            "the lagging peer must be gone after a maintenance pass"
        );
    }

    /// **Proves:** a peer own `NewPeakWallet` is what sets its recorded peak — the PRODUCTION write
    /// path, not the test seam. Without this the eviction would be measured entirely against
    /// numbers tests injected, and a handler that recorded nothing would leave every peer at zero
    /// and therefore permanently un-evictable.
    #[tokio::test]
    async fn a_peers_recorded_peak_comes_from_the_peak_it_announces() {
        let pool = empty_pool(2);
        let mut subscription = pool.subscribe_frames(8).await;
        let addr = address(1);
        let (session, _source) = followed_session(&pool, addr).await;

        session.send(peak_message(4242)).await.expect("send a peak");
        drain_at_least(&mut subscription, 1).await;

        // The handler writes the peak and then publishes, both from its own detached task, so the
        // write is ordered before the frame this test just saw. The poll is for the scheduler, not
        // for the ordering: it bounds the wait rather than granting the handler extra chances.
        for _ in 0..200 {
            if pool.recorded_peak_for_tests(addr).await == Some(4242) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(
            pool.recorded_peak_for_tests(addr).await,
            Some(4242),
            "the announced peak must be recorded against the peer that announced it"
        );
    }

    // -----------------------------------------------------------------------
    // The reference peak itself
    // -----------------------------------------------------------------------

    /// **Proves:** the reference is the LOWER median, so an even split resolves toward keeping
    /// peers, and it is `None` when nobody has spoken — never a zero, which every peer is above and
    /// which would evict the entire pool.
    #[test]
    fn the_reference_peak_is_the_lower_median_and_is_unknown_when_nobody_has_spoken() {
        assert_eq!(reference_peak(&[]), None);
        assert_eq!(reference_peak(&[7]), Some(7));
        assert_eq!(
            reference_peak(&[10, 20]),
            Some(10),
            "the LOWER of two middles"
        );
        assert_eq!(
            reference_peak(&[30, 10, 20]),
            Some(20),
            "order does not matter"
        );
    }

    /// **Proves:** one outlier in either direction cannot move the reference — the property the
    /// hostile-peak test rests on, stated on the pure function so a change to it is caught here
    /// rather than only through the pool.
    #[test]
    fn a_single_outlier_cannot_move_the_reference_peak() {
        assert_eq!(reference_peak(&[100, 100, 100, u32::MAX]), Some(100));
        assert_eq!(reference_peak(&[100, 100, 100, 1]), Some(100));
    }

    // -----------------------------------------------------------------------
    // Who VOTES in the reference peak (#42)
    // -----------------------------------------------------------------------

    /// Admit `entries` as `(origin, announced peak)` at `address(i + 1)`.
    ///
    /// [`pool_at_peaks`] cannot express these fixtures: its peers are all `Discovered`, and the
    /// property under test is precisely what changes when the origins DIFFER.
    async fn pool_of(max_peers: usize, entries: &[(PeerOrigin, u32)]) -> PeerPool {
        let pool = empty_pool(max_peers);
        for (i, (origin, peak)) in entries.iter().enumerate() {
            assert!(
                pool.admit_at_peak_for_tests(
                    loopback_peer().await,
                    address(i as u8 + 1),
                    *origin,
                    *peak,
                )
                .await,
                "fixture entry {i} must be admitted"
            );
        }
        pool
    }

    /// **Proves (#42):** hostile `Priority` entries cannot VOTE the only independent peer out of
    /// the pool.
    ///
    /// `Priority` entries are already exempt from BEING evicted. That is a different property from
    /// whether they COUNT toward the reference the eviction is measured against, and granting the
    /// second is what let two co-resident entries — the exact pair a local attacker supplies
    /// (dig_ecosystem#2648) — carry the median to a fabricated height and take
    /// `independent_peer_count` from 1 to 0.
    ///
    /// **The fixture is the minimum that can express the failure and is sized from the crate's own
    /// bound.** The attack needs the priority voices to be at least as many as the discovered ones
    /// — at two discovered peers the median already sits on an honest voice — so the worst case
    /// reachable in production is `PRIORITY_SLOTS` hostile entries against ONE discovered peer.
    /// That is not an arbitrarily small fixture; it is the whole of what the dialler can produce.
    #[tokio::test]
    async fn hostile_priority_entries_cannot_vote_the_only_independent_peer_out() {
        let inflated = CURRENT + 1_000_000;
        let pool = pool_of(
            5,
            &[
                (PeerOrigin::Priority, inflated),
                (PeerOrigin::Priority, inflated),
                (PeerOrigin::Discovered, CURRENT),
            ],
        )
        .await;
        assert_eq!(pool.independent_peer_count().await, 1);

        assert!(
            pool.evict_lagging_peers_for_tests().await.is_empty(),
            "peers that are not independent voices must not set the bar that evicts one"
        );
        assert_eq!(
            pool.independent_peer_count().await,
            1,
            "the pool must not be emptied of independent voices by entries that never were any"
        );
    }

    /// **The control, and the placement proof.** The filter must narrow WHOSE peak is counted, not
    /// switch the eviction off.
    ///
    /// Here the priority entries are far BELOW the chain rather than above it, which drags an
    /// unfiltered median down onto the lagging peer and shields it. Counting only the discovered
    /// peaks puts the reference back on the honest majority and the lagger goes. So this fixture
    /// fails both for an implementation that stopped evicting and for one that kept the priority
    /// votes — and its verdict is the OPPOSITE of the test above, which no "evict less" change can
    /// satisfy at the same time.
    #[tokio::test]
    async fn priority_entries_below_the_chain_cannot_shield_a_lagging_discovered_peer() {
        let stale = CURRENT - 500_000;
        let behind = CURRENT - PEAK_LAG_EVICTION - 1;
        let pool = pool_of(
            8,
            &[
                (PeerOrigin::Priority, stale),
                (PeerOrigin::Priority, stale),
                (PeerOrigin::Discovered, CURRENT),
                (PeerOrigin::Discovered, CURRENT),
                (PeerOrigin::Discovered, CURRENT),
                (PeerOrigin::Discovered, behind),
            ],
        )
        .await;

        assert_eq!(
            pool.evict_lagging_peers_for_tests().await,
            vec![address(6)],
            "the reference follows the independent peers, so the lagger is still evicted"
        );
    }

    // -----------------------------------------------------------------------
    // Dialling wider than capacity (dig_ecosystem#2836)
    // -----------------------------------------------------------------------

    /// A round of dials against a simulated network of heterogeneous peers.
    ///
    /// `network[i]` is the handshake time of the i-th peer discovery offers, and the pool opens
    /// `dials` of them. The list is deliberately NOT sorted: the peers that answer first are
    /// mediocre and the good ones sit further in, which is what makes a narrow dial and a wide one
    /// reach different answers. A network whose best peers came first would let a pool with no
    /// selection at all score identically, and prove nothing.
    fn round_over(network: &[u64], dials: usize) -> Vec<DialCandidate<usize>> {
        network
            .iter()
            .take(dials)
            .enumerate()
            .map(|(i, ms)| DialCandidate {
                origin: PeerOrigin::Discovered,
                handshake: Duration::from_millis(*ms),
                connection: i,
            })
            .collect()
    }

    /// **Proves (dig_ecosystem#2836):** dialling wider than capacity retains BETTER peers than
    /// dialling exactly to capacity, on the same network and for the same number of slots.
    ///
    /// The comparison is the test. Both arms fill `SLOTS` slots from the same peers in the same
    /// order and differ only in how many dials the round opened, so nothing but the width can
    /// explain the split — and if [`DIAL_OVERSUBSCRIPTION`] were 1, `dials_for(SLOTS)` would equal
    /// `SLOTS`, the two arms would be the same round, and the strict inequality below would fail.
    /// A test that passed at either width would be measuring the sort, not the oversubscription.
    #[test]
    fn dialling_wider_than_capacity_retains_better_peers_than_dialling_to_it() {
        const SLOTS: usize = 4;
        // Eight peers. The first four - all a narrow round can see - are the slow half.
        let network = [900, 700, 800, 750, 40, 90, 60, 20];

        let worst_kept = |dials: usize| {
            most_credible(round_over(&network, dials), SLOTS)
                .iter()
                .map(|c| c.handshake)
                .max()
                .expect("the round admitted nothing")
        };

        let narrow = worst_kept(SLOTS);
        let wide = worst_kept(dials_for(SLOTS));

        assert!(
            wide < narrow,
            "the wide round's worst kept peer ({wide:?}) must beat the narrow round's ({narrow:?})"
        );
        assert_eq!(
            most_credible(round_over(&network, dials_for(SLOTS)), SLOTS).len(),
            SLOTS,
            "widening the dial must not change how many slots are filled"
        );
    }

    /// **Proves:** the width is the CANDIDATE set only — capacity stays derived from the sample.
    ///
    /// The regression this ticket was raised from was a pool sized by a literal, and the ticket's
    /// own premise ("holds a fixed max of 5") would have reintroduced it. So the number of peers a
    /// round admits is pinned to the slots asked for, at every width, and capacity is pinned to
    /// the derivation rather than to any number this change introduces.
    #[test]
    fn oversubscription_widens_the_candidate_set_and_never_the_capacity() {
        let network: Vec<u64> = (1..=64).collect();

        for slots in 1..=default_max_peers() {
            assert!(
                dials_for(slots) > slots,
                "a round with {slots} slots must offer more candidates than it can keep"
            );
            assert_eq!(
                most_credible(round_over(&network, dials_for(slots)), slots).len(),
                slots,
                "a round fills the slots it was given, never the dials it opened"
            );
        }

        assert_eq!(
            default_max_peers(),
            PRIORITY_SLOTS + 1 + QUORUM_SAMPLE + 1,
            "capacity remains derived from the sample; oversubscription is not one of its terms"
        );
    }

    /// **Proves:** a priority candidate is never crowded out by a faster stranger.
    ///
    /// The priority entry is the SLOWEST in the round, so any ranking that considered only latency
    /// would discard it — and on a single-slot round it is discarded in favour of a peer the
    /// operator did not choose. It is not an independent voice (#42), so keeping it costs the
    /// quorum nothing and losing it costs the operator the node they configured.
    #[test]
    fn a_priority_candidate_outranks_a_faster_stranger() {
        let mut round = round_over(&[1, 2, 3], 3);
        round.push(DialCandidate {
            origin: PeerOrigin::Priority,
            handshake: Duration::from_millis(999),
            connection: 99,
        });

        let kept = most_credible(round, 1);

        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0].origin,
            PeerOrigin::Priority,
            "the operator's own node is admitted because they said so, not because it was quick"
        );
    }

    /// **Proves:** with no independent voice having spoken, the pool evicts NOTHING — stated, not
    /// left to fall out of the arithmetic.
    ///
    /// This is the decision the filter forces: once only `Discovered` peaks vote, a pool that holds
    /// none — a just-started host whose priority addresses connected first — has no reference at
    /// all. Refusing to evict is the fail-safe direction: `corroboration_readiness` already REFUSES
    /// below its floor, so a pool that keeps a peer it cannot judge downgrades a read, while a pool
    /// that evicts on a bar set by non-voices destroys the very peers it needs.
    #[tokio::test]
    async fn a_pool_whose_only_speakers_are_priority_entries_evicts_nothing() {
        let pool = pool_of(
            5,
            &[
                (PeerOrigin::Priority, CURRENT + 1_000_000),
                (PeerOrigin::Priority, CURRENT - 1_000_000),
                (PeerOrigin::Discovered, 0),
            ],
        )
        .await;

        assert!(
            pool.evict_lagging_peers_for_tests().await.is_empty(),
            "no independent voice has spoken, so there is no bar to evict against"
        );
        assert_eq!(pool.held_addresses_for_tests().await.len(), 3);
    }
}
