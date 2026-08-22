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
use super::plurality::{CORROBORATION_FLOOR, PEER_LIFETIME, PRIORITY_SLOTS};

/// How many dial rounds [`PeerPool::fill_toward_capacity`] may spend reaching capacity.
///
/// DERIVED, not chosen, for the same reason [`default_max_peers`](super::plurality::default_max_peers)
/// is: the priority addresses are tried SEQUENTIALLY and a round admits at most one of them, so
/// [`PRIORITY_SLOTS`] rounds can be consumed before a single dial reaches discovery at all. Two
/// more are then owed — one that reaches discovery with every priority address excluded, and one
/// for the ordinary attrition of a round whose dials did not all land.
///
/// A literal `3` was wrong on exactly the host this rule exists for: an operator with
/// `TRUSTED_FULLNODE` who also runs a node spends round one on the trusted address and round two
/// on the loopback, leaving ONE round for discovery and no slack. If that round admitted fewer
/// than [`CORROBORATION_FLOOR`] independent peers the pool could never arm, and there was no
/// fourth round in which to try. Deriving it means a third priority address widens the budget
/// instead of silently consuming it.
const FILL_ROUNDS: usize = PRIORITY_SLOTS + 2;

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
    /// preferred local node from a discovered one — see [`connect::PeerOrigin`].
    origin: connect::PeerOrigin,
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
    /// failure-driven — a dead connection stays in `entries`, counted as a held peer and
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
    /// have several available — and one peer is a pool that can never corroborate anything.
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
            for _ in 0..wanted {
                let tls = self.tls.clone();
                let held = held.clone();
                let network = self.network;
                let timeout = self.connect_timeout;
                dials.push(async move {
                    connect::connect_random_peer_excluding(network, &tls, timeout, &held).await
                });
            }

            let mut admitted = 0usize;
            while let Some(result) = dials.next().await {
                match result {
                    Ok((peer, addr, receiver, origin)) => {
                        if self.admit_and_follow(peer, addr, receiver, origin).await {
                            admitted += 1;
                        }
                    }
                    Err(e) => log::debug!("peer connect failed: {e}"),
                }
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
        if !self.admit(peer, address, origin, source.session).await {
            return false;
        }
        self.fanout.open_session(source).await;
        self.spawn_receiver_handler(source, receiver);
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

    /// Every peer that could CORROBORATE an answer already given by the peer at `asked`.
    ///
    /// A corroborating peer must be two things at once, and neither alone is enough:
    ///
    /// - **A different address than `asked`.** Asking the same connection twice returns the same
    ///   opinion twice, which reads as agreement while being one voice.
    /// - **[`PeerOrigin::Discovered`](connect::PeerOrigin).** A peer reached from a preferred
    ///   address — an operator's node, or one on this machine — is an excellent peer to READ from
    ///   and is not evidence about the chain independent of this host, exactly as
    ///   [`independent_peer_count`](Self::independent_peer_count) records.
    ///
    /// They are returned ALL AT ONCE, and there is deliberately no singular form of this. Asking
    /// corroborators one at a time lets the first responder settle a claim about the chain, which
    /// is exactly the power a hostile peer has (dig_ecosystem#2462) — and a single corroborator
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
    /// preferred address — an operator's trusted node or one on this machine — which are excellent
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
    /// The count is of the peers that WILL be asked to corroborate — precisely the set
    /// [`select_corroborating_peers`](Self::select_corroborating_peers) returns for the same
    /// address, because both use [`is_corroborator`](Self::is_corroborator) to decide the set.
    ///
    /// **It takes the answering address rather than subtracting one blindly.** An earlier version
    /// charged the asker's slot against the independent set whatever the asker was, so a read from
    /// the operator's own node — which is not in that set at all — silently spent an independent
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
    /// request FAILURE and cannot substitute for this — a captured peer does not fail.
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

    /// One maintenance pass: rotate out an over-age peer, then refill toward capacity.
    ///
    /// Cycling before refilling is deliberate. Refilling first would find the pool at capacity and
    /// do nothing, so the rotation would leave a permanently smaller pool.
    pub async fn maintain(&self) {
        self.eject_dead_sessions().await;
        self.cycle_expired_peers().await;
        self.try_refill().await;
    }

    /// Remove the peers whose sessions have ENDED.
    ///
    /// Session death is the pool's third eviction reason and it is neither of the other two: a
    /// disconnected or protocol-violating peer has not failed a request, so
    /// [`eject_peer`](Self::eject_peer) never fires for it, and it need not be old, so cycling may
    /// be minutes away. Until it is removed the pool counts it as held and `select_peer` keeps
    /// offering it — a peer count that overstates what the pool can actually reach.
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
    /// is already held — a pool of N connections to one address reports itself healthy while being
    /// a single point of both failure and deceit (dig_ecosystem#2648).
    ///
    /// **Both checks are made while HOLDING the write lock, and that placement is the whole
    /// correctness of this.** Dials run concurrently, so any check made before acquiring the lock —
    /// under the read lock, or by the caller — is a time-of-check/time-of-use gap: two fills of the
    /// same address each observe it absent, then each pushes, and the duplicate is admitted by
    /// exactly the code written to prevent it. The check and the push must be one critical section.
    async fn admit(
        &self,
        peer: Peer,
        address: SocketAddr,
        origin: connect::PeerOrigin,
        session: SessionId,
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
    /// can tell one held peer's frames from another's — which is what lets it follow the peer it
    /// chose and eject one whose frames it rejected.
    ///
    /// **`pub(crate)`, so the one-path claim on [`admit_and_follow`](Self::admit_and_follow) holds
    /// across the crate boundary too.** A caller outside the crate could otherwise supply its own
    /// [`FrameSource`] and publish frames under a session the pool never allocated — attribution
    /// that is unforgeable in-crate becomes forgeable the moment the constructor is exported.
    ///
    /// **The task never ends quietly.** Both ways a session can stop — the transport closing, and
    /// a message this crate cannot decode — publish a [`PoolFrame::SessionEnded`] and record the
    /// death for [`eject_dead_sessions`](Self::eject_dead_sessions). Returning silently would
    /// leave a subscriber unable to distinguish a peer that stopped from a chain that is quiet,
    /// and would leave the pool holding a connection nothing will ever read from.
    pub(crate) fn spawn_receiver_handler(
        &self,
        source: FrameSource,
        mut receiver: mpsc::Receiver<Message>,
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
                        fanout
                            .publish(
                                source,
                                PoolFrame::CoinStates {
                                    height: update.height,
                                    fork_height: update.fork_height,
                                    items: update.items,
                                },
                            )
                            .await;
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
    /// Falling further behind than `capacity` ENDS the subscription — see
    /// [`FrameSubscription`](super::frames::FrameSubscription) for why a gap is not an option.
    pub async fn subscribe_frames(&self, capacity: usize) -> FrameSubscription {
        self.fanout.subscribe(capacity).await
    }
}

/// Construction and admission reachable from OTHER modules' tests.
///
/// [`PeerPool::new`] dials the network, so a test of anything built ON the pool — the backend's
/// absence corroboration, for one — cannot use it. These wrap the private internals rather than
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
        self.admit(peer, address, origin, session).await
    }

    /// Admit a connection AND follow `receiver`, exactly as a real dial would.
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
    use crate::peer::test_support::{address, loopback_peer};

    use super::PeerPool as _Pool;
    fn empty_pool(max_peers: usize) -> PeerPool {
        _Pool::for_tests(max_peers)
    }

    /// **The defect, and the one shape that separates a locked re-check from a TOCTOU dedupe.**
    ///
    /// Eight fills of the SAME address are admitted CONCURRENTLY, which is how the pool fills in
    /// production: `PeerPool::new` races `max_peers` dials with no knowledge of each other, and each
    /// may return the same priority address. A dedupe that reads the entry list before taking the
    /// write lock passes a sequential test and fails this one — every task observes the address
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
    /// Without this, an `admit` that rejected everything after the first — or that lost racing
    /// pushes — would satisfy the distinctness test while breaking the pool.
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

    /// An ejected address is admissible again — distinctness must not become a permanent ban.
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
    /// empty offline — an exact, network-free fixture for the empty-pool branch.
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
    /// while the pool provably holds nothing — the one shape that separates a measurement from a
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
    /// ago, one moments ago, and BOTH are healthy — no request is made, so `eject_peer`, the
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
    /// Without it, a cycler that ejected the oldest entry unconditionally — no lifetime check at
    /// all — would satisfy the test above while churning the pool on every pass.
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
    /// A pool filled to the SHIPPED default — one priority entry, which is the ordinary case
    /// because the dialler tries the loopback first, and discovered peers for the rest — must
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
    /// Two discovered peers means exactly one corroborator once the answering peer is set aside —
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
    /// Pins the bound from the other side — a gate that refused everything would satisfy the test
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
    /// Same peer COUNT as the arming control above, different origins — the one fixture shape that
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
    /// worth on its own, and the answer comes from a preferred peer that is not in that set — so
    /// charging the asker's slot against it, as a blind `- 1` does, reports a pool with two
    /// genuine corroborators as having one.
    ///
    /// That is not a missed opportunity, it is a downgrade with a destination: the answer becomes
    /// `Uncorroborated*` and the router settles it against the centralized coinset tier
    /// (`router.rs`), substituting one HTTPS source for the untrusted plurality NC-12 asks for. On
    /// a host with `TRUSTED_FULLNODE` or a co-resident node — the configuration this pool is sized
    /// for — that is the ordinary path, not an edge case.
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

        assert!(pool.admitted(peer.clone(), asked, PeerOrigin::Discovered).await);
        assert!(pool.admitted(peer.clone(), priority, PeerOrigin::Priority).await);
        assert!(pool.admitted(peer.clone(), discovered_1, PeerOrigin::Discovered).await);
        assert!(pool.admitted(peer.clone(), discovered_2, PeerOrigin::Discovered).await);

        // Both should see exactly 2 corroborators: discovered_1 and discovered_2.
        // Not `asked` (excluded by address), not `priority` (excluded by origin).
        let readiness = pool.corroboration_readiness(asked).await;
        let selected = pool.select_corroborating_peers(asked).await;

        let readiness_count = match readiness {
            CorroborationReadiness::Armed { corroborators } => corroborators,
            CorroborationReadiness::Insufficient { corroborators, .. } => corroborators,
        };

        assert_eq!(
            readiness_count, selected.len(),
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

    use chia_protocol::Bytes32;

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
    /// `Bytes32 + u32 + u128 + u32` the body requires — which is what any peer can send at will,
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
    /// expected number of frames has arrived or the budget runs out, and returns whatever it has —
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
    /// without attribution — or that attributed every frame to one session — gives both frames the
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
    /// decode — the `if let Ok(..)` this replaces — delivers BOTH peaks and no ending, which is a
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
            "a dead session is still HELD until maintenance runs — which is the gap being closed"
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
    /// death that is still recorded against it. An ejection matching on address alone — the
    /// obvious implementation — removes the live replacement there and leaves the pool short, with
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
}
