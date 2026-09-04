pub mod block;
pub mod connect;
pub mod frames;
pub mod light_client;
pub mod ordering;
pub mod plurality;
pub mod pool;
pub mod set_agreement;
pub mod translate;

#[cfg(test)]
mod corroboration_tests;
#[cfg(test)]
mod set_corroboration_tests;
#[cfg(test)]
pub(crate) mod test_support;

use std::net::SocketAddr;
use std::time::Duration;

use chia_consensus::consensus_constants::ConsensusConstants;
use chia_protocol::{
    Bytes32, CoinStateFilters, FullBlock as ProtoFullBlock, RejectAdditionsRequest, RejectBlock,
    RejectHeaderRequest, RejectRemovalsRequest, RequestAdditions, RequestBlock, RequestBlockHeader,
    RequestFeeEstimates, RequestRemovals, RespondAdditions, RespondBlock, RespondBlockHeader,
    RespondFeeEstimates, RespondRemovals, SpendBundle as ProtoBundle,
};
use chia_wallet_sdk::client::Peer;
use chia_wallet_sdk::types::{MAINNET_CONSTANTS, TESTNET11_CONSTANTS};
use tokio_tungstenite::Connector;

use crate::types::*;

use crate::NetworkType;
pub use light_client::{ChiaLightClient, LightClientProvider, SubmitOutcome};
use plurality::CORROBORATION_FLOOR;
pub use pool::PeerRequirement;
use pool::{CorroborationReadiness, PeerPool};
use set_agreement::{
    as_of_is_supported, common_height, contradiction, fingerprint, normalise_at, project, SetMember,
};
pub use set_agreement::{CorroboratedSet, HeightedSet, SetAnswer, SetProjection};

// ---------------------------------------------------------------------------
// OptAnswer
// ---------------------------------------------------------------------------

/// What the peer tier was able to establish about a thing that may or may not exist.
///
/// [`Option`] cannot express this, and that is precisely how dig_ecosystem#2456 stayed invisible:
/// `None` was read as *"the chain does not have this"* while it meant *"one anonymous peer sent an
/// empty list"*. The two are different facts and a caller needs to tell them apart, so the peer
/// tier reports which one it has and lets the router decide what to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptAnswer<T> {
    /// The thing exists, here it is, and an independent peer agreed on what it says about the
    /// chain.
    ///
    /// A record is checkable against its own fields only as far as its IDENTITY: a coin id is
    /// `SHA256(parent_coin_info ‖ puzzle_hash ‖ amount)` and covers nothing else. `created_height`
    /// and `spent_height` — the entire reason such a read is made — are copied from what the peer
    /// sent, so a positive answer is corroborated exactly like an absence is
    /// (dig_ecosystem#2462).
    Found(T),
    /// The thing exists on ONE peer's word, and no independent peer said the same.
    ///
    /// The record is still carried, because it is a real answer and the router may yet find a
    /// second voice for it; what it is not is evidence about the chain. A consumer handed this
    /// directly MUST NOT record a height from it.
    UncorroboratedFound(T),
    /// Two independent peers, at different addresses, both report it absent.
    CorroboratedAbsent,
    /// One peer reports it absent and no second independent peer could say so too.
    ///
    /// The peer tier has no basis to call this absence. Whether it can become one depends on
    /// sources the peer tier does not own, so it hands the undecided fact up rather than deciding
    /// it (see [`QueryRouter`](crate::router::QueryRouter), which may corroborate against coinset).
    UncorroboratedAbsent,
}

/// Whether a round of `agreed` agreeing answers, taken from a pool in state `readiness`, may be
/// reported as CORROBORATED.
///
/// Both halves are required and neither implies the other:
///
/// - **The pool must have been [`Armed`](CorroborationReadiness::Armed)** — it held at least
///   [`CORROBORATION_FLOOR`] independent peers besides the one that answered. This is a fact about
///   the pool's membership, checked before the round, so an answer can never be reported as
///   corroborated by a pool that never had the voices to corroborate it.
/// - **At least [`CORROBORATION_FLOOR`] peers must have AGREED** — a fact about the round itself.
///   A pool that held enough peers and then had them time out has corroborated nothing, and
///   membership alone cannot see that.
///
/// The membership half is not implied by the agreement half even though readiness now counts
/// exactly the peers that will be asked. Readiness is a SNAPSHOT taken before the round, but on a
/// shared [`QueryRouter`] multiple concurrent callers may each call [`PeerPool::maintain`] during
/// the same question, so a round can be answered by more peers than the readiness snapshot held.
/// Requiring the pool to have been armed BEFORE the round is what stops a pool that could not have
/// corroborated anything from being rescued by a peer that arrived mid-question.
///
/// The floor is on agreement, never on peers asked: reading "I asked and heard no contradiction"
/// as agreement is how silence becomes a second opinion.
fn corroborated(readiness: CorroborationReadiness, agreed: usize) -> bool {
    matches!(readiness, CorroborationReadiness::Armed { .. }) && agreed >= CORROBORATION_FLOOR
}

// ---------------------------------------------------------------------------
// PeerBackend
// ---------------------------------------------------------------------------

/// A backend over an EMPTY pool, dialling nothing.
///
/// Exists so a test of something built on the backend — the router's settlement of a graded
/// answer, which needs a `QueryRouter` and therefore a `PeerBackend` — can be written without a
/// network. Every read through it fails for want of a peer, which is correct: such a test must
/// supply the answer it is settling, never obtain one.
#[cfg(test)]
impl PeerBackend {
    pub(crate) fn for_tests() -> Self {
        Self::for_tests_with_capacity(0)
    }

    /// A backend over an empty pool that will ADMIT up to `max_peers`, so a test can populate it
    /// with loopback peers at chosen addresses and origins.
    pub(crate) fn for_tests_with_capacity(max_peers: usize) -> Self {
        Self {
            pool: pool::PeerPool::for_tests(max_peers),
            network: NetworkType::Mainnet,
            request_timeout: Duration::from_millis(1),
        }
    }

    /// The pool underneath, so a test can admit peers into the backend it is exercising.
    pub(crate) fn pool_for_tests(&self) -> &pool::PeerPool {
        &self.pool
    }
}

pub struct PeerBackend {
    pool: PeerPool,
    network: NetworkType,
    request_timeout: Duration,
}

impl PeerBackend {
    pub async fn new(
        network: crate::NetworkType,
        tls: Connector,
        max_peers: usize,
        requirement: PeerRequirement,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, ChiaQueryError> {
        let pool = PeerPool::new(network, tls, max_peers, requirement, connect_timeout).await?;
        Ok(Self {
            pool,
            network,
            request_timeout,
        })
    }

    /// Get the consensus constants for the configured network.
    pub fn constants(&self) -> &ConsensusConstants {
        match self.network {
            NetworkType::Mainnet => &MAINNET_CONSTANTS,
            NetworkType::Testnet11 => &TESTNET11_CONSTANTS,
        }
    }

    /// Genesis challenge for the configured network.  Used as the header_hash
    /// when querying coin state from height 0 (required by the peer protocol
    /// -- `Bytes32::default()` causes rejection).
    fn genesis_challenge(&self) -> Bytes32 {
        self.constants().genesis_challenge
    }

    pub async fn has_peers(&self) -> bool {
        self.pool.has_peers().await
    }

    /// How many peers this backend HOLDS right now — see [`PeerPool::peer_count`].
    pub async fn peer_count(&self) -> usize {
        self.pool.peer_count().await
    }

    /// How many held peers are INDEPENDENT opinions — see [`PeerPool::independent_peer_count`].
    pub async fn independent_peer_count(&self) -> usize {
        self.pool.independent_peer_count().await
    }

    /// Whether a corroborated read of an answer given by `asked` may be attempted at all — see
    /// [`PeerPool::corroboration_readiness`]. It REFUSES rather than degrading.
    pub async fn corroboration_readiness(&self, asked: SocketAddr) -> pool::CorroborationReadiness {
        self.pool.corroboration_readiness(asked).await
    }

    /// Subscribe to the frames arriving on this backend's pooled sessions.
    ///
    /// Falling further behind than `capacity` ENDS the subscription rather than skipping a frame —
    /// see [`frames::FrameSubscription`].
    ///
    /// The subscription follows the session held at `address` and receives only that session's
    /// frames, so no other held peer can end it (chia-query#34). `None` when no live session is
    /// held there — see [`PeerPool::subscribe_frames`](pool::PeerPool::subscribe_frames).
    pub async fn subscribe_frames(
        &self,
        address: SocketAddr,
        capacity: usize,
    ) -> Option<frames::FrameSubscription> {
        self.pool.subscribe_frames(address, capacity).await
    }

    // -----------------------------------------------------------------------
    // Select a peer (round-robin) then attempt to refill if pool is short.
    // -----------------------------------------------------------------------

    async fn pick(&self) -> Result<(Peer, SocketAddr), ChiaQueryError> {
        // One maintenance pass per request: rotate out a peer that has outlived
        // [`plurality::PEER_LIFETIME`], then refill if under capacity.
        //
        // Driving cycling from the request path rather than a timer task is deliberate — it is the
        // only place the pool is reliably reached, and a cycling policy that nothing calls is
        // indistinguishable from no cycling at all (NC-12).
        self.pool.maintain().await;

        self.pool
            .select_peer()
            .await
            .ok_or_else(|| ChiaQueryError::PeerConnection("no peers available".into()))
    }

    // =======================================================================
    // Public try_* methods -- each selects a peer, makes the request, and
    // ejects the peer on failure.
    // =======================================================================

    pub async fn try_get_coin_record_by_name(
        &self,
        name: &str,
    ) -> Result<CoinRecord, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_coin_record_by_name(&peer, name).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    /// Absence-aware sibling of [`try_get_coin_record_by_name`](Self::try_get_coin_record_by_name).
    ///
    /// A rejected/timed-out request is a failure -> `Err`. An EMPTY coin-state list from a
    /// successful `RespondCoinState` is one peer's WORD that the coin does not exist, which is not
    /// the same thing as it not existing, so the answer is graded by [`OptAnswer`] rather than
    /// flattened into `Ok(None)` -- see
    /// [`read_opt_corroborated`](Self::read_opt_corroborated) (SPEC §3).
    pub async fn try_get_coin_record_by_name_opt(
        &self,
        name: &str,
    ) -> Result<OptAnswer<CoinRecord>, ChiaQueryError> {
        self.read_opt_corroborated(|peer| async move {
            self.do_get_coin_record_by_name_opt(&peer, name).await
        })
        .await
    }

    /// Read something that may be absent, and CORROBORATE whichever answer comes back.
    ///
    /// `read` is run against one selected peer, and that peer's answer is then put to independent
    /// peers — peers at DIFFERENT addresses that were
    /// [discovered](pool::PeerPool::select_corroborating_peers) rather than preferred. Neither
    /// direction is taken on one peer's word:
    ///
    /// Both directions are graded against [`CORROBORATION_FLOOR`], and a contradiction outranks
    /// any amount of agreement:
    ///
    /// - **Present** — the record's chain claim (see [`ChainClaim`]) is put to every independent
    ///   peer at once. [`CORROBORATION_FLOOR`] peers agreeing makes it
    ///   [`Found`](OptAnswer::Found); one that says anything else — a different height, or nothing
    ///   at all — fails the read with [`SourcesDisagree`](ChiaQueryError::SourcesDisagree); too few
    ///   agreeing voices leaves it [`UncorroboratedFound`](OptAnswer::UncorroboratedFound).
    /// - **Absent** — every independent peer is asked the same question. [`CORROBORATION_FLOOR`]
    ///   agreeing absences make it [`CorroboratedAbsent`](OptAnswer::CorroboratedAbsent); a peer
    ///   that produces the thing is [`SourcesDisagree`](ChiaQueryError::SourcesDisagree); too few
    ///   agreeing voices leaves it [`UncorroboratedAbsent`](OptAnswer::UncorroboratedAbsent).
    ///
    /// **One agreeing peer is not corroboration.** An `Uncorroborated*` answer is not a failure —
    /// it is the honest report that the peer tier could not establish the fact, and the router
    /// settles it against another tier or surfaces it as
    /// [`UncorroboratedPresence`](ChiaQueryError::UncorroboratedPresence). What it must never do
    /// is report a one-voice answer as corroborated.
    ///
    /// **Why presence asks everyone and absence asks one.** A hostile peer that answers an absence
    /// wrongly is refuted by any honest peer, so a single corroborator is a sufficient confidence
    /// floor. A hostile peer that answers a PRESENCE wrongly is claiming a height, and letting the
    /// first responder settle that would let whichever peer is fastest decide whether money is
    /// treated as confirmed — so every independent peer is queried CONCURRENTLY and any
    /// contradiction beats any agreement (NC-12, dig_ecosystem#2462). The cost of that is bounded:
    /// a hostile corroborator can make a read fail, which the caller retries, but it can never
    /// make a read return a fact.
    ///
    /// Neither direction is an N-of-N barrier — a peer that fails to answer is ejected and does
    /// not hold the read up — because requiring the whole pool would let one dead peer stall every
    /// query.
    async fn read_opt_corroborated<T, F, Fut>(
        &self,
        read: F,
    ) -> Result<OptAnswer<T>, ChiaQueryError>
    where
        T: ChainClaim,
        F: Fn(Peer) -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    {
        let (peer, addr) = self.pick().await?;
        let first = match read(peer).await {
            Ok(v) => v,
            Err(e) => {
                self.pool.eject_peer(addr).await;
                return Err(e);
            }
        };

        match first {
            Some(found) => self.corroborate_presence(found, addr, &read).await,
            None => self.corroborate_absence(addr, &read).await,
        }
    }

    /// Put a positive answer to every independent peer at once, and grade the agreement.
    async fn corroborate_presence<T, F, Fut>(
        &self,
        found: T,
        addr: SocketAddr,
        read: &F,
    ) -> Result<OptAnswer<T>, ChiaQueryError>
    where
        T: ChainClaim,
        F: Fn(Peer) -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    {
        // Read the pool's arming BEFORE the round, and treat it as a ceiling on the verdict
        // rather than a gate on asking. Asking cannot make an answer worse — a contradiction is
        // decisive however few peers are held — but reporting corroboration from a pool that never
        // held enough independent voices is exactly the degradation the floor exists to prevent.
        let readiness = self.pool.corroboration_readiness(addr).await;

        let corroborators = self.pool.select_corroborating_peers(addr).await;
        if corroborators.is_empty() {
            log::debug!("presence reported by {addr} has no independent corroborator available");
            return Ok(OptAnswer::UncorroboratedFound(found));
        }

        // Every corroborator is asked concurrently and EVERY answer is collected before anything
        // is decided. Grading as the answers arrive would hand the outcome to whichever peer is
        // fastest, which is the property a hostile peer controls.
        let answers =
            futures_util::future::join_all(corroborators.into_iter().map(|(peer, peer_addr)| {
                let answer = read(peer);
                async move { (peer_addr, answer.await) }
            }))
            .await;

        let claim = found.chain_claim();
        let mut agreed = 0usize;
        let mut disagreement: Option<String> = None;
        let mut failed: Vec<SocketAddr> = Vec::new();

        for (peer_addr, answer) in answers {
            match answer {
                Ok(Some(other)) if other.chain_claim() == claim => agreed += 1,
                Ok(Some(other)) => {
                    disagreement.get_or_insert_with(|| {
                        format!(
                            "peer {addr} claims `{claim}`, peer {peer_addr} claims `{}`",
                            other.chain_claim()
                        )
                    });
                }
                Ok(None) => {
                    disagreement.get_or_insert_with(|| {
                        format!("peer {addr} reports present, peer {peer_addr} reports absent")
                    });
                }
                Err(e) => {
                    log::debug!("corroborator {peer_addr} failed: {e}");
                    failed.push(peer_addr);
                }
            }
        }

        // Ejection happens whatever the verdict: a peer that failed a read is ejected everywhere
        // else in this backend, and a disagreement is not a reason to keep a broken connection.
        for peer_addr in failed {
            self.pool.eject_peer(peer_addr).await;
        }

        // A contradiction outranks any amount of agreement. Nothing in the answers says which set
        // to believe, so counting votes would invent a fact — and would let an attacker holding
        // two pool slots manufacture one.
        if let Some(detail) = disagreement {
            return Err(ChiaQueryError::SourcesDisagree(detail));
        }

        if !corroborated(readiness, agreed) {
            log::debug!(
                "presence reported by {addr} drew {agreed} agreeing voices with the pool \
                 {readiness:?}; below the floor of {CORROBORATION_FLOOR}"
            );
            return Ok(OptAnswer::UncorroboratedFound(found));
        }
        Ok(OptAnswer::Found(found))
    }

    /// Put an absence to EVERY independent peer at once, and grade the agreement.
    ///
    /// Asking all of them together, rather than one at a time, is the same requirement presence
    /// has: a single corroborator lets whichever peer is asked settle a claim about the chain, and
    /// which peer that is, is not a property the reader controls.
    async fn corroborate_absence<T, F, Fut>(
        &self,
        addr: SocketAddr,
        read: &F,
    ) -> Result<OptAnswer<T>, ChiaQueryError>
    where
        T: ChainClaim,
        F: Fn(Peer) -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    {
        let readiness = self.pool.corroboration_readiness(addr).await;

        let corroborators = self.pool.select_corroborating_peers(addr).await;
        if corroborators.is_empty() {
            log::debug!("absence reported by {addr} has no independent corroborator available");
            return Ok(OptAnswer::UncorroboratedAbsent);
        }

        let answers =
            futures_util::future::join_all(corroborators.into_iter().map(|(peer, peer_addr)| {
                let answer = read(peer);
                async move { (peer_addr, answer.await) }
            }))
            .await;

        let mut agreed = 0usize;
        let mut disagreement: Option<String> = None;
        let mut failed: Vec<SocketAddr> = Vec::new();

        for (peer_addr, answer) in answers {
            match answer {
                Ok(None) => agreed += 1,
                Ok(Some(_)) => {
                    disagreement.get_or_insert_with(|| {
                        format!("peer {addr} reports absent, peer {peer_addr} reports present")
                    });
                }
                Err(e) => {
                    log::debug!("corroborator {peer_addr} failed: {e}");
                    failed.push(peer_addr);
                }
            }
        }

        // A peer that fails a read is ejected everywhere else in this backend, whatever the
        // verdict turns out to be.
        for peer_addr in failed {
            self.pool.eject_peer(peer_addr).await;
        }

        if let Some(detail) = disagreement {
            return Err(ChiaQueryError::SourcesDisagree(detail));
        }

        if !corroborated(readiness, agreed) {
            log::debug!(
                "absence reported by {addr} drew {agreed} agreeing voices with the pool \
                 {readiness:?}; below the floor of {CORROBORATION_FLOOR}"
            );
            return Ok(OptAnswer::UncorroboratedAbsent);
        }
        Ok(OptAnswer::CorroboratedAbsent)
    }

    /// Read a POPULATION — a set that may legitimately differ between two honest sources — and
    /// grade it by height-normalised set equality.
    ///
    /// The set counterpart of [`read_opt_corroborated`](Self::read_opt_corroborated), and it uses
    /// the same plurality primitives for the same reasons: readiness is read BEFORE the round,
    /// every corroborator is drawn by
    /// [`select_corroborating_peers`](pool::PeerPool::select_corroborating_peers) so there is no
    /// second notion of "independent voice", every one of them is asked CONCURRENTLY, and one
    /// contradiction outranks any amount of agreement. What differs is only what "agreement"
    /// means — see [`set_agreement`] for the rule and for what each wrong version of it costs.
    ///
    /// `read` MUST return the source's RAW answer: every record in range, spent ones included, no
    /// client-side filtering. Filtering inside `read` throws away the information normalisation
    /// needs, and a filter applied per source before comparison turns two honest truncations into
    /// a false [`SourcesDisagree`](ChiaQueryError::SourcesDisagree). The caller's `projection` is
    /// applied to the agreed set, AFTER the verdict (chia-query#33).
    ///
    /// Three outcomes, and the middle one is the point:
    ///
    /// - **[`Corroborated`](SetAnswer::Corroborated)** — the pool was
    ///   [`Armed`](CorroborationReadiness::Armed) and at least [`CORROBORATION_FLOOR`] independent
    ///   peers returned the identical normalised set.
    /// - **[`Uncorroborated`](SetAnswer::Uncorroborated)** — nobody contradicted it and too few
    ///   agreed. Not a failure and not evidence; the router settles it against another tier.
    /// - **`Err(SourcesDisagree)`** — some source returned a different set at the same height.
    ///   Reported, never resolved: nothing in two contradictory sets says which to believe, and
    ///   picking the larger or the smaller is exactly the lever chia-query#47 describes.
    ///
    /// Silence is not agreement. A corroborator whose read FAILS is ejected and does not count
    /// towards the floor, so a pool that has been quietly reduced to one voice reports
    /// `Uncorroborated` rather than corroborating against itself.
    async fn read_set_corroborated<T, F, Fut>(
        &self,
        read: F,
        projection: SetProjection,
    ) -> Result<SetAnswer<T>, ChiaQueryError>
    where
        T: SetMember,
        F: Fn(Peer) -> Fut,
        Fut: std::future::Future<Output = Result<HeightedSet<T>, ChiaQueryError>>,
    {
        let (peer, addr) = self.pick().await?;

        // Read arming BEFORE the round, exactly as the scalar path does: it is a ceiling on the
        // verdict, not a gate on asking, and a pool that never held enough voices must not be
        // rescued by one that arrived mid-question.
        let readiness = self.pool.corroboration_readiness(addr).await;

        let first = match read(peer).await {
            Ok(v) => v,
            Err(e) => {
                self.pool.eject_peer(addr).await;
                return Err(e);
            }
        };

        let corroborators = self.pool.select_corroborating_peers(addr).await;
        let answers = if corroborators.is_empty() {
            log::debug!("set reported by {addr} has no independent corroborator available");
            Vec::new()
        } else {
            futures_util::future::join_all(corroborators.into_iter().map(|(peer, peer_addr)| {
                let answer = read(peer);
                async move { (peer_addr, answer.await) }
            }))
            .await
        };

        let mut others: Vec<(SocketAddr, HeightedSet<T>)> = Vec::new();
        let mut failed: Vec<SocketAddr> = Vec::new();
        for (peer_addr, answer) in answers {
            match answer {
                Ok(set) => others.push((peer_addr, set)),
                Err(e) => {
                    log::debug!("set corroborator {peer_addr} failed: {e}");
                    failed.push(peer_addr);
                }
            }
        }
        for peer_addr in failed {
            self.pool.eject_peer(peer_addr).await;
        }

        // The common height is taken over the sources that ANSWERED — the asked peer plus every
        // corroborator that produced a set. A source that failed contributes no as-of height,
        // because a height from a source that gave no set is a height nothing was held to.
        let mut as_of: Vec<u32> = Vec::with_capacity(others.len() + 1);
        as_of.push(first.as_of_height);
        as_of.extend(others.iter().map(|(_, set)| set.as_of_height));

        let Some(height) = common_height(&as_of, projection.end_height) else {
            // Only reachable on a chain shorter than SETTLED_LAG. Refusing is the fail-closed
            // direction and the only honest one: there is no settled height to answer about, and
            // an empty set at height 0 would read as a corroborated emptiness.
            return Err(ChiaQueryError::UncorroboratedPresence(format!(
                "no settled common height exists for the sources that answered ({as_of:?})"
            )));
        };

        let normalised = normalise_at(&first.items, height);
        let claim = fingerprint(&normalised);

        let mut agreed = 0usize;
        let mut disagreement: Option<String> = None;
        for (peer_addr, set) in &others {
            let other = fingerprint(&normalise_at(&set.items, height));
            match contradiction(
                &format!("peer {addr}"),
                &claim,
                &format!("peer {peer_addr}"),
                &other,
            ) {
                None => agreed += 1,
                Some(detail) => {
                    disagreement.get_or_insert(format!("at height {height}: {detail}"));
                }
            }
        }

        if let Some(detail) = disagreement {
            return Err(ChiaQueryError::SourcesDisagree(detail));
        }

        let items = project(normalised, projection);

        // A caller asking about a window that has not settled yet cannot be answered from settled
        // state, and an empty set is the WRONG way to say so — it is indistinguishable from "there
        // is nothing there". The answer is downgraded so the router looks for another voice.
        let window_has_settled = projection.start_height.is_none_or(|start| start <= height);
        if !window_has_settled {
            log::debug!(
                "the caller's start_height is above the settled common height {height}; nothing \
                 settled can be said about that window yet"
            );
            return Ok(SetAnswer::Uncorroborated {
                items,
                as_of_height: height,
            });
        }

        if !corroborated(readiness, agreed) {
            log::debug!(
                "set reported by {addr} drew {agreed} agreeing voices with the pool \
                 {readiness:?}; below the floor of {CORROBORATION_FLOOR}"
            );
            return Ok(SetAnswer::Uncorroborated {
                items,
                as_of_height: height,
            });
        }

        Ok(SetAnswer::Corroborated {
            items,
            as_of_height: height,
        })
    }

    /// The as-of height for a source whose wire response carries none of its own.
    ///
    /// `RespondPuzzleState` states the block its answer is a snapshot of; `RespondCoinState` and
    /// `RespondChildren` do not. For those the peer's own announced peak is the anchor — see
    /// [`PeerPool::announced_peak`](pool::PeerPool::announced_peak) for why it is sound in both
    /// directions and why a peer that has announced nothing is REFUSED rather than treated as
    /// height zero.
    ///
    /// The refusal ejects the peer and the pool refills. That is deliberate: a pooled session that
    /// has never announced a peak is not a witness this crate can hold to a height, and continuing
    /// without one would mean normalising against a number nobody claimed.
    async fn peer_as_of(&self, peer: &Peer) -> Result<u32, ChiaQueryError> {
        self.pool
            .announced_peak(peer.socket_addr())
            .await
            .ok_or_else(|| {
                ChiaQueryError::PeerConnection(
                    "peer has announced no peak, so its answer cannot be held to a height".into(),
                )
            })
    }

    /// Absence-aware read of the spend that spent `coin_id`.
    ///
    /// A coin that is unknown (no coin-state) and one that is unspent (no spent height) are both
    /// "there is no such spend", and both rest on a peer's word alone, so both are graded by
    /// [`OptAnswer`] through [`read_opt_corroborated`](Self::read_opt_corroborated). `Err` only
    /// when the peer read itself fails.
    pub async fn try_get_coin_spend_opt(
        &self,
        coin_id: &str,
    ) -> Result<OptAnswer<CoinSpend>, ChiaQueryError> {
        self.read_opt_corroborated(|peer| async move {
            self.do_get_coin_spend_opt(&peer, coin_id).await
        })
        .await
    }

    /// Every coin at `puzzle_hash`, graded by height-normalised set agreement.
    ///
    /// **This is the read a wallet balance and the collateral census are taken through**, and it
    /// was single-peer and ungraded until chia-query#35/#47: whatever the first responsive source
    /// said was the answer. A source that OMITS a coin makes the network look smaller than it is,
    /// and omission is free while addition costs on-chain collateral — so the ungraded read was
    /// asymmetrically exploitable in exactly the direction that costs the network money.
    ///
    /// The wire request asks for spent coins whatever `include_spent` says, and the caller's
    /// filters are applied to the AGREED set; see [`set_agreement`] for why either order matters.
    pub async fn try_get_coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        self.read_set_corroborated(
            |peer| async move {
                self.do_puzzle_hash_query(&peer, &[puzzle_hash], false)
                    .await
            },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    /// Every coin at any of `puzzle_hashes`, graded by height-normalised set agreement.
    ///
    /// One round over the whole batch rather than one per hash: the peer protocol takes the batch,
    /// so splitting it would multiply the dials without adding a voice.
    pub async fn try_get_coin_records_by_puzzle_hashes(
        &self,
        puzzle_hashes: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        let hashes: Vec<&str> = puzzle_hashes.iter().map(String::as_str).collect();
        self.read_set_corroborated(
            |peer| {
                let hashes = hashes.clone();
                async move { self.do_puzzle_hash_query(&peer, &hashes, false).await }
            },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    /// Every coin hinted at `hint`, graded by height-normalised set agreement.
    pub async fn try_get_coin_records_by_hint(
        &self,
        hint: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        self.read_set_corroborated(
            |peer| async move { self.do_puzzle_hash_query(&peer, &[hint], true).await },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    /// Every coin hinted at any of `hints`, graded by height-normalised set agreement.
    pub async fn try_get_coin_records_by_hints(
        &self,
        hints: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        let hs: Vec<&str> = hints.iter().map(String::as_str).collect();
        self.read_set_corroborated(
            |peer| {
                let hs = hs.clone();
                async move { self.do_puzzle_hash_query(&peer, &hs, true).await }
            },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    /// The coin records for `names`, graded by height-normalised set agreement.
    ///
    /// Graded by the SET rule rather than the scalar one even though the caller named the coins:
    /// a requested id present in one source's answer and missing from another's is a
    /// contradiction, and the scalar path has no way to say that about a batch.
    ///
    /// `RespondCoinState` carries no height of its own, so each source is anchored on its own
    /// announced peak — see [`peer_as_of`](Self::peer_as_of).
    pub async fn try_get_coin_records_by_names(
        &self,
        names: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        self.read_set_corroborated(
            |peer| async move {
                let items = self.do_coin_ids_query(&peer, names).await?;
                let as_of_height = self.peer_as_of(&peer).await?;
                Ok(HeightedSet {
                    items,
                    as_of_height,
                })
            },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    /// The spend that spent `coin_id` at `height`, graded by the scalar corroboration path.
    ///
    /// A `CoinSpend` is a scalar claim about the chain, not a population, so it takes
    /// [`read_opt_corroborated`](Self::read_opt_corroborated) and the existing [`ChainClaim`]
    /// rather than the set rule. It was single-peer and ungraded until chia-query#35 — whichever
    /// peer answered first decided what program had run.
    ///
    /// **Two checks are made locally, before any vote, and neither needs a second peer:**
    ///
    /// - the reply names the coin it is answering for, so a peer cannot answer about a different
    ///   coin ([`do_get_puzzle_and_solution_raw`](Self::do_get_puzzle_and_solution_raw));
    /// - the puzzle reveal must hash to the coin's own puzzle hash
    ///   ([`verify_reveal_against_puzzle_hash`]), which is why
    ///   [`do_get_puzzle_and_solution_opt`](Self::do_get_puzzle_and_solution_opt) pays for a
    ///   coin-state request first.
    ///
    /// **The SOLUTION is not self-verifying, and that is what corroboration is for.** Nothing in
    /// the coin commits to the solution, so a fabricated one parses exactly like the real one and
    /// no local check can tell them apart.
    ///
    /// `Ok(None)` means the peer answered and had no such spend — an ungraded absence until
    /// [`read_opt_corroborated`](Self::read_opt_corroborated) grades it.
    pub async fn try_get_puzzle_and_solution(
        &self,
        coin_id: &str,
        height: u32,
    ) -> Result<OptAnswer<CoinSpend>, ChiaQueryError> {
        self.read_opt_corroborated(|peer| async move {
            self.do_get_puzzle_and_solution_opt(&peer, coin_id, height)
                .await
        })
        .await
    }

    /// A fee estimate.
    ///
    /// **Single-peer by design, and stated here so the silence is not read as an oversight
    /// (chia-query#35).** A fee estimate is one node's ADVICE about a future mempool, not a claim
    /// about chain state: there is no fact for a second peer to agree with, two honest nodes
    /// legitimately differ, and a caller that acts on it pays a fee rather than recording a
    /// falsehood about money. Corroborating it would spend dials to average opinions.
    pub async fn try_get_fee_estimate(
        &self,
        target_times: &[u64],
    ) -> Result<FeeEstimate, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_fee_estimate(&peer, target_times).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    /// Push a signed bundle to the mempool.
    ///
    /// **Single-peer by design, and stated here so the silence is not read as an oversight
    /// (chia-query#35).** This is a WRITE, not a read: there is no existing fact for a second peer
    /// to agree about, and the only thing corroboration could grade is whether other peers also
    /// accepted the bundle — which is a question about propagation, answered by reading the coin
    /// back afterwards through a read that IS graded.
    pub async fn try_push_tx(&self, bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_push_tx(&peer, bundle).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- block record by height (RequestBlockHeader) -------------------------

    /// One block record, on ONE peer's word.
    ///
    /// **Kept single-peer deliberately, and only for the RANGE read**
    /// ([`try_get_block_records`](Self::try_get_block_records)), where grading would run one full
    /// corroboration round per height and turn a thousand-block walk into a thousand rounds.
    /// Every single-height read goes through
    /// [`try_get_block_record_by_height_opt`](Self::try_get_block_record_by_height_opt), which is
    /// graded. A caller reaching for this one is asking for a range and accepting that trade.
    pub async fn try_get_block_record_by_height(
        &self,
        height: u32,
    ) -> Result<BlockRecord, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_block_record_by_height(&peer, height).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    /// One block record, GRADED by the scalar corroboration path.
    ///
    /// A block record is a scalar claim, so it takes
    /// [`read_opt_corroborated`](Self::read_opt_corroborated) with
    /// [`ChainClaim for BlockRecord`](crate::types::BlockRecord) — `(height, header_hash,
    /// timestamp)`.
    ///
    /// **Corroboration rather than verification, and the reason is the field callers read.** A
    /// header block's hash is recomputable from its own contents, so the identity of a record is
    /// checkable locally. `timestamp` is not: it lives in the foliage and cannot be re-derived from
    /// the record alone, and it is precisely what
    /// [`block_timestamp_opt`](crate::router::QueryRouter::block_timestamp_opt) exists to read.
    /// Verifying the hash would authenticate the part nobody was asking about. Hash verification
    /// is a future tightening on top of this, not a reason to stay single-peer.
    ///
    /// `Ok(None)` means the peer REJECTED the header request, which a peer does for a height
    /// beyond its own peak — one peer's word that there is no such block, graded like any other
    /// absence.
    pub async fn try_get_block_record_by_height_opt(
        &self,
        height: u32,
    ) -> Result<OptAnswer<BlockRecord>, ChiaQueryError> {
        self.read_opt_corroborated(|peer| async move {
            self.do_get_block_record_by_height_opt(&peer, height).await
        })
        .await
    }

    // -- additions and removals (RequestAdditions + RequestRemovals) ---------
    // Available for callers who have both height and header_hash.  The
    // coinset.org API only requires header_hash, so the router cannot
    // automatically peer-back this endpoint without a height lookup first.

    #[allow(dead_code)]
    pub async fn try_get_additions_and_removals(
        &self,
        height: u32,
        header_hash: &str,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_get_additions_and_removals(&peer, height, header_hash)
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- children (for parent_id queries) -----------------------------------

    /// The children of `parent_id`, graded by height-normalised set agreement.
    ///
    /// Same wire shape and same asymmetry as the puzzle-hash reads: a source can drop a child by
    /// staying quiet, so the set rule applies here for the same reason it applies there.
    ///
    /// `RespondChildren` carries no height of its own, so each source is anchored on its own
    /// announced peak — see [`peer_as_of`](Self::peer_as_of).
    pub async fn try_get_children(
        &self,
        parent_id: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<SetAnswer<CoinRecord>, ChiaQueryError> {
        self.read_set_corroborated(
            |peer| async move {
                let items = self.do_get_children(&peer, parent_id).await?;
                let as_of_height = self.peer_as_of(&peer).await?;
                Ok(HeightedSet {
                    items,
                    as_of_height,
                })
            },
            SetProjection {
                start_height,
                end_height,
                include_spent,
            },
        )
        .await
    }

    // -- get full block by height (RequestBlock) ------------------------------

    /// A full block by height.
    ///
    /// **Single-peer by design, and stated here so the silence is not read as an oversight
    /// (chia-query#35).** This read and the three below it feed CLVM parsing whose output is bound
    /// to the block's own hashes: a block that does not hash to its header is rejected by the
    /// parse, and the additions and removals are derived from the generator rather than asserted by
    /// the peer. What corroboration adds to a self-checking artifact is a second copy of the same
    /// bytes.
    pub async fn try_get_block_by_height(
        &self,
        height: u32,
    ) -> Result<serde_json::Value, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_block_by_height(&peer, height).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- additions and removals from a full block (CLVM parsing) -------------

    pub async fn try_get_additions_and_removals_from_block(
        &self,
        height: u32,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_get_additions_and_removals_from_block(&peer, height)
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- block spends with puzzle_reveal + solution (CLVM parsing) -----------

    pub async fn try_get_block_spends_by_height(
        &self,
        height: u32,
    ) -> Result<Vec<CoinSpend>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_block_spends(&peer, height).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- block spends WITH parsed conditions --------------------------------

    pub async fn try_get_block_spends_with_conditions(
        &self,
        height: u32,
    ) -> Result<Vec<CoinSpendWithConditions>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let proto_block = self.fetch_full_block(&peer, height).await;
        if proto_block.is_err() {
            self.pool.eject_peer(addr).await;
        }
        let proto_block = proto_block?;
        block::block_spends_with_conditions(&proto_block, self.constants())
    }

    // -- puzzle and solution (resolve height from coin state if needed) ------

    pub async fn try_get_puzzle_and_solution_auto(
        &self,
        coin_id: &str,
    ) -> Result<CoinSpend, ChiaQueryError> {
        // First find the coin's spent_height via request_coin_state.
        let (peer, addr) = self.pick().await?;
        let id = translate::parse_bytes32(coin_id)?;

        let state_resp = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(vec![id], None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state rejected".into()))?;

        let cs = state_resp
            .coin_states
            .first()
            .ok_or_else(|| ChiaQueryError::PeerRejection("coin not found".into()))?;
        let spent_height = cs
            .spent_height
            .ok_or_else(|| ChiaQueryError::PeerRejection("coin is not spent".into()))?;

        let res = self
            .do_get_puzzle_and_solution_raw(&peer, coin_id, spent_height)
            .await
            .and_then(|spend| {
                spend
                    .ok_or_else(|| ChiaQueryError::PeerRejection("puzzle solution rejected".into()))
            })
            .map(|spend| CoinSpend {
                coin: Coin::from_protocol(&cs.coin),
                ..spend
            })
            .and_then(|spend| {
                // The coin state was fetched above, so the reveal is checked against the coin's own
                // puzzle hash instead of being taken on the peer's word.
                verify_reveal_against_puzzle_hash(&spend)?;
                Ok(spend)
            });
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- block records range ------------------------------------------------

    pub async fn try_get_block_records(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Vec<BlockRecord>, ChiaQueryError> {
        let mut records = Vec::with_capacity((end - start) as usize);
        for height in start..end {
            records.push(self.try_get_block_record_by_height(height).await?);
        }
        Ok(records)
    }

    // -- blocks range -------------------------------------------------------

    pub async fn try_get_blocks_range(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Vec<serde_json::Value>, ChiaQueryError> {
        let mut blocks = Vec::with_capacity((end - start) as usize);
        for height in start..end {
            blocks.push(self.try_get_block_by_height(height).await?);
        }
        Ok(blocks)
    }

    // -- network info (hardcoded from chia constants) ------------------------

    pub fn network_info(&self) -> NetworkInfo {
        let c = self.constants();
        NetworkInfo {
            network_name: self.network.network_id().to_string(),
            network_prefix: match self.network {
                NetworkType::Mainnet => "xch".to_string(),
                NetworkType::Testnet11 => "txch".to_string(),
            },
            genesis_challenge: format!("0x{}", hex::encode(c.genesis_challenge)),
        }
    }

    // -- aggsig additional data (from consensus constants) -------------------

    pub fn aggsig_additional_data(&self) -> String {
        format!(
            "0x{}",
            hex::encode(self.constants().agg_sig_me_additional_data)
        )
    }

    // -- peak height (from tracked NewPeakWallet messages) ------------------

    pub async fn peak_height(&self) -> u32 {
        self.pool.peak_height().await
    }

    // =======================================================================
    // Internal implementation helpers
    // =======================================================================

    async fn do_get_coin_record_by_name(
        &self,
        peer: &Peer,
        name: &str,
    ) -> Result<CoinRecord, ChiaQueryError> {
        let coin_id = translate::parse_bytes32(name)?;

        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(vec![coin_id], None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state request rejected".into()))?;

        response
            .coin_states
            .first()
            .map(translate::coin_state_to_record)
            .ok_or_else(|| ChiaQueryError::PeerRejection("coin not found".into()))
    }

    /// Absence-aware coin-record read: a successful response with no coin-state is `Ok(None)`; a
    /// rejected/timed-out request is `Err`.
    async fn do_get_coin_record_by_name_opt(
        &self,
        peer: &Peer,
        name: &str,
    ) -> Result<Option<CoinRecord>, ChiaQueryError> {
        let coin_id = translate::parse_bytes32(name)?;

        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(vec![coin_id], None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state request rejected".into()))?;

        // An empty coin-state list from a SUCCESSFUL response is this peer's word that the coin
        // does not exist. It carries no proof -- a peer a block behind, mid-reorg, pruning, or
        // lying produces the identical bytes -- so it is reported as this ONE peer's answer and
        // corroborated a layer up (dig_ecosystem#2456).
        Ok(response
            .coin_states
            .first()
            .map(translate::coin_state_to_record))
    }

    /// Absence-aware read of the spend that spent `coin_id`: `Ok(None)` when the coin is unknown or
    /// unspent, `Err` when the peer read fails.
    async fn do_get_coin_spend_opt(
        &self,
        peer: &Peer,
        coin_id: &str,
    ) -> Result<Option<CoinSpend>, ChiaQueryError> {
        let id = translate::parse_bytes32(coin_id)?;

        let state_resp = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(vec![id], None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state rejected".into()))?;

        // Unknown coin or unspent coin => there is genuinely no spend => Ok(None).
        let Some(cs) = state_resp.coin_states.first() else {
            return Ok(None);
        };
        let Some(spent_height) = cs.spent_height else {
            return Ok(None);
        };

        // The RAW form, because the coin state is already in hand above — the `_opt` form would
        // fetch it a second time to do exactly what the next few lines do.
        let Some(spend) = self
            .do_get_puzzle_and_solution_raw(peer, coin_id, spent_height)
            .await?
        else {
            return Ok(None);
        };

        // Substitute the GENUINE spent coin from the coin-state lookup for the name-only placeholder
        // that `do_get_puzzle_and_solution_raw` builds (the peer `PuzzleSolutionResponse` omits the
        // coin). The singleton-lineage walk binds each fetched spend to the requested coin id
        // (`spend.coin.coin_id() == current`, chia-query#7); a placeholder coin hashes to the wrong
        // id and fails that binding closed, making peer-sourced lineage resolution impossible. The
        // real coin is already in hand here, so return it and let the binding authenticate the hop.
        let spend = CoinSpend {
            coin: Coin::from_protocol(&cs.coin),
            ..spend
        };

        // The real coin is in hand here, so the reveal is REFUTED locally rather than corroborated:
        // a puzzle whose tree hash is not the coin's puzzle hash cannot be the puzzle that coin was
        // locked with, whatever any number of peers say.
        verify_reveal_against_puzzle_hash(&spend)?;
        Ok(Some(spend))
    }

    /// Every coin state at `hashes`, as ONE source sees it, WITH the height that source says
    /// its answer is a snapshot of.
    ///
    /// Two things it deliberately no longer does, both of which it used to:
    ///
    /// - **It does not throw the anchor away.** `RespondPuzzleState` carries `height` and
    ///   `header_hash`, and the page loop discarded both. That height is the peer's own statement
    ///   of the block its walk finished at, and it is what makes set agreement well defined
    ///   (chia-query#47) — without it, two honest peers one block apart have no common ground to
    ///   be compared on.
    /// - **It does not filter.** The wire request asks for spent coins whatever the caller wanted,
    ///   and no height filter is applied here at all. A peer one block ahead would otherwise
    ///   silently omit a coin spent at `H + 1` that normalisation could then never recover, and a
    ///   per-source filter applied before comparison turns two honest truncations into a false
    ///   disagreement (chia-query#33). The caller's filters are applied to the AGREED set, in
    ///   [`read_set_corroborated`](Self::read_set_corroborated).
    async fn do_puzzle_hash_query(
        &self,
        peer: &Peer,
        hashes: &[&str],
        include_hinted: bool,
    ) -> Result<HeightedSet<CoinRecord>, ChiaQueryError> {
        let puzzle_hashes: Vec<Bytes32> = hashes
            .iter()
            .map(|h| translate::parse_bytes32(h))
            .collect::<Result<_, _>>()?;

        let filters = CoinStateFilters {
            // ALWAYS true, whatever the caller asked for. See the doc comment above.
            include_spent: true,
            include_unspent: true,
            include_hinted,
            min_amount: 0,
        };

        let mut all_states = Vec::new();
        // The peer protocol requires the header_hash to correspond to previous_height. We only
        // know the genesis header hash, so we always start from the beginning; the caller's
        // start_height is a projection applied after agreement.
        let mut prev_height: Option<u32> = None;
        let mut prev_header = self.genesis_challenge();
        let as_of_height;

        loop {
            let response = tokio::time::timeout(self.request_timeout, {
                peer.request_puzzle_state(
                    puzzle_hashes.clone(),
                    prev_height,
                    prev_header,
                    filters.clone(),
                    false,
                )
            })
            .await
            .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
            .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
            .map_err(|_| ChiaQueryError::PeerRejection("puzzle state request rejected".into()))?;

            all_states.extend(response.coin_states.iter().cloned());

            if response.is_finished {
                // The FINAL page's height is the anchor: the walk stops when it reaches the peer's
                // own subscription height, so this is the block the whole answer is a snapshot of.
                as_of_height = response.height;
                break;
            }
            prev_height = Some(response.height);
            prev_header = response.header_hash;
        }

        // `as_of_height` is the ONE anchor in this crate taken from an untrusted peer's wire
        // response rather than from a number the pool already polices, and the round's common
        // height is a `min` over it (chia-query#56). Held to the peer's own announced peak here,
        // at the point the value enters, so a source whose answer its announcements do not support
        // never reaches the round at all.
        //
        // Failing is the right shape rather than clamping: this peer's answer is unusable, and
        // `read_set_corroborated` already ejects a source whose read errors and excludes it from
        // the height vote. Clamping would keep a fabricated claim in the round under a nicer number.
        let announced = self.pool.announced_peak(peer.socket_addr()).await;
        if !as_of_is_supported(announced, as_of_height) {
            return Err(ChiaQueryError::PeerRejection(format!(
                "peer {} answered as of height {as_of_height}, which its own announced peak \
                 ({announced:?}) does not support",
                peer.socket_addr(),
            )));
        }

        Ok(HeightedSet {
            items: translate::coin_states_to_records(&all_states),
            as_of_height,
        })
    }

    async fn do_coin_ids_query(
        &self,
        peer: &Peer,
        names: &[String],
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let ids: Vec<Bytes32> = names
            .iter()
            .map(|n| translate::parse_bytes32(n))
            .collect::<Result<_, _>>()?;

        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(ids, None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state request rejected".into()))?;

        Ok(translate::coin_states_to_records(&response.coin_states))
    }

    /// The spend at `height` exactly as the peer sent it — placeholder coin and all.
    ///
    /// `Ok(None)` when the peer REJECTS the request, which is that peer's word that it has no such
    /// spend; `Err` only when the read itself fails.
    ///
    /// **The answer is bound to the coin that was asked about.** `PuzzleSolutionResponse` names
    /// the coin it is answering for, and a peer that answers about a different one is refuted
    /// locally rather than being put to a vote — the same binding the singleton lineage walk
    /// relies on. Without it a peer could answer every request with the one spend it happens to
    /// hold.
    ///
    /// The reply carries `coin_name` and NOT the full coin, so the returned spend has a
    /// name-only placeholder coin. Every caller substitutes the real one; see
    /// [`do_get_puzzle_and_solution_opt`](Self::do_get_puzzle_and_solution_opt) for why that is
    /// not optional.
    async fn do_get_puzzle_and_solution_raw(
        &self,
        peer: &Peer,
        coin_id: &str,
        height: u32,
    ) -> Result<Option<CoinSpend>, ChiaQueryError> {
        let id = translate::parse_bytes32(coin_id)?;

        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_puzzle_and_solution(id, height)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?;

        // A rejection is this peer's word that it has no such spend, not a failure of the read.
        let Ok(response) = response else {
            return Ok(None);
        };

        if response.coin_name != id {
            return Err(ChiaQueryError::PeerRejection(format!(
                "peer answered about coin {} when asked about {coin_id}",
                translate::hex32(&response.coin_name)
            )));
        }

        Ok(Some(translate::make_coin_spend(
            &chia_protocol::Coin {
                parent_coin_info: response.coin_name,
                puzzle_hash: Bytes32::default(),
                amount: 0,
            },
            &response.puzzle,
            &response.solution,
        )))
    }

    /// The spend at `height`, on the REAL coin, with its puzzle reveal already refuted or accepted.
    ///
    /// Costs one extra request — the coin state — and buys two things that the placeholder form
    /// cannot have:
    ///
    /// - **A comparable [`ChainClaim`].** A spend's claim includes the coin it spent. A placeholder
    ///   coin carries a zero puzzle hash and a zero amount, so a peer answer and a coinset answer
    ///   about the SAME spend would never compare equal and every settlement against coinset would
    ///   report [`SourcesDisagree`](ChiaQueryError::SourcesDisagree) — a false refusal on every
    ///   read, which is the "too strict" failure this crate's grading has to avoid as carefully as
    ///   the loose one.
    /// - **Local refutation of the reveal.** With the coin in hand,
    ///   [`verify_reveal_against_puzzle_hash`] settles whether the puzzle could possibly be the one
    ///   that locked this coin, without a vote a hostile majority could win.
    ///
    /// The SOLUTION remains unverifiable — nothing in the coin commits to it — which is exactly
    /// why the answer is still corroborated on top.
    async fn do_get_puzzle_and_solution_opt(
        &self,
        peer: &Peer,
        coin_id: &str,
        height: u32,
    ) -> Result<Option<CoinSpend>, ChiaQueryError> {
        let id = translate::parse_bytes32(coin_id)?;

        let state = tokio::time::timeout(self.request_timeout, {
            peer.request_coin_state(vec![id], None, self.genesis_challenge(), false)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("coin state rejected".into()))?;

        // A coin this peer does not know about has no spend on this peer's word either.
        let Some(cs) = state.coin_states.first() else {
            return Ok(None);
        };

        let Some(spend) = self
            .do_get_puzzle_and_solution_raw(peer, coin_id, height)
            .await?
        else {
            return Ok(None);
        };

        let spend = CoinSpend {
            coin: Coin::from_protocol(&cs.coin),
            ..spend
        };
        verify_reveal_against_puzzle_hash(&spend)?;
        Ok(Some(spend))
    }

    async fn do_get_fee_estimate(
        &self,
        peer: &Peer,
        target_times: &[u64],
    ) -> Result<FeeEstimate, ChiaQueryError> {
        let request = RequestFeeEstimates {
            time_targets: target_times.to_vec(),
        };

        let response: RespondFeeEstimates =
            tokio::time::timeout(self.request_timeout, peer.request_infallible(request))
                .await
                .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
                .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?;

        let estimates: Vec<f64> = response
            .estimates
            .estimates
            .iter()
            .map(|e| e.estimated_fee_rate.mojos_per_clvm_cost as f64)
            .collect();

        Ok(translate::make_fee_estimate(
            estimates,
            target_times.to_vec(),
        ))
    }

    async fn do_push_tx(
        &self,
        peer: &Peer,
        bundle: &SpendBundle,
    ) -> Result<TxStatus, ChiaQueryError> {
        let proto = to_protocol_spend_bundle(bundle)?;

        let ack = tokio::time::timeout(self.request_timeout, peer.send_transaction(proto))
            .await
            .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
            .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?;

        Ok(translate::ack_to_tx_status(ack.status, ack.error))
    }

    // -- full block by height (RequestBlock / RespondBlock) -------------------

    async fn do_get_block_by_height(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<serde_json::Value, ChiaQueryError> {
        let proto_block = self.fetch_full_block(peer, height).await?;
        serde_json::to_value(&proto_block)
            .map_err(|e| ChiaQueryError::PeerConnection(format!("serialize block: {e}")))
    }

    // -- additions and removals via CLVM (from chia-block-listener pattern) --

    async fn do_get_additions_and_removals_from_block(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        let proto_block = self.fetch_full_block(peer, height).await?;
        block::block_additions_and_removals(&proto_block, height, self.constants())
    }

    // -- block spends via CLVM (puzzle_reveal + solution) --------------------

    async fn do_get_block_spends(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<Vec<CoinSpend>, ChiaQueryError> {
        let proto_block = self.fetch_full_block(peer, height).await?;
        block::block_spends(&proto_block, self.constants())
    }

    // -- shared: fetch a FullBlock from a peer by height ---------------------

    async fn fetch_full_block(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<ProtoFullBlock, ChiaQueryError> {
        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_fallible::<RespondBlock, RejectBlock, _>(RequestBlock {
                height,
                include_transaction_block: true,
            })
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("block request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("block request rejected".into()))?;

        Ok(response.block)
    }

    // -- block record by height (from chia-block-listener pattern) -----------

    async fn do_get_block_record_by_height(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<BlockRecord, ChiaQueryError> {
        self.do_get_block_record_by_height_opt(peer, height)
            .await?
            .ok_or_else(|| ChiaQueryError::PeerRejection("header request rejected".into()))
    }

    /// Absence-aware block-record read: `Ok(None)` when the peer REJECTS the header request,
    /// `Err` when the read itself fails.
    ///
    /// A peer rejects a header request for a height beyond its own peak, so a rejection is that
    /// peer's word that there is no such block — one source's word, graded a layer up like any
    /// other absence.
    async fn do_get_block_record_by_height_opt(
        &self,
        peer: &Peer,
        height: u32,
    ) -> Result<Option<BlockRecord>, ChiaQueryError> {
        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_fallible::<RespondBlockHeader, RejectHeaderRequest, _>(
                RequestBlockHeader { height },
            )
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?;

        let Ok(response) = response else {
            return Ok(None);
        };

        Ok(Some(translate::header_block_to_block_record(
            &response.header_block,
        )))
    }

    // -- additions and removals (from chia-block-listener pattern) -----------

    async fn do_get_additions_and_removals(
        &self,
        peer: &Peer,
        height: u32,
        header_hash_hex: &str,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        let header_hash = translate::parse_bytes32(header_hash_hex)?;

        // Request additions and removals in parallel.
        let (adds_result, rems_result) = tokio::join!(
            tokio::time::timeout(self.request_timeout, {
                peer.request_fallible::<RespondAdditions, RejectAdditionsRequest, _>(
                    RequestAdditions {
                        height,
                        header_hash: Some(header_hash),
                        puzzle_hashes: None,
                    },
                )
            }),
            tokio::time::timeout(self.request_timeout, {
                peer.request_fallible::<RespondRemovals, RejectRemovalsRequest, _>(
                    RequestRemovals {
                        height,
                        header_hash,
                        coin_names: None,
                    },
                )
            }),
        );

        let adds = adds_result
            .map_err(|_| ChiaQueryError::PeerConnection("additions request timed out".into()))?
            .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
            .map_err(|_| ChiaQueryError::PeerRejection("additions rejected".into()))?;

        let rems = rems_result
            .map_err(|_| ChiaQueryError::PeerConnection("removals request timed out".into()))?
            .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
            .map_err(|_| ChiaQueryError::PeerRejection("removals rejected".into()))?;

        Ok(translate::additions_removals_to_response(
            &adds, &rems, height,
        ))
    }

    // -- children (RequestChildren is already on Peer) ----------------------

    async fn do_get_children(
        &self,
        peer: &Peer,
        parent_id: &str,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let coin_name = translate::parse_bytes32(parent_id)?;

        let response = tokio::time::timeout(self.request_timeout, peer.request_children(coin_name))
            .await
            .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
            .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?;

        Ok(translate::coin_states_to_records(&response.coin_states))
    }
}

// ---------------------------------------------------------------------------
// Local refutation
// ---------------------------------------------------------------------------

/// Refute a puzzle reveal that cannot belong to the coin it claims to unlock.
///
/// A coin's `puzzle_hash` IS the tree hash of the puzzle that locks it, so this is a fact the
/// reader can settle alone. Checking it locally is strictly better than corroborating it: a wrong
/// reveal is rejected outright rather than put to a vote that a hostile majority could win, and no
/// dial is spent.
///
/// It is only callable where the REAL coin is in hand. A `PuzzleSolutionResponse` carries the coin
/// name and not the coin, so a spend built from that reply alone has a placeholder puzzle hash and
/// nothing to check against — which is exactly why the spend read still needs corroboration on top
/// (chia-query#35).
///
/// The SOLUTION is not covered and cannot be: nothing in the coin commits to it, so a fabricated
/// solution parses like a real one. That is the residue corroboration exists for.
fn verify_reveal_against_puzzle_hash(spend: &CoinSpend) -> Result<(), ChiaQueryError> {
    let expected = translate::parse_hex(&spend.coin.puzzle_hash)?;
    let reveal = translate::parse_hex(&spend.puzzle_reveal)?;

    let actual = clvm_utils::tree_hash_from_bytes(&reveal).map_err(|e| {
        ChiaQueryError::PeerRejection(format!("puzzle reveal is not valid CLVM: {e}"))
    })?;

    if actual.to_bytes().as_slice() != expected.as_slice() {
        return Err(ChiaQueryError::PeerRejection(format!(
            "puzzle reveal hashes to 0x{} but coin {} is locked with {}",
            hex::encode(actual.to_bytes()),
            spend.coin.parent_coin_info,
            spend.coin.puzzle_hash
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SpendBundle conversion
// ---------------------------------------------------------------------------

fn to_protocol_spend_bundle(bundle: &SpendBundle) -> Result<ProtoBundle, ChiaQueryError> {
    let coin_spends: Vec<chia_protocol::CoinSpend> = bundle
        .coin_spends
        .iter()
        .map(|cs| {
            Ok(chia_protocol::CoinSpend {
                coin: chia_protocol::Coin {
                    parent_coin_info: translate::parse_bytes32(&cs.coin.parent_coin_info)?,
                    puzzle_hash: translate::parse_bytes32(&cs.coin.puzzle_hash)?,
                    amount: cs.coin.amount,
                },
                puzzle_reveal: chia_protocol::Program::from(chia_protocol::Bytes::from(
                    translate::parse_hex(&cs.puzzle_reveal)?,
                )),
                solution: chia_protocol::Program::from(chia_protocol::Bytes::from(
                    translate::parse_hex(&cs.solution)?,
                )),
            })
        })
        .collect::<Result<_, ChiaQueryError>>()?;

    let sig_bytes = translate::parse_hex(&bundle.aggregated_signature)?;
    let sig_arr: [u8; 96] = sig_bytes
        .try_into()
        .map_err(|_| ChiaQueryError::InvalidRequest("signature must be 96 bytes".into()))?;
    let aggregated_signature = chia_bls::Signature::from_bytes(&sig_arr)
        .map_err(|e| ChiaQueryError::InvalidRequest(format!("bad BLS signature: {e}")))?;

    Ok(ProtoBundle {
        coin_spends,
        aggregated_signature,
    })
}

#[cfg(test)]
mod grading_tests {
    use super::*;

    /// **A round that agrees cannot rescue a pool that was never armed.**
    ///
    /// This is the state the two conjuncts of [`corroborated`] exist to separate, and it is
    /// reachable in production for one reason: readiness is a snapshot taken BEFORE the round,
    /// while background refills can add peers during it — so more peers can agree than were held
    /// when the question was asked.
    ///
    /// No pool-level fixture can reach it, because a pool asks exactly the peers it counted. So it
    /// is asserted here, on the grading function itself. Without it, deleting the membership
    /// conjunct from [`corroborated`] leaves the whole suite green.
    #[test]
    fn agreement_alone_does_not_corroborate_when_the_pool_was_not_armed() {
        let unarmed = CorroborationReadiness::Insufficient {
            corroborators: 1,
            required: CORROBORATION_FLOOR,
        };

        assert!(
            !corroborated(unarmed, CORROBORATION_FLOOR),
            "a pool that held too few independent peers has corroborated nothing, however many \
             voices answered"
        );
    }

    /// The control from the other side: armed AND agreed is the only state that corroborates.
    ///
    /// Paired with the test above, this pins both conjuncts — a `corroborated` that always
    /// returned `false` would satisfy that one on its own.
    #[test]
    fn an_armed_pool_with_a_floor_of_agreement_corroborates() {
        let armed = CorroborationReadiness::Armed {
            corroborators: CORROBORATION_FLOOR,
        };

        assert!(corroborated(armed, CORROBORATION_FLOOR));
        assert!(
            !corroborated(armed, CORROBORATION_FLOOR - 1),
            "an armed pool whose corroborators timed out has corroborated nothing"
        );
    }
}
