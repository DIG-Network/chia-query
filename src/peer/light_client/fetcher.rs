//! [`CoinStateFetcher`] — the async seam the [`LightClientProvider`](super::LightClientProvider)
//! reads through, and [`PooledFetcher`], its implementation over the crate's SHARED
//! [`PeerPool`](crate::peer::pool::PeerPool).
//!
//! # Why this borrows a session rather than owning one
//!
//! Its ancestor (`chia-peer`'s `PeerFetcher`) held a `Peer` of its own and dialled to get it, which
//! is exactly the duplication dig_ecosystem#2761 exists to remove: a node running a light client
//! alongside a wallet replica held two TLS connections to two independently-chosen full nodes, with
//! two notions of the peak and nothing able to reconcile them. Here the pool is the only thing that
//! dials, and the light client is one of its borrowers.
//!
//! # Subscribing reads are ANCHORED; one-shot reads are not
//!
//! A subscription is server-side state on ONE connection. Arming it on whichever peer the pool's
//! rotation happened to return would scatter the subscription set across the pool, and the
//! resulting `CoinStateUpdate` pushes would arrive from sources the drive-loop is not following —
//! so they would be discarded as unattributed, and the cache would go quietly stale while every
//! surface kept reporting a healthy subscription.
//!
//! So a `subscribe = true` read always goes to the ANCHOR: the one pooled session this light client
//! has pinned. A `subscribe = false` read has no such constraint and goes through the pool's
//! ordinary rotation, which is strictly better than the single connection it replaces — a one-shot
//! read now draws from N held peers and ejects the one that fails it.
//!
//! # What this does NOT do
//!
//! It does not corroborate. Corroboration belongs to [`PeerPool`](crate::peer::pool::PeerPool),
//! gated on [`corroboration_readiness`](crate::peer::PeerBackend::corroboration_readiness) and
//! counting only [`Discovered`](crate::peer::connect::PeerOrigin::Discovered) peers, and folding
//! the light client in changes none of that. A light-client read is one peer's word, graded as such
//! by the registry that composes it.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chia_protocol::{Bytes32, CoinState, CoinStateFilters, Program, SpendBundle};
use chia_wallet_sdk::client::Peer;
use tokio::sync::{mpsc, RwLock};

use super::error::LightClientError;
use crate::peer::connect::PeerOrigin;
use crate::peer::frames::{FrameSource, FrameSubscription};
use crate::peer::PeerBackend;

/// Max pages an UNTRUSTED peer may return for one paged puzzle-state read before we fail closed.
/// Bounds a hostile peer that never sets `is_finished` (would otherwise hang + OOM).
const MAX_PUZZLE_STATE_PAGES: usize = 10_000;

/// Max coin states accumulated across a single paged read before we fail closed. Bounds unbounded
/// memory growth from a peer that streams coins forever.
const MAX_ACCUMULATED_COIN_STATES: usize = 500_000;

/// The reads a provider issues against a Chia full node when its local cache misses.
///
/// `subscribe = true` arms a server-side subscription so future changes stream back as
/// `CoinStateUpdate`s; `subscribe = false` is a one-shot read that never grows the subscription set.
#[async_trait]
pub trait CoinStateFetcher: Send + Sync {
    /// Reads the current state of `coin_ids`. An empty result is a PROVABLE absence.
    async fn coin_states(
        &self,
        coin_ids: Vec<Bytes32>,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, LightClientError>;

    /// Reads every coin paying to `puzzle_hashes` (paging through the peer's `is_finished` protocol
    /// until complete), applying `filters`.
    async fn puzzle_states(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, LightClientError>;

    /// Reads the direct children created by spending `coin_id`.
    async fn children(&self, coin_id: Bytes32) -> Result<Vec<CoinState>, LightClientError>;

    /// Reads the puzzle reveal + solution of the coin spent at `height`.
    ///
    /// Callers reach this ONLY after confirming the coin is spent, so absence is impossible: a
    /// rejection/absence is a "could not answer" and is returned as `Err(_)` (fail closed), NEVER a
    /// misleading `Ok(None)` that would corrupt the interface's parent-walk authentication.
    async fn puzzle_and_solution(
        &self,
        coin_id: Bytes32,
        height: u32,
    ) -> Result<(Program, Program), LightClientError>;
}

/// A [`CoinStateFetcher`] over the crate's shared peer pool.
///
/// Cloning shares the pool handle and the anchor, so a provider handed out by
/// [`ChiaLightClient::as_chain_source_provider`](super::ChiaLightClient::as_chain_source_provider)
/// reads through the same session the drive-loop is following.
#[derive(Clone)]
pub struct PooledFetcher {
    backend: Arc<PeerBackend>,
    /// The pinned session every SUBSCRIBING read is issued on — see the module docs.
    ///
    /// `None` until the first subscribing read pins one, and cleared by
    /// [`release_anchor`](Self::release_anchor) when that session ends, so the next subscribing
    /// read re-anchors on a live peer instead of a dead one.
    anchor: Arc<RwLock<Option<Anchor>>>,
    /// Where a newly pinned session's frame subscription is handed to the drive-loop.
    ///
    /// A subscription now names ONE session (chia-query#34), so it cannot be opened before the
    /// session exists — and the session this client follows is chosen lazily, by the first
    /// subscribing read. The drive-loop therefore starts with nothing to read and is fed a
    /// subscription each time an anchor is pinned.
    ///
    /// UNBOUNDED because it carries one item per re-anchor, an event driven by this client's own
    /// reconnects rather than by any peer, and because a bounded send that blocked would do so
    /// while holding the anchor write lock.
    subscriptions: mpsc::UnboundedSender<FrameSubscription>,
    /// Whether the subscription set is NO LONGER armed on a session this client is following.
    ///
    /// **Lives here, beside the anchor, because every way the set can come unarmed is an UNPIN of
    /// the anchor** — and a flag kept anywhere else has to be remembered at each of those sites.
    /// It was previously held by [`ChiaLightClient`](super::ChiaLightClient) and written only by
    /// the drive-loop, so the FAILED-READ path ([`discard`](Self::discard), reached from all seven
    /// read sites) unpinned the anchor and set nothing: from that moment the drive-loop followed
    /// nothing, every frame failed [`follows`](super::follows), and the cache froze while
    /// [`needs_rearm`](Self::needs_rearm) still answered "armed" (chia-query#59).
    ///
    /// Set on any real unpin, cleared only by a COMPLETED
    /// [`reconnect`](super::ChiaLightClient::reconnect). Erring towards set costs a redundant
    /// re-subscription; erring the other way costs a silently stale cache, which is the failure
    /// this whole signal exists to make visible.
    rearm_needed: Arc<AtomicBool>,
    request_timeout: Duration,
}

/// The pinned session: the peer to issue subscribing reads on, and who it is.
#[derive(Clone)]
pub(super) struct Anchor {
    pub(super) peer: Peer,
    pub(super) address: SocketAddr,
    /// The pooled session this anchor is pinned to, ADDRESS AND SESSION.
    ///
    /// The session half is what separates this connection from a replacement dialled to the same
    /// address. An anchor identified by address alone is torn down by its own predecessor's
    /// `SessionEnded`, which arrives whenever the dead connection's transport finally closes —
    /// after the pool has already ejected it and refilled the slot.
    pub(super) source: FrameSource,
    /// How the pool reached this peer. Held so the provider can say whether its answers come from
    /// an operator-preferred node or a discovered one, which is the fact
    /// [`ProviderKind`](dig_chainsource_interface::ProviderKind) exists to carry — and which
    /// `chia-peer` could not report at all, because its dialler had no origin concept.
    pub(super) origin: PeerOrigin,
}

impl PooledFetcher {
    /// Builds a fetcher drawing sessions from `backend`, bounding every request with
    /// `request_timeout`, and handing each newly pinned session's frame subscription to
    /// `subscriptions`.
    pub(super) fn new(
        backend: Arc<PeerBackend>,
        request_timeout: Duration,
        subscriptions: mpsc::UnboundedSender<FrameSubscription>,
    ) -> Self {
        Self {
            backend,
            anchor: Arc::new(RwLock::new(None)),
            subscriptions,
            rearm_needed: Arc::new(AtomicBool::new(false)),
            request_timeout,
        }
    }

    /// Whether the subscription set needs re-arming — see [`rearm_needed`](Self::rearm_needed).
    pub(super) fn needs_rearm(&self) -> bool {
        self.rearm_needed.load(Ordering::Acquire)
    }

    /// Records that the subscription set has been re-armed on the current anchor.
    pub(super) fn clear_rearm(&self) {
        self.rearm_needed.store(false, Ordering::Release);
    }

    /// The pinned session, drawn from the pool and recorded on first use.
    ///
    /// Written under the same lock the read takes, so two concurrent subscribing reads cannot pin
    /// two different sessions and split the subscription set between them.
    ///
    /// **Pinning and FOLLOWING happen together, under that one lock.** The frame subscription names
    /// the session (chia-query#34), so opening it here is what guarantees the drive-loop follows
    /// exactly the connection the subscribing reads are about to be issued on — rather than an
    /// address that may already hold a different one. A pool that no longer has a live session at
    /// the picked address REFUSES to anchor: pinning a dead session would arm subscriptions on a
    /// connection nothing will ever push to, and the cache would go quietly stale while every
    /// surface reported it healthy.
    pub(super) async fn anchor(&self) -> Result<Anchor, LightClientError> {
        let mut slot = self.anchor.write().await;
        if let Some(existing) = slot.as_ref() {
            return Ok(existing.clone());
        }
        let (peer, address) = self
            .backend
            .pick()
            .await
            .map_err(|e| LightClientError::Transport(e.to_string()))?;
        let origin = self
            .backend
            .pool
            .origin_of(address)
            .await
            .ok_or(LightClientError::NotConnected)?;
        let subscription = self
            .backend
            .subscribe_frames(address, super::FRAME_BUFFER)
            .await
            .ok_or(LightClientError::NotConnected)?;
        let anchor = Anchor {
            peer,
            address,
            origin,
            source: subscription.source(),
        };
        // A send failure means the drive-loop is gone, which happens only as the client is dropped.
        // The anchor is still valid for the reads in flight, so this is logged rather than failed.
        if self.subscriptions.send(subscription).is_err() {
            log::debug!(
                "light-client drive-loop has stopped; anchor {address} will not be followed"
            );
        }
        *slot = Some(anchor.clone());
        Ok(anchor)
    }

    /// The currently pinned session, without pinning one.
    pub(super) async fn current_anchor(&self) -> Option<Anchor> {
        self.anchor.read().await.clone()
    }

    /// The currently pinned SESSION, if any.
    pub(super) async fn anchor_source(&self) -> Option<FrameSource> {
        self.anchor.read().await.as_ref().map(|a| a.source)
    }

    /// Unpins the anchor if it is still `source`, so the next subscribing read re-anchors.
    ///
    /// Guarded on the whole [`FrameSource`] rather than on the address: a `SessionEnded` belonging
    /// to a session that is no longer the anchor must not tear down a healthy anchor pinned since,
    /// and after a reconnect the replacement frequently sits at the SAME address — so an
    /// address-only guard would let a dead connection's parting frame unpin its live successor.
    /// Unpinning ASKS FOR A RE-ARM, and does so here rather than at each call site. The
    /// subscription set is server-side state on the session that was pinned, so losing the pin is
    /// exactly the event that leaves it armed nowhere this client is reading from.
    ///
    /// Guarded on an ACTUAL unpin, not attempted unconditionally: a `SessionEnded` from a session
    /// this client already replaced does not unpin anything, and reporting a re-arm for it would
    /// undo one that had just succeeded.
    pub(super) async fn release_anchor(&self, source: FrameSource) {
        let mut slot = self.anchor.write().await;
        if slot.as_ref().is_some_and(|a| a.source == source) {
            *slot = None;
            self.rearm_needed.store(true, Ordering::Release);
        }
    }

    /// Unpins the anchor if it is held at `address`, whatever session that is.
    ///
    /// For the FAILED-READ path, where the address is all the failure names: a read that could not
    /// be served by the connection at `address` must not leave the anchor pointing there, and the
    /// only session this client could have been reading is the one it had pinned.
    /// Like [`release_anchor`](Self::release_anchor), this ASKS FOR A RE-ARM when it really unpins.
    /// A failed read leaves the subscription set armed on a session nothing is reading any more,
    /// which is indistinguishable from a quiet chain unless it is reported (chia-query#59).
    pub(super) async fn release_anchor_at(&self, address: SocketAddr) {
        let mut slot = self.anchor.write().await;
        if slot.as_ref().is_some_and(|a| a.address == address) {
            *slot = None;
            self.rearm_needed.store(true, Ordering::Release);
        }
    }

    /// The session a read of this kind is issued on: the anchor when it will arm a subscription,
    /// otherwise whichever peer the pool's rotation offers.
    async fn session(&self, subscribe: bool) -> Result<(Peer, SocketAddr), LightClientError> {
        if subscribe {
            let anchor = self.anchor().await?;
            return Ok((anchor.peer, anchor.address));
        }
        self.backend
            .pick()
            .await
            .map_err(|e| LightClientError::Transport(e.to_string()))
    }

    /// Removes a peer that failed a read from the pool, and unpins it if it was the anchor.
    ///
    /// Both halves are required: ejecting alone would leave the anchor pointing at a session the
    /// pool no longer holds, so every subsequent subscribing read would be issued on a dead
    /// connection and the drive-loop would follow a source that never speaks again.
    async fn discard(&self, address: SocketAddr) {
        self.backend.pool.eject_peer(address).await;
        self.release_anchor_at(address).await;
    }

    /// The height-0 `header_hash` the peer protocol requires (`Bytes32::default()` is rejected).
    fn genesis_challenge(&self) -> Bytes32 {
        self.backend.genesis_challenge()
    }

    /// Submits `bundle` to the network, returning the ack's `status` byte and the node's own
    /// error text where it gave one (`1` = admitted to the mempool; every other byte is not).
    ///
    /// A WRITE, so it goes to the anchor: the light client's cache follows that session, and
    /// pushing a bundle to a peer whose `CoinStateUpdate`s it discards would leave the spend
    /// invisible to its own reads.
    pub(super) async fn send_transaction(
        &self,
        bundle: SpendBundle,
    ) -> Result<(u8, Option<String>), LightClientError> {
        let (peer, address) = self.session(true).await?;
        let result = self
            .with_timeout(peer.send_transaction(bundle))
            .await
            .and_then(|r| r.map_err(|e| LightClientError::Transport(e.to_string())));
        match result {
            Ok(ack) => Ok((ack.status, ack.error)),
            Err(e) => {
                self.discard(address).await;
                Err(e)
            }
        }
    }

    /// Removes the server-side subscription to `coin_ids` from the anchor session.
    pub(super) async fn remove_coin_subscriptions(
        &self,
        coin_ids: Vec<Bytes32>,
    ) -> Result<(), LightClientError> {
        let (peer, address) = self.session(true).await?;
        let result = self
            .with_timeout(peer.remove_coin_subscriptions(Some(coin_ids)))
            .await
            .and_then(|r| r.map_err(|e| LightClientError::Transport(e.to_string())));
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                self.discard(address).await;
                Err(e)
            }
        }
    }

    /// Wraps `fut` with the configured request timeout, mapping an elapsed deadline to
    /// [`LightClientError::Timeout`].
    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = T>,
    ) -> Result<T, LightClientError> {
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| LightClientError::Timeout)
    }
}

/// One page of a paged `request_puzzle_state` read.
struct PuzzleStatePage {
    coin_states: Vec<CoinState>,
    height: u32,
    header_hash: Bytes32,
    is_finished: bool,
}

/// Drives a paged puzzle-state read to completion, bounding an UNTRUSTED peer three ways so it can
/// neither hang the caller nor exhaust memory: a page cap, a total accumulated-coin cap, and a
/// strict-progress requirement (each unfinished page MUST advance `height`). Any violation fails
/// closed with `Err`, never an unbounded loop.
///
/// The page fetch is injected so the bounding policy is unit-testable without a live peer.
async fn collect_paged<F, Fut>(
    genesis_challenge: Bytes32,
    mut fetch_page: F,
) -> Result<Vec<CoinState>, LightClientError>
where
    F: FnMut(Option<u32>, Bytes32) -> Fut,
    Fut: std::future::Future<Output = Result<PuzzleStatePage, LightClientError>>,
{
    let mut all = Vec::new();
    let mut previous_height: Option<u32> = None;
    let mut header_hash = genesis_challenge;

    for _page in 0..MAX_PUZZLE_STATE_PAGES {
        let page = fetch_page(previous_height, header_hash).await?;
        all.extend(page.coin_states);
        if all.len() > MAX_ACCUMULATED_COIN_STATES {
            return Err(LightClientError::Rejected(format!(
                "puzzle-state response exceeded {MAX_ACCUMULATED_COIN_STATES} coins"
            )));
        }
        if page.is_finished {
            return Ok(all);
        }
        if previous_height.is_some_and(|prev| page.height <= prev) {
            return Err(LightClientError::Rejected(
                "puzzle-state paging did not advance the height".into(),
            ));
        }
        previous_height = Some(page.height);
        header_hash = page.header_hash;
    }
    Err(LightClientError::Rejected(format!(
        "puzzle-state paging exceeded {MAX_PUZZLE_STATE_PAGES} pages"
    )))
}

#[async_trait]
impl CoinStateFetcher for PooledFetcher {
    async fn coin_states(
        &self,
        coin_ids: Vec<Bytes32>,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, LightClientError> {
        let (peer, address) = self.session(subscribe).await?;
        let result = self
            .with_timeout(peer.request_coin_state(
                coin_ids,
                None,
                self.genesis_challenge(),
                subscribe,
            ))
            .await
            .and_then(|r| r.map_err(|e| LightClientError::Transport(e.to_string())))
            .and_then(|r| {
                r.map_err(|_| LightClientError::Rejected("coin-state request rejected".into()))
            });
        match result {
            Ok(response) => Ok(response.coin_states),
            Err(e) => {
                self.discard(address).await;
                Err(e)
            }
        }
    }

    async fn puzzle_states(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, LightClientError> {
        // Every page of one paged read MUST come from the SAME session: the protocol's
        // `previous_height`/`header_hash` cursor is that peer's, and continuing it on another peer
        // asks a stranger to resume a walk it never started. So the session is drawn once, here,
        // and closed over — never re-drawn per page.
        let (peer, address) = self.session(subscribe).await?;
        // Subscribe only once the final page arrives, so a single subscription covers the set.
        let result = collect_paged(self.genesis_challenge(), |previous_height, header_hash| {
            let peer = peer.clone();
            let puzzle_hashes = puzzle_hashes.clone();
            let filters = filters.clone();
            let this = self;
            async move {
                let response = this
                    .with_timeout(peer.request_puzzle_state(
                        puzzle_hashes,
                        previous_height,
                        header_hash,
                        filters,
                        subscribe,
                    ))
                    .await?
                    .map_err(|e| LightClientError::Transport(e.to_string()))?
                    .map_err(|_| {
                        LightClientError::Rejected("puzzle-state request rejected".into())
                    })?;
                Ok(PuzzleStatePage {
                    coin_states: response.coin_states,
                    height: response.height,
                    header_hash: response.header_hash,
                    is_finished: response.is_finished,
                })
            }
        })
        .await;
        if result.is_err() {
            self.discard(address).await;
        }
        result
    }

    async fn children(&self, coin_id: Bytes32) -> Result<Vec<CoinState>, LightClientError> {
        let (peer, address) = self.session(false).await?;
        let result = self
            .with_timeout(peer.request_children(coin_id))
            .await
            .and_then(|r| r.map_err(|e| LightClientError::Transport(e.to_string())));
        match result {
            Ok(response) => Ok(response.coin_states),
            Err(e) => {
                self.discard(address).await;
                Err(e)
            }
        }
    }

    async fn puzzle_and_solution(
        &self,
        coin_id: Bytes32,
        height: u32,
    ) -> Result<(Program, Program), LightClientError> {
        let (peer, address) = self.session(false).await?;
        let outcome = match self
            .with_timeout(peer.request_puzzle_and_solution(coin_id, height))
            .await
            .and_then(|r| r.map_err(|e| LightClientError::Transport(e.to_string())))
        {
            Ok(outcome) => outcome,
            Err(e) => {
                self.discard(address).await;
                return Err(e);
            }
        };
        match outcome {
            Ok(response) => Ok((response.puzzle, response.solution)),
            // The caller only asks after confirming a spent height, so a reject here is NOT a
            // genuine absence — it is a peer that could not/would not answer. Fail closed with Err,
            // never a misleading Ok(None) (mirrors how coin-state rejects map to Err(Rejected)).
            Err(_) => {
                self.discard(address).await;
                Err(LightClientError::Rejected(
                    "peer rejected puzzle/solution for a known-spent coin".into(),
                ))
            }
        }
    }
}
