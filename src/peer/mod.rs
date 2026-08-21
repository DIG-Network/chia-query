pub mod block;
pub mod connect;
pub mod pool;
pub mod translate;

#[cfg(test)]
mod corroboration_tests;
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
use pool::PeerPool;
pub use pool::PeerRequirement;

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
        Self {
            pool: pool::PeerPool::for_tests(0),
            network: NetworkType::Mainnet,
            request_timeout: Duration::from_millis(1),
        }
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

    // -----------------------------------------------------------------------
    // Select a peer (round-robin) then attempt to refill if pool is short.
    // -----------------------------------------------------------------------

    async fn pick(&self) -> Result<(Peer, SocketAddr), ChiaQueryError> {
        // Attempt a background refill if under capacity.
        self.pool.try_refill().await;

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
    /// [discovered](pool::PeerPool::select_corroborating_peer) rather than preferred. Neither
    /// direction is taken on one peer's word:
    ///
    /// - **Present** — the record's chain claim (see [`ChainClaim`]) is put to every independent
    ///   peer at once. One that agrees makes it [`Found`](OptAnswer::Found); one that says
    ///   anything else — a different height, or nothing at all — fails the read with
    ///   [`SourcesDisagree`](ChiaQueryError::SourcesDisagree); nobody able to answer leaves it
    ///   [`UncorroboratedFound`](OptAnswer::UncorroboratedFound).
    /// - **Absent** — one further independent peer is asked the same question. Both absent is
    ///   [`CorroboratedAbsent`](OptAnswer::CorroboratedAbsent); a peer that produces the thing is
    ///   [`SourcesDisagree`](ChiaQueryError::SourcesDisagree); nobody to ask is
    ///   [`UncorroboratedAbsent`](OptAnswer::UncorroboratedAbsent).
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

        if agreed == 0 {
            log::debug!("presence reported by {addr} was corroborated by nobody who could answer");
            return Ok(OptAnswer::UncorroboratedFound(found));
        }
        Ok(OptAnswer::Found(found))
    }

    /// Put an absence to ONE further independent peer, and grade the agreement.
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
        let Some((other, other_addr)) = self.pool.select_corroborating_peer(addr).await else {
            log::debug!("absence reported by {addr} has no independent corroborator available");
            return Ok(OptAnswer::UncorroboratedAbsent);
        };

        match read(other).await {
            Ok(None) => Ok(OptAnswer::CorroboratedAbsent),
            Ok(Some(_)) => Err(ChiaQueryError::SourcesDisagree(format!(
                "peer {addr} reports absent, peer {other_addr} reports present"
            ))),
            Err(e) => {
                // The corroborator could not answer, so nothing was corroborated. Eject it — a
                // peer that fails a read is ejected everywhere else in this backend — and report
                // the absence as the undecided fact it still is, rather than letting a failed
                // second opinion read as a confirmed first one.
                self.pool.eject_peer(other_addr).await;
                log::debug!("corroborator {other_addr} failed: {e}");
                Ok(OptAnswer::UncorroboratedAbsent)
            }
        }
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

    pub async fn try_get_coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_puzzle_hash_query(
                &peer,
                &[puzzle_hash],
                start_height,
                end_height,
                include_spent,
                false,
            )
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    pub async fn try_get_coin_records_by_puzzle_hashes(
        &self,
        puzzle_hashes: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let hashes: Vec<&str> = puzzle_hashes.iter().map(String::as_str).collect();
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_puzzle_hash_query(
                &peer,
                &hashes,
                start_height,
                end_height,
                include_spent,
                false,
            )
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    pub async fn try_get_coin_records_by_hint(
        &self,
        hint: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_puzzle_hash_query(
                &peer,
                &[hint],
                start_height,
                end_height,
                include_spent,
                true,
            )
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    pub async fn try_get_coin_records_by_hints(
        &self,
        hints: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let hs: Vec<&str> = hints.iter().map(String::as_str).collect();
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_puzzle_hash_query(&peer, &hs, start_height, end_height, include_spent, true)
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    pub async fn try_get_coin_records_by_names(
        &self,
        names: &[String],
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_coin_ids_query(&peer, names).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    pub async fn try_get_puzzle_and_solution(
        &self,
        coin_id: &str,
        height: u32,
    ) -> Result<CoinSpend, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self
            .do_get_puzzle_and_solution(&peer, coin_id, height)
            .await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

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

    pub async fn try_push_tx(&self, bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_push_tx(&peer, bundle).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- block record by height (RequestBlockHeader) -------------------------

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

    pub async fn try_get_children(
        &self,
        parent_id: &str,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let (peer, addr) = self.pick().await?;
        let res = self.do_get_children(&peer, parent_id).await;
        if res.is_err() {
            self.pool.eject_peer(addr).await;
        }
        res
    }

    // -- get full block by height (RequestBlock) ------------------------------

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
            .do_get_puzzle_and_solution(&peer, coin_id, spent_height)
            .await;
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

    pub fn peak_height(&self) -> u32 {
        self.pool.peak_height()
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

        let spend = self
            .do_get_puzzle_and_solution(peer, coin_id, spent_height)
            .await?;

        // Substitute the GENUINE spent coin from the coin-state lookup for the name-only placeholder
        // that `do_get_puzzle_and_solution` builds (the peer `PuzzleSolutionResponse` omits the full
        // coin). The singleton-lineage walk binds each fetched spend to the requested coin id
        // (`spend.coin.coin_id() == current`, chia-query#7); a placeholder coin hashes to the wrong
        // id and fails that binding closed, making peer-sourced lineage resolution impossible. The
        // real coin is already in hand here, so return it and let the binding authenticate the hop.
        let spend = CoinSpend {
            coin: Coin::from_protocol(&cs.coin),
            ..spend
        };
        Ok(Some(spend))
    }

    async fn do_puzzle_hash_query(
        &self,
        peer: &Peer,
        hashes: &[&str],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent: bool,
        include_hinted: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        let puzzle_hashes: Vec<Bytes32> = hashes
            .iter()
            .map(|h| translate::parse_bytes32(h))
            .collect::<Result<_, _>>()?;

        let filters = CoinStateFilters {
            include_spent,
            include_unspent: true,
            include_hinted,
            min_amount: 0,
        };

        let mut all_states = Vec::new();
        // The peer protocol requires the header_hash to correspond to
        // previous_height.  We only know the genesis header hash, so we always
        // start from the beginning and apply start_height as a client-side
        // filter.  For callers that provide a start_height, this is slower but
        // correct.
        let mut prev_height: Option<u32> = None;
        let mut prev_header = self.genesis_challenge();

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
                break;
            }
            prev_height = Some(response.height);
            prev_header = response.header_hash;
        }

        // Client-side height filters.
        let records: Vec<CoinRecord> = all_states
            .iter()
            .filter(|cs| {
                let h = cs.created_height.unwrap_or(0);
                let above_start = start_height.is_none_or(|s| h >= s);
                let below_end = end_height.is_none_or(|e| h <= e);
                above_start && below_end
            })
            .map(translate::coin_state_to_record)
            .collect();

        Ok(records)
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

    async fn do_get_puzzle_and_solution(
        &self,
        peer: &Peer,
        coin_id: &str,
        height: u32,
    ) -> Result<CoinSpend, ChiaQueryError> {
        let id = translate::parse_bytes32(coin_id)?;

        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_puzzle_and_solution(id, height)
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("puzzle solution rejected".into()))?;

        Ok(translate::make_coin_spend(
            // We need the coin for the CoinSpend.  The peer response
            // (PuzzleSolutionResponse) has coin_name but not the full coin.
            // We'll build a partial coin using the name as parent_coin_info
            // placeholder -- the puzzle_reveal and solution are the important
            // parts.  Callers who need the full coin can query separately.
            &chia_protocol::Coin {
                parent_coin_info: response.coin_name,
                puzzle_hash: Bytes32::default(),
                amount: 0,
            },
            &response.puzzle,
            &response.solution,
        ))
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

        Ok(translate::ack_to_tx_status(ack.status))
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
        let response = tokio::time::timeout(self.request_timeout, {
            peer.request_fallible::<RespondBlockHeader, RejectHeaderRequest, _>(
                RequestBlockHeader { height },
            )
        })
        .await
        .map_err(|_| ChiaQueryError::PeerConnection("request timed out".into()))?
        .map_err(|e| ChiaQueryError::PeerConnection(e.to_string()))?
        .map_err(|_| ChiaQueryError::PeerRejection("header request rejected".into()))?;

        Ok(translate::header_block_to_block_record(
            &response.header_block,
        ))
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
