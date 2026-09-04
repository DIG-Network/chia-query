//! QueryRouter -- dispatches each request to the peer backend first (with one
//! retry on a different peer) and falls back to the coinset.org HTTP API if
//! both peer attempts fail.

use std::collections::HashMap;
use std::sync::Arc;

use chia_consensus::consensus_constants::ConsensusConstants;
use chia_consensus::flags::DONT_VALIDATE_SIGNATURE;
use serde_json::Value;

use crate::coinset::CoinsetClient;
use crate::peer::set_agreement::{
    common_height, contradiction, fingerprint, normalise_at, project,
};
use crate::peer::{CorroboratedSet, OptAnswer, PeerBackend, SetAnswer, SetProjection};
use crate::types::*;

#[cfg(test)]
mod absence_tests;
#[cfg(test)]
mod presence_tests;
#[cfg(test)]
mod set_settlement_tests;

// ---------------------------------------------------------------------------
// Puzzle condition extraction helper
// ---------------------------------------------------------------------------

/// Run a puzzle against its solution (from a CoinSpend) and extract the CLVM
/// output conditions.  Used by `get_puzzle_and_solution_with_conditions`.
fn run_puzzle_conditions(spend: &CoinSpend, constants: &ConsensusConstants) -> Vec<Condition> {
    let flags = DONT_VALIDATE_SIGNATURE;
    let Ok(puzzle_bytes) = crate::peer::translate::parse_hex(&spend.puzzle_reveal) else {
        return Vec::new();
    };
    let Ok(solution_bytes) = crate::peer::translate::parse_hex(&spend.solution) else {
        return Vec::new();
    };

    let mut allocator = chia_consensus::allocator::make_allocator(flags);

    let Ok(puzzle_node) = clvmr::serde::node_from_bytes(&mut allocator, &puzzle_bytes) else {
        return Vec::new();
    };
    let Ok(solution_node) = clvmr::serde::node_from_bytes(&mut allocator, &solution_bytes) else {
        return Vec::new();
    };

    let dialect = clvmr::chia_dialect::ChiaDialect::new(flags);
    match clvmr::run_program::run_program(
        &mut allocator,
        &dialect,
        puzzle_node,
        solution_node,
        constants.max_block_cost_clvm,
    ) {
        Ok(clvmr::reduction::Reduction(_, output)) => {
            crate::peer::block::parse_conditions_public(&allocator, output)
        }
        Err(_) => Vec::new(),
    }
}

/// Decide what one peer's uncorroborated absence becomes, given whatever the coinset tier said.
///
/// `coinset` is `None` when the coinset tier was not consulted at all because the fallback is
/// disabled — the distinction matters, since "nobody else was asked" and "somebody else was asked
/// and agreed" are the two facts this whole change exists to keep apart.
///
/// Absence is only ever reported when a SECOND source says it too. Everything else is an error,
/// and a contradiction is surfaced rather than broken in favour of either source: nothing in two
/// contradictory answers says which one to believe.
fn settle_uncorroborated_absence<T>(
    coinset: Option<Result<Option<T>, ChiaQueryError>>,
) -> Result<Option<T>, ChiaQueryError> {
    match coinset {
        None => Err(ChiaQueryError::UncorroboratedAbsence(
            "one peer reported absence, no second peer was available, and the coinset fallback is \
             disabled"
                .into(),
        )),
        Some(Ok(None)) => Ok(None),
        Some(Ok(Some(_))) => Err(ChiaQueryError::SourcesDisagree(
            "a peer reports absent, the coinset API reports present".into(),
        )),
        Some(Err(e)) => Err(ChiaQueryError::UncorroboratedAbsence(format!(
            "one peer reported absence and the coinset API could not corroborate it: {e}"
        ))),
    }
}

/// What a positive answer that only ONE source will vouch for becomes.
///
/// The mirror of [`settle_uncorroborated_absence`], and the more dangerous of the two: an absence
/// nobody can confirm leaves a caller polling, while a PRESENCE nobody can confirm makes it stop
/// and record a height. `coinset` is `Some` only when the fallback tier was consulted — `None`
/// means it is disabled, and a fact nobody could be brought to agree with is never returned as one
/// (dig_ecosystem#2462).
fn settle_uncorroborated_presence<T: ChainClaim>(
    found: T,
    coinset: Option<Result<Option<T>, ChiaQueryError>>,
) -> Result<Option<T>, ChiaQueryError> {
    match coinset {
        None => Err(ChiaQueryError::UncorroboratedPresence(
            "one peer produced a record, no second peer was available, and the coinset fallback \
             is disabled"
                .into(),
        )),
        Some(Ok(Some(other))) if other.chain_claim() == found.chain_claim() => Ok(Some(found)),
        Some(Ok(Some(other))) => Err(ChiaQueryError::SourcesDisagree(format!(
            "a peer claims `{}`, the coinset API claims `{}`",
            found.chain_claim(),
            other.chain_claim()
        ))),
        Some(Ok(None)) => Err(ChiaQueryError::SourcesDisagree(
            "a peer reports present, the coinset API reports absent".into(),
        )),
        Some(Err(e)) => Err(ChiaQueryError::UncorroboratedPresence(format!(
            "one peer produced a record and the coinset API could not corroborate it: {e}"
        ))),
    }
}

/// What a population answer that only ONE peer will vouch for becomes.
///
/// The set counterpart of [`settle_uncorroborated_presence`], and it settles the same way: coinset
/// is asked the SAME question, its answer is held to the SAME height `H` the peer answer was held
/// to, and the two normalised sets must be equal. Comparing coinset's answer at its own tip against
/// a peer answer at `H` would manufacture a disagreement out of ordinary lag — the exact trap the
/// set rule exists to avoid.
///
/// `coinset` is `Some` only when the fallback tier was consulted; `None` means it is disabled, and
/// a set nobody could be brought to agree with is never returned as one. `coinset` MUST be the
/// UNFILTERED form of the question — every coin in range, spent ones included — because a filter
/// applied to one side of a comparison turns a projection into an omission (chia-query#33).
///
/// | peer tier | coinset | result |
/// |---|---|---|
/// | one peer's set | the same set at `H` | `Ok` — two independent sources agree |
/// | one peer's set | a different set at `H` | [`SourcesDisagree`](ChiaQueryError::SourcesDisagree) |
/// | one peer's set | unreachable or disabled | [`UncorroboratedPresence`](ChiaQueryError::UncorroboratedPresence) |
///
/// There is no arm that returns a set nobody would second. That is the point: the established
/// behaviour returned exactly that, silently, from whichever source answered first.
fn settle_uncorroborated_set(
    items: Vec<CoinRecord>,
    as_of_height: u32,
    projection: SetProjection,
    coinset: Option<Result<Vec<CoinRecord>, ChiaQueryError>>,
) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
    let raw = match coinset {
        None => {
            return Err(ChiaQueryError::UncorroboratedPresence(
                "one peer produced a set, no floor of independent peers agreed with it, and the                  coinset fallback is disabled"
                    .into(),
            ))
        }
        Some(Err(e)) => {
            return Err(ChiaQueryError::UncorroboratedPresence(format!(
                "one peer produced a set and the coinset API could not corroborate it: {e}"
            )))
        }
        Some(Ok(raw)) => raw,
    };

    let theirs = project(normalise_at(&raw, as_of_height), projection);
    match contradiction(
        "the peer tier",
        &fingerprint(&items),
        "the coinset API",
        &fingerprint(&theirs),
    ) {
        None => Ok(CorroboratedSet {
            items,
            as_of_height,
        }),
        Some(detail) => Err(ChiaQueryError::SourcesDisagree(format!(
            "at height {as_of_height}: {detail}"
        ))),
    }
}

pub struct QueryRouter {
    /// The peer tier, SHARED.
    ///
    /// Held behind an `Arc` so a [`ChiaLightClient`](crate::peer::ChiaLightClient) built from the
    /// same client borrows this pool rather than dialling one of its own — the unification
    /// dig_ecosystem#2761 exists to make. Two pools would mean two peaks and two sets of held
    /// peers inside one process, which is the state this replaces.
    pub(crate) peer: Arc<PeerBackend>,
    pub(crate) coinset: CoinsetClient,
    pub(crate) coinset_fallback_enabled: bool,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl QueryRouter {
    /// Try `peer_fn` twice (each call will select a different peer because the
    /// first failure ejects the peer).  If both fail, fall back to `coinset_fn`.
    async fn peer_then_coinset<T>(
        &self,
        peer_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
        peer_retry: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
        coinset_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
    ) -> Result<T, ChiaQueryError> {
        // First peer attempt
        match peer_fn.await {
            Ok(v) => return Ok(v),
            Err(e) => log::debug!("peer attempt 1 failed: {e}"),
        }

        // Retry on a different peer
        match peer_retry.await {
            Ok(v) => Ok(v),
            Err(peer_err) => {
                if !self.coinset_fallback_enabled {
                    return Err(peer_err);
                }
                // Fall back to coinset
                coinset_fn
                    .await
                    .map_err(|ce| ChiaQueryError::AllSourcesFailed {
                        peer_error: Box::new(peer_err),
                        coinset_error: Some(Box::new(ce)),
                    })
            }
        }
    }

    /// Absence-aware variant of [`peer_then_coinset`](Self::peer_then_coinset).
    ///
    /// `Ok(None)` from this router means **corroborated absence**: two independent sources were
    /// asked and both said the thing does not exist. Absence that only one source will vouch for
    /// is [`UncorroboratedAbsence`](ChiaQueryError::UncorroboratedAbsence) — an error, because a
    /// caller reading `None` as "the chain provably does not have this" would otherwise be told a
    /// falsehood by one anonymous peer's empty list (dig_ecosystem#2456).
    ///
    /// The peer tier grades its own answer (see
    /// [`PeerBackend::read_opt_corroborated`](crate::peer::PeerBackend)); this method decides what
    /// an ungraded absence becomes once the coinset tier is available to be a second voice:
    ///
    /// | peer tier | coinset | result |
    /// |---|---|---|
    /// | found | not asked | `Ok(Some)` — presence is self-verifying |
    /// | both peers absent | not asked | `Ok(None)` |
    /// | one peer absent | absent | `Ok(None)` — two independent sources agree |
    /// | one peer absent | found | [`SourcesDisagree`](ChiaQueryError::SourcesDisagree) |
    /// | one peer absent | unreachable or disabled | [`UncorroboratedAbsence`](ChiaQueryError::UncorroboratedAbsence) |
    /// | peer read failed | any | the retry, then the coinset fallback, as before |
    ///
    /// **The stated limit.** When no peer answers at all, the coinset tier is the only source
    /// there is, and its absence is returned as `Ok(None)` on its own — the behaviour a
    /// coinset-only client has always had. That is one source, so it is one source's word; what
    /// this method removes is absence resting on an *anonymous, unauthenticated* peer, not the
    /// weaker claim that a single named HTTPS endpoint is infallible.
    async fn peer_then_coinset_opt<T: ChainClaim>(
        &self,
        peer_fn: impl std::future::Future<Output = Result<OptAnswer<T>, ChiaQueryError>>,
        peer_retry: impl std::future::Future<Output = Result<OptAnswer<T>, ChiaQueryError>>,
        coinset_fn: impl std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    ) -> Result<Option<T>, ChiaQueryError> {
        let first = match peer_fn.await {
            Ok(answer) => Some(answer),
            Err(e) => {
                log::debug!("peer opt attempt 1 failed: {e}");
                None
            }
        };

        if let Some(answer) = first {
            return self.settle_peer_answer(answer, coinset_fn).await;
        }

        match peer_retry.await {
            Ok(answer) => self.settle_peer_answer(answer, coinset_fn).await,
            Err(peer_err) => {
                if !self.coinset_fallback_enabled {
                    return Err(peer_err);
                }
                coinset_fn
                    .await
                    .map_err(|ce| ChiaQueryError::AllSourcesFailed {
                        peer_error: Box::new(peer_err),
                        coinset_error: Some(Box::new(ce)),
                    })
            }
        }
    }

    /// Turn the peer tier's graded answer into the router's contract, consulting coinset as a
    /// second voice when — and only when — the peer tier could not find one itself.
    async fn settle_peer_answer<T: ChainClaim>(
        &self,
        answer: OptAnswer<T>,
        coinset_fn: impl std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    ) -> Result<Option<T>, ChiaQueryError> {
        match answer {
            OptAnswer::Found(v) => Ok(Some(v)),
            OptAnswer::CorroboratedAbsent => Ok(None),
            OptAnswer::UncorroboratedFound(v) => {
                let coinset = if self.coinset_fallback_enabled {
                    Some(coinset_fn.await)
                } else {
                    None
                };
                settle_uncorroborated_presence(v, coinset)
            }
            OptAnswer::UncorroboratedAbsent => {
                let coinset = if self.coinset_fallback_enabled {
                    Some(coinset_fn.await)
                } else {
                    None
                };
                settle_uncorroborated_absence(coinset)
            }
        }
    }

    /// Turn the peer tier's graded set answer into the router's contract, consulting coinset as a
    /// second voice when — and only when — the peer tier could not find one itself.
    ///
    /// The set counterpart of [`settle_peer_answer`](Self::settle_peer_answer). The decision
    /// itself lives in [`settle_uncorroborated_set`], a free function, so it can be exercised
    /// without a router; this method is the plumbing that fetches the second opinion.
    async fn settle_set_answer(
        &self,
        answer: SetAnswer<CoinRecord>,
        projection: SetProjection,
        coinset_raw: impl std::future::Future<Output = Result<Vec<CoinRecord>, ChiaQueryError>>,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        match answer {
            SetAnswer::Corroborated {
                items,
                as_of_height,
            } => Ok(CorroboratedSet {
                items,
                as_of_height,
            }),
            SetAnswer::Uncorroborated {
                items,
                as_of_height,
            } => {
                let coinset = if self.coinset_fallback_enabled {
                    Some(coinset_raw.await)
                } else {
                    None
                };
                settle_uncorroborated_set(items, as_of_height, projection, coinset)
            }
        }
    }

    /// Population-read counterpart of [`peer_then_coinset_opt`](Self::peer_then_coinset_opt).
    ///
    /// Same shape: one peer attempt, one retry on a different peer, and only then the coinset tier
    /// on its own. The difference is what happens when a peer DOES answer — the answer is graded
    /// and settled rather than returned, so "the first responsive source wins" stops being the
    /// contract (chia-query#47).
    async fn peer_then_coinset_set(
        &self,
        peer_fn: impl std::future::Future<Output = Result<SetAnswer<CoinRecord>, ChiaQueryError>>,
        peer_retry: impl std::future::Future<Output = Result<SetAnswer<CoinRecord>, ChiaQueryError>>,
        coinset_raw: impl std::future::Future<Output = Result<Vec<CoinRecord>, ChiaQueryError>>,
        projection: SetProjection,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let first = match peer_fn.await {
            Ok(answer) => Some(answer),
            Err(e) => {
                log::debug!("peer set attempt 1 failed: {e}");
                None
            }
        };

        if let Some(answer) = first {
            return self
                .settle_set_answer(answer, projection, coinset_raw)
                .await;
        }

        match peer_retry.await {
            Ok(answer) => {
                self.settle_set_answer(answer, projection, coinset_raw)
                    .await
            }
            Err(peer_err) => {
                if !self.coinset_fallback_enabled {
                    return Err(peer_err);
                }
                self.coinset_only_set(coinset_raw, projection, peer_err)
                    .await
            }
        }
    }

    /// The coinset tier answering ALONE, because no peer answered at all.
    ///
    /// **The stated limit, unchanged from the scalar path**: with no peer reachable the coinset API
    /// is the only source there is, so this is one source's word. What the crate removes is a set
    /// resting on an *anonymous, unauthenticated* peer, not the weaker claim that a single named
    /// HTTPS endpoint is infallible.
    ///
    /// It still gets a height. Coinset states no as-of height with its answers, so its own peak is
    /// read and put through the SAME `common_height` rule — a coinset answer is a source's answer,
    /// and a source that cannot be dated cannot be compared to anything later.
    async fn coinset_only_set(
        &self,
        coinset_raw: impl std::future::Future<Output = Result<Vec<CoinRecord>, ChiaQueryError>>,
        projection: SetProjection,
        peer_err: ChiaQueryError,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let all_sources_failed = |ce: ChiaQueryError| ChiaQueryError::AllSourcesFailed {
            peer_error: Box::new(peer_err),
            coinset_error: Some(Box::new(ce)),
        };

        let peak = match self.coinset.get_blockchain_state().await {
            Ok(state) => state.peak.map(|p| p.height),
            Err(ce) => return Err(all_sources_failed(ce)),
        };
        let Some(peak) = peak else {
            return Err(all_sources_failed(ChiaQueryError::CoinsetApiError(
                "the coinset API reports no peak, so its answer cannot be held to a height".into(),
            )));
        };
        let Some(as_of_height) = common_height(&[peak], projection.end_height) else {
            return Err(all_sources_failed(ChiaQueryError::CoinsetApiError(
                format!("no settled common height exists below the coinset peak {peak}"),
            )));
        };

        let raw = match coinset_raw.await {
            Ok(raw) => raw,
            Err(ce) => return Err(all_sources_failed(ce)),
        };

        Ok(CorroboratedSet {
            items: project(normalise_at(&raw, as_of_height), projection),
            as_of_height,
        })
    }

    /// For endpoints that have no peer protocol equivalent.
    fn require_coinset(&self, endpoint: &str) -> Result<(), ChiaQueryError> {
        if !self.coinset_fallback_enabled {
            Err(ChiaQueryError::UnsupportedWithoutCoinset(endpoint.into()))
        } else {
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Blocks (all coinset-only)
// ---------------------------------------------------------------------------

impl QueryRouter {
    /// Peer-backed: fetches the full block by header_hash (via coinset to
    /// resolve the height), then parses additions/removals from the CLVM
    /// generator.  Falls back to the coinset endpoint on failure.
    pub async fn get_additions_and_removals(
        &self,
        header_hash: &str,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        // The peer protocol needs a height.  Resolve it from the block record.
        if let Ok(record) = self.get_block_record(header_hash).await {
            // Try parsing via peer + CLVM.
            match self
                .peer
                .try_get_additions_and_removals_from_block(record.height)
                .await
            {
                Ok(r) => return Ok(r),
                Err(e) => log::debug!("peer additions_and_removals failed: {e}"),
            }
        }
        // Fallback to coinset.
        if self.coinset_fallback_enabled {
            self.coinset.get_additions_and_removals(header_hash).await
        } else {
            Err(ChiaQueryError::UnsupportedWithoutCoinset(
                "get_additions_and_removals".into(),
            ))
        }
    }

    pub async fn get_block(&self, header_hash: &str) -> Result<FullBlock, ChiaQueryError> {
        // Resolve height from block record, try peer first.
        if let Ok(record) = self.get_block_record(header_hash).await {
            match self.peer.try_get_block_by_height(record.height).await {
                Ok(b) => return Ok(b),
                Err(e) => log::debug!("peer get_block failed: {e}"),
            }
        }
        if self.coinset_fallback_enabled {
            self.coinset.get_block(header_hash).await
        } else {
            Err(ChiaQueryError::UnsupportedWithoutCoinset(
                "get_block".into(),
            ))
        }
    }

    /// Fetch a full block by height.  Peer-backed via `RequestBlock`.
    pub async fn get_block_by_height(&self, height: u32) -> Result<FullBlock, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_block_by_height(height),
            self.peer.try_get_block_by_height(height),
            async {
                // Coinset has no direct by-height endpoint for full blocks, so
                // resolve the header_hash first.
                let record = self.coinset.get_block_record_by_height(height).await?;
                self.coinset.get_block(&record.header_hash).await
            },
        )
        .await
    }

    pub async fn get_block_count_metrics(&self) -> Result<BlockCountMetrics, ChiaQueryError> {
        self.require_coinset("get_block_count_metrics")?;
        self.coinset.get_block_count_metrics().await
    }

    pub async fn get_block_record(&self, header_hash: &str) -> Result<BlockRecord, ChiaQueryError> {
        self.require_coinset("get_block_record")?;
        self.coinset.get_block_record(header_hash).await
    }

    /// The block record at `height`, or an error when no source will second one.
    ///
    /// A fail-closed wrapper over
    /// [`get_block_record_by_height_opt`](Self::get_block_record_by_height_opt) rather than a
    /// second path to the same data: two readings of "is there a block here" would be a rival
    /// implementation, and the ungraded one is the one that would win the race.
    pub async fn get_block_record_by_height(
        &self,
        height: u32,
    ) -> Result<BlockRecord, ChiaQueryError> {
        self.get_block_record_by_height_opt(height)
            .await?
            .ok_or_else(|| {
                ChiaQueryError::PeerRejection(format!("no block record at height {height}"))
            })
    }

    pub async fn get_block_records(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Vec<BlockRecord>, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_block_records(start, end),
            self.peer.try_get_block_records(start, end),
            self.coinset.get_block_records(start, end),
        )
        .await
    }

    /// Peer-backed: fetches the full block, then runs the CLVM generator to
    /// extract every coin spend with its puzzle_reveal and solution.
    pub async fn get_block_spends(
        &self,
        header_hash: &str,
    ) -> Result<Vec<CoinSpend>, ChiaQueryError> {
        // Resolve height from block record.
        if let Ok(record) = self.get_block_record(header_hash).await {
            match self
                .peer
                .try_get_block_spends_by_height(record.height)
                .await
            {
                Ok(r) => return Ok(r),
                Err(e) => log::debug!("peer block_spends failed: {e}"),
            }
        }
        if self.coinset_fallback_enabled {
            self.coinset.get_block_spends(header_hash).await
        } else {
            Err(ChiaQueryError::UnsupportedWithoutCoinset(
                "get_block_spends".into(),
            ))
        }
    }

    /// Peer-backed: fetches full block, runs CLVM generator, then runs each
    /// puzzle(solution) to extract parsed conditions.
    pub async fn get_block_spends_with_conditions(
        &self,
        header_hash: &str,
    ) -> Result<Vec<CoinSpendWithConditions>, ChiaQueryError> {
        if let Ok(record) = self.get_block_record(header_hash).await {
            match self
                .peer
                .try_get_block_spends_with_conditions(record.height)
                .await
            {
                Ok(r) => return Ok(r),
                Err(e) => log::debug!("peer block_spends_with_conditions failed: {e}"),
            }
        }
        if self.coinset_fallback_enabled {
            self.coinset
                .get_block_spends_with_conditions(header_hash)
                .await
        } else {
            Err(ChiaQueryError::UnsupportedWithoutCoinset(
                "get_block_spends_with_conditions".into(),
            ))
        }
    }

    pub async fn get_blocks(
        &self,
        start: u32,
        end: u32,
        exclude_header_hash: bool,
        exclude_reorged: bool,
    ) -> Result<Vec<FullBlock>, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_blocks_range(start, end),
            self.peer.try_get_blocks_range(start, end),
            self.coinset
                .get_blocks(start, end, exclude_header_hash, exclude_reorged),
        )
        .await
    }

    pub async fn get_unfinished_block_headers(
        &self,
    ) -> Result<Vec<UnfinishedBlockHeader>, ChiaQueryError> {
        self.require_coinset("get_unfinished_block_headers")?;
        self.coinset.get_unfinished_block_headers().await
    }
}

// ---------------------------------------------------------------------------
// Coins (peer-backed with coinset fallback)
// ---------------------------------------------------------------------------

impl QueryRouter {
    pub async fn get_coin_record_by_name(&self, name: &str) -> Result<CoinRecord, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_coin_record_by_name(name),
            self.peer.try_get_coin_record_by_name(name),
            self.coinset.get_coin_record_by_name(name),
        )
        .await
    }

    /// Absence-aware [`get_coin_record_by_name`](Self::get_coin_record_by_name): a PROVABLE absence
    /// is `Ok(None)`, every transport/rejection/parse failure is `Err`. A successful peer or coinset
    /// response that reports no such coin is the authoritative absence; only when the peer read
    /// itself fails does the router fall back to coinset (SPEC §3).
    pub async fn get_coin_record_by_name_opt(
        &self,
        name: &str,
    ) -> Result<Option<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset_opt(
            self.peer.try_get_coin_record_by_name_opt(name),
            self.peer.try_get_coin_record_by_name_opt(name),
            self.coinset.get_coin_record_by_name_opt(name),
        )
        .await
    }

    /// Absence-aware read of the spend that spent `coin_id`: `Ok(None)` when the coin is provably
    /// unspent/unknown, `Err` when the read could not be completed.
    pub async fn get_coin_spend_opt(
        &self,
        coin_id: &str,
    ) -> Result<Option<CoinSpend>, ChiaQueryError> {
        self.peer_then_coinset_opt(
            self.peer.try_get_coin_spend_opt(coin_id),
            self.peer.try_get_coin_spend_opt(coin_id),
            self.coinset.get_puzzle_and_solution_opt(coin_id, None),
        )
        .await
    }

    /// The current peak height, or `Ok(None)` when no source exposes a peak; `Err` on failure.
    pub async fn peak_height_opt(&self) -> Result<Option<u32>, ChiaQueryError> {
        let state = self.get_blockchain_state().await?;
        Ok(state.peak.map(|p| p.height))
    }

    /// The Unix timestamp of the block at `height`: `Ok(None)` when no such block exists or the
    /// block carries no timestamp; `Err` on failure.
    pub async fn block_timestamp_opt(&self, height: u32) -> Result<Option<u64>, ChiaQueryError> {
        let record = self.get_block_record_by_height_opt(height).await?;
        Ok(record.and_then(|r| r.timestamp))
    }

    /// Absence-aware block-record read, GRADED.
    ///
    /// Used by [`block_timestamp_opt`](Self::block_timestamp_opt), which is the reason it is
    /// corroborated rather than verified: a header block's hash is recomputable from its own
    /// contents, but `timestamp` is foliage and is not — so hash verification would authenticate
    /// the block's name while leaving the field the caller actually reads on one peer's word.
    ///
    /// It was single-peer and ungraded until chia-query#35: a successful peer read was taken on
    /// ONE peer's word, and only a peer FAILURE fell through to coinset.
    pub async fn get_block_record_by_height_opt(
        &self,
        height: u32,
    ) -> Result<Option<BlockRecord>, ChiaQueryError> {
        self.peer_then_coinset_opt(
            self.peer.try_get_block_record_by_height_opt(height),
            self.peer.try_get_block_record_by_height_opt(height),
            self.coinset.get_block_record_by_height_opt(height),
        )
        .await
    }

    /// Every coin hinted at `hint`, GRADED, with the height the answer is true about.
    pub async fn get_coin_records_by_hint_graded(
        &self,
        hint: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };
        self.peer_then_coinset_set(
            self.peer.try_get_coin_records_by_hint(
                hint,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.peer.try_get_coin_records_by_hint(
                hint,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.coinset
                .get_coin_records_by_hint(hint, None, None, true),
            projection,
        )
        .await
    }

    /// Every coin hinted at `hint`.
    ///
    /// A fail-closed wrapper over
    /// [`get_coin_records_by_hint_graded`](Self::get_coin_records_by_hint_graded) that drops
    /// `as_of_height`. Prefer the graded twin when the answer's height matters — and it usually
    /// does, because this set is a true statement about that height rather than about the tip.
    pub async fn get_coin_records_by_hint(
        &self,
        hint: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_hint_graded(hint, start_height, end_height, include_spent_coins)
            .await?
            .items)
    }

    /// Every coin hinted at any of `hints`, GRADED, with the height the answer is true about.
    pub async fn get_coin_records_by_hints_graded(
        &self,
        hints: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };
        self.peer_then_coinset_set(
            self.peer.try_get_coin_records_by_hints(
                hints,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.peer.try_get_coin_records_by_hints(
                hints,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.coinset
                .get_coin_records_by_hints(hints, None, None, true),
            projection,
        )
        .await
    }

    /// Every coin hinted at any of `hints`. Fail-closed wrapper over the graded twin.
    pub async fn get_coin_records_by_hints(
        &self,
        hints: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_hints_graded(hints, start_height, end_height, include_spent_coins)
            .await?
            .items)
    }

    /// The coin records for `names`, GRADED, with the height the answer is true about.
    pub async fn get_coin_records_by_names_graded(
        &self,
        names: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };
        self.peer_then_coinset_set(
            self.peer.try_get_coin_records_by_names(
                names,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.peer.try_get_coin_records_by_names(
                names,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.coinset
                .get_coin_records_by_names(names, None, None, true),
            projection,
        )
        .await
    }

    /// The coin records for `names`. Fail-closed wrapper over the graded twin.
    ///
    /// The peer path used to ignore `start_height`, `end_height` and `include_spent_coins`
    /// entirely while the coinset path honoured them, so the same call returned different sets
    /// depending on which tier answered. Both tiers now go through one projection.
    pub async fn get_coin_records_by_names(
        &self,
        names: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_names_graded(names, start_height, end_height, include_spent_coins)
            .await?
            .items)
    }

    /// The children of every id in `parent_ids`, GRADED, with the height the answer is true about.
    ///
    /// Each parent is a separate graded round, and the combined answer is only as good as its
    /// weakest part: the reported `as_of_height` is the LOWEST of the rounds, so the whole set is
    /// dated by the oldest thing in it rather than by the freshest. Any round that disagrees fails
    /// the whole call — a partial answer here would be an omission wearing a success.
    pub async fn get_coin_records_by_parent_ids_graded(
        &self,
        parent_ids: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };

        let mut items = Vec::new();
        let mut as_of_height = u32::MAX;
        for parent_id in parent_ids {
            let answer = self
                .peer_then_coinset_set(
                    self.peer.try_get_children(
                        parent_id,
                        start_height,
                        end_height,
                        include_spent_coins,
                    ),
                    self.peer.try_get_children(
                        parent_id,
                        start_height,
                        end_height,
                        include_spent_coins,
                    ),
                    self.coinset.get_coin_records_by_parent_ids(
                        std::slice::from_ref(parent_id),
                        None,
                        None,
                        true,
                    ),
                    projection,
                )
                .await?;
            as_of_height = as_of_height.min(answer.as_of_height);
            items.extend(answer.items);
        }

        Ok(CorroboratedSet {
            items,
            // An empty `parent_ids` asks nothing, so there is no round to be dated by. Reporting
            // `u32::MAX` would claim the empty answer is true about a height nobody has reached.
            as_of_height: if parent_ids.is_empty() {
                0
            } else {
                as_of_height
            },
        })
    }

    /// The children of every id in `parent_ids`. Fail-closed wrapper over the graded twin.
    pub async fn get_coin_records_by_parent_ids(
        &self,
        parent_ids: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_parent_ids_graded(
                parent_ids,
                start_height,
                end_height,
                include_spent_coins,
            )
            .await?
            .items)
    }

    /// Every coin at `puzzle_hash`, GRADED, with the height the answer is true about.
    ///
    /// **This is the read a wallet balance and the collateral census are taken through.** It was
    /// ungraded until chia-query#35/#47 — whatever the first responsive source said was the
    /// answer — which made an omission free in exactly the direction that costs the network money
    /// (dig-node#405).
    ///
    /// `as_of_height` is not optional decoration. The set is a TRUE statement about the chain at
    /// that height and a FALSE one about the tip, and a consumer recording a balance or a census
    /// count needs to know which it has.
    pub async fn get_coin_records_by_puzzle_hash_graded(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };
        self.peer_then_coinset_set(
            self.peer.try_get_coin_records_by_puzzle_hash(
                puzzle_hash,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.peer.try_get_coin_records_by_puzzle_hash(
                puzzle_hash,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.coinset
                .get_coin_records_by_puzzle_hash(puzzle_hash, None, None, true),
            projection,
        )
        .await
    }

    /// Every coin at `puzzle_hash`.
    ///
    /// A fail-closed wrapper over
    /// [`get_coin_records_by_puzzle_hash_graded`](Self::get_coin_records_by_puzzle_hash_graded)
    /// that drops `as_of_height`. It keeps its signature and gains the
    /// [`SourcesDisagree`](ChiaQueryError::SourcesDisagree) and
    /// [`UncorroboratedPresence`](ChiaQueryError::UncorroboratedPresence) outcomes, which mean
    /// UNKNOWN and should be retried — never an empty or short set.
    ///
    /// Leaving this one on the ungraded path would have left the hole open under the name every
    /// consumer already calls.
    pub async fn get_coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_puzzle_hash_graded(
                puzzle_hash,
                start_height,
                end_height,
                include_spent_coins,
            )
            .await?
            .items)
    }

    /// Every coin at any of `puzzle_hashes`, GRADED, with the height the answer is true about.
    pub async fn get_coin_records_by_puzzle_hashes_graded(
        &self,
        puzzle_hashes: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<CorroboratedSet<CoinRecord>, ChiaQueryError> {
        let projection = SetProjection {
            start_height,
            end_height,
            include_spent: include_spent_coins,
        };
        self.peer_then_coinset_set(
            self.peer.try_get_coin_records_by_puzzle_hashes(
                puzzle_hashes,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.peer.try_get_coin_records_by_puzzle_hashes(
                puzzle_hashes,
                start_height,
                end_height,
                include_spent_coins,
            ),
            self.coinset
                .get_coin_records_by_puzzle_hashes(puzzle_hashes, None, None, true),
            projection,
        )
        .await
    }

    /// Every coin at any of `puzzle_hashes`. Fail-closed wrapper over the graded twin.
    pub async fn get_coin_records_by_puzzle_hashes(
        &self,
        puzzle_hashes: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        Ok(self
            .get_coin_records_by_puzzle_hashes_graded(
                puzzle_hashes,
                start_height,
                end_height,
                include_spent_coins,
            )
            .await?
            .items)
    }

    /// No peer equivalent -- always coinset.
    pub async fn get_memos_by_coin_name(&self, name: &str) -> Result<Value, ChiaQueryError> {
        self.require_coinset("get_memos_by_coin_name")?;
        self.coinset.get_memos_by_coin_name(name).await
    }

    /// The spend that spent `coin_id`, or an error when no source will second one.
    ///
    /// Graded by the scalar corroboration path on both branches (chia-query#35). It was
    /// single-peer and ungraded before: whichever peer answered first decided what program had
    /// run.
    ///
    /// With no `height`, the read goes through
    /// [`get_coin_spend_opt`](Self::get_coin_spend_opt), which resolves the spent height from a
    /// graded coin-state read and verifies the puzzle reveal against the coin's own puzzle hash.
    /// Routing it to the peer tier's `_auto` form instead would be a second, ungraded path to the
    /// same answer.
    pub async fn get_puzzle_and_solution(
        &self,
        coin_id: &str,
        height: Option<u32>,
    ) -> Result<CoinSpend, ChiaQueryError> {
        let spend = if let Some(h) = height {
            self.peer_then_coinset_opt(
                self.peer.try_get_puzzle_and_solution(coin_id, h),
                self.peer.try_get_puzzle_and_solution(coin_id, h),
                self.coinset.get_puzzle_and_solution_opt(coin_id, height),
            )
            .await?
        } else {
            self.get_coin_spend_opt(coin_id).await?
        };

        spend.ok_or_else(|| {
            ChiaQueryError::PeerRejection(format!(
                "no spend for coin {coin_id}: it is unknown or unspent"
            ))
        })
    }

    /// Peer-backed: get puzzle & solution, then run puzzle(solution) to extract
    /// parsed conditions.
    pub async fn get_puzzle_and_solution_with_conditions(
        &self,
        coin_id: &str,
        height: Option<u32>,
    ) -> Result<CoinSpendWithConditions, ChiaQueryError> {
        // Try to get the spend via peer first.
        let spend = match self.get_puzzle_and_solution(coin_id, height).await {
            Ok(s) => s,
            Err(_) => {
                if self.coinset_fallback_enabled {
                    return self
                        .coinset
                        .get_puzzle_and_solution_with_conditions(coin_id, height)
                        .await;
                }
                return Err(ChiaQueryError::PeerRejection(
                    "could not retrieve puzzle and solution".into(),
                ));
            }
        };

        // Run puzzle(solution) to extract conditions.
        let conditions = run_puzzle_conditions(&spend, self.peer.constants());
        Ok(CoinSpendWithConditions {
            coin_spend: spend,
            conditions,
        })
    }

    pub async fn push_tx(&self, bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_push_tx(bundle),
            self.peer.try_push_tx(bundle),
            self.coinset.push_tx(bundle),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Fees (peer-backed with coinset fallback)
// ---------------------------------------------------------------------------

impl QueryRouter {
    pub async fn get_fee_estimate(
        &self,
        spend_bundle: Option<&SpendBundle>,
        target_times: Option<&[u64]>,
        spend_count: Option<u64>,
    ) -> Result<FeeEstimate, ChiaQueryError> {
        let times = target_times.unwrap_or(&[60, 120, 300]);
        self.peer_then_coinset(
            self.peer.try_get_fee_estimate(times),
            self.peer.try_get_fee_estimate(times),
            self.coinset
                .get_fee_estimate(spend_bundle, target_times, spend_count),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Full node / network (all coinset-only)
// ---------------------------------------------------------------------------

impl QueryRouter {
    /// Peer-backed: derived from the chia consensus constants for the
    /// configured network.
    pub async fn get_aggsig_additional_data(&self) -> Result<String, ChiaQueryError> {
        Ok(self.peer.aggsig_additional_data())
    }

    /// Peer-backed: derived from the chia consensus constants for the
    /// configured network.
    pub async fn get_network_info(&self) -> Result<NetworkInfo, ChiaQueryError> {
        Ok(self.peer.network_info())
    }

    /// Peer-backed partially: peak height is tracked from `NewPeakWallet`
    /// messages received from peers.  Full state comes from coinset.
    pub async fn get_blockchain_state(&self) -> Result<BlockchainState, ChiaQueryError> {
        // Try coinset first for full state.
        if self.coinset_fallback_enabled {
            if let Ok(state) = self.coinset.get_blockchain_state().await {
                return Ok(state);
            }
        }
        // Fallback: return a minimal state from the peer-tracked peak.
        let peak = self.peer.peak_height().await;
        if peak == 0 {
            return Err(ChiaQueryError::PeerConnection(
                "no peak observed from peers yet".into(),
            ));
        }
        Ok(BlockchainState {
            peak: Some(BlockRecord {
                height: peak,
                ..Default::default()
            }),
            sync: Some(SyncState {
                synced: true,
                sync_mode: false,
                sync_progress_height: peak,
                sync_tip_height: peak,
            }),
            ..Default::default()
        })
    }

    pub async fn get_network_space(
        &self,
        newer_block_header_hash: &str,
        older_block_header_hash: &str,
    ) -> Result<u64, ChiaQueryError> {
        self.require_coinset("get_network_space")?;
        self.coinset
            .get_network_space(newer_block_header_hash, older_block_header_hash)
            .await
    }
}

// ---------------------------------------------------------------------------
// Mempool (all coinset-only)
// ---------------------------------------------------------------------------

impl QueryRouter {
    pub async fn get_all_mempool_items(
        &self,
    ) -> Result<HashMap<String, MempoolItem>, ChiaQueryError> {
        self.require_coinset("get_all_mempool_items")?;
        self.coinset.get_all_mempool_items().await
    }

    pub async fn get_all_mempool_tx_ids(&self) -> Result<Vec<String>, ChiaQueryError> {
        self.require_coinset("get_all_mempool_tx_ids")?;
        self.coinset.get_all_mempool_tx_ids().await
    }

    pub async fn get_mempool_item_by_tx_id(
        &self,
        tx_id: &str,
    ) -> Result<MempoolItem, ChiaQueryError> {
        self.require_coinset("get_mempool_item_by_tx_id")?;
        self.coinset.get_mempool_item_by_tx_id(tx_id).await
    }

    pub async fn get_mempool_items_by_coin_name(
        &self,
        coin_name: &str,
        include_spent_coins: Option<bool>,
    ) -> Result<Vec<MempoolItem>, ChiaQueryError> {
        self.require_coinset("get_mempool_items_by_coin_name")?;
        self.coinset
            .get_mempool_items_by_coin_name(coin_name, include_spent_coins)
            .await
    }
}
