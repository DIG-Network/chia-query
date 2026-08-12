//! QueryRouter -- dispatches each request to the peer backend first (with one
//! retry on a different peer) and falls back to the coinset.org HTTP API if
//! both peer attempts fail.

use std::collections::HashMap;

use chia_consensus::consensus_constants::ConsensusConstants;
use chia_consensus::flags::DONT_VALIDATE_SIGNATURE;
use serde_json::Value;

use crate::coinset::CoinsetClient;
use crate::peer::PeerBackend;
use crate::types::*;

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

pub struct QueryRouter {
    pub(crate) peer: PeerBackend,
    pub(crate) coinset: CoinsetClient,
    pub(crate) coinset_fallback_enabled: bool,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Dispatch one read across the peer tier and the coinset fallback.
///
/// A free function, and generic over the whole result type, because it is the one place the
/// order of sources is decided and both [`QueryRouter::peer_then_coinset`] and its absence-aware
/// sibling are that same decision — the `Option` variant differs only in what `T` is. Written as
/// a method it would need a router, which needs a live `PeerBackend`, which needs a socket; as a
/// free function over plain flags the ordering itself is directly testable.
///
/// `peer_reachable` is false when the pool holds nothing AND there is somewhere else to ask. In
/// that case NEITHER peer future is awaited: they are dropped un-polled, so no peer is selected
/// and no refill is waited on. Every other case is the historical behaviour — one peer attempt,
/// one retry on a different peer, then the fallback.
async fn dispatch_read<T>(
    peer_reachable: bool,
    coinset_fallback_enabled: bool,
    peer_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
    peer_retry: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
    coinset_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
) -> Result<T, ChiaQueryError> {
    if !peer_reachable {
        // No peer error to report: no peer was asked. Reporting the coinset error alone is the
        // honest account of what was attempted.
        return coinset_fn.await;
    }

    // First peer attempt
    match peer_fn.await {
        Ok(v) => return Ok(v),
        Err(e) => log::debug!("peer attempt 1 failed: {e}"),
    }

    // Retry on a different peer
    match peer_retry.await {
        Ok(v) => Ok(v),
        Err(peer_err) => {
            if !coinset_fallback_enabled {
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

impl QueryRouter {
    /// Whether this read should consult the peer tier at all, starting a detached refill if not.
    ///
    /// An EMPTY pool refills IN FRONT of the read that found it empty, which costs that read up
    /// to two connect-timeout sweeps for a decentralized answer the coinset tier would have given
    /// in milliseconds — and on the one read whose fill is least likely to succeed. Frictionless
    /// consumption settles it: when a fallback is configured the refill is detached and the read
    /// goes straight to the fallback, and because the fill still happens the NEXT read is
    /// peer-served.
    ///
    /// With NO fallback configured there is nothing to be fast with, so the read keeps waiting on
    /// the fill — an answer late beats no answer.
    async fn peer_tier_is_worth_waiting_for(&self) -> bool {
        if !self.coinset_fallback_enabled || self.peer.has_peers().await {
            return true;
        }
        self.peer.try_refill_detached();
        false
    }

    /// Try `peer_fn` twice (each call will select a different peer because the
    /// first failure ejects the peer).  If both fail, fall back to `coinset_fn`.
    async fn peer_then_coinset<T>(
        &self,
        peer_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
        peer_retry: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
        coinset_fn: impl std::future::Future<Output = Result<T, ChiaQueryError>>,
    ) -> Result<T, ChiaQueryError> {
        dispatch_read(
            self.peer_tier_is_worth_waiting_for().await,
            self.coinset_fallback_enabled,
            peer_fn,
            peer_retry,
            coinset_fn,
        )
        .await
    }

    /// Absence-aware variant of [`peer_then_coinset`](Self::peer_then_coinset).
    ///
    /// A successful peer response — whether `Some` (found) or `None` (provable absence) — is
    /// authoritative and returned immediately. Only a peer FAILURE falls through to the retry and
    /// then the coinset fallback, so a genuine absence is never masked by a fallback and a failure
    /// is never collapsed into `Ok(None)` (SPEC §3).
    async fn peer_then_coinset_opt<T>(
        &self,
        peer_fn: impl std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
        peer_retry: impl std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
        coinset_fn: impl std::future::Future<Output = Result<Option<T>, ChiaQueryError>>,
    ) -> Result<Option<T>, ChiaQueryError> {
        dispatch_read(
            self.peer_tier_is_worth_waiting_for().await,
            self.coinset_fallback_enabled,
            peer_fn,
            peer_retry,
            coinset_fn,
        )
        .await
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

    /// Peer-backed via `RequestBlockHeader` / `RespondBlockHeader` (pattern
    /// from chia-block-listener).
    pub async fn get_block_record_by_height(
        &self,
        height: u32,
    ) -> Result<BlockRecord, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_block_record_by_height(height),
            self.peer.try_get_block_record_by_height(height),
            self.coinset.get_block_record_by_height(height),
        )
        .await
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

    /// Absence-aware block-record read used by [`block_timestamp_opt`](Self::block_timestamp_opt).
    async fn get_block_record_by_height_opt(
        &self,
        height: u32,
    ) -> Result<Option<BlockRecord>, ChiaQueryError> {
        // A successful peer read is authoritative; only a peer FAILURE falls through to coinset,
        // whose null block_record is provable absence.
        if let Ok(record) = self.peer.try_get_block_record_by_height(height).await {
            return Ok(Some(record));
        }
        match self.peer.try_get_block_record_by_height(height).await {
            Ok(record) => Ok(Some(record)),
            Err(peer_err) => {
                if self.coinset_fallback_enabled {
                    self.coinset
                        .get_block_record_by_height_opt(height)
                        .await
                        .map_err(|ce| ChiaQueryError::AllSourcesFailed {
                            peer_error: Box::new(peer_err),
                            coinset_error: Some(Box::new(ce)),
                        })
                } else {
                    Err(peer_err)
                }
            }
        }
    }

    pub async fn get_coin_records_by_hint(
        &self,
        hint: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset(
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
            self.coinset.get_coin_records_by_hint(
                hint,
                start_height,
                end_height,
                include_spent_coins,
            ),
        )
        .await
    }

    pub async fn get_coin_records_by_hints(
        &self,
        hints: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset(
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
            self.coinset.get_coin_records_by_hints(
                hints,
                start_height,
                end_height,
                include_spent_coins,
            ),
        )
        .await
    }

    pub async fn get_coin_records_by_names(
        &self,
        names: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset(
            self.peer.try_get_coin_records_by_names(names),
            self.peer.try_get_coin_records_by_names(names),
            self.coinset.get_coin_records_by_names(
                names,
                start_height,
                end_height,
                include_spent_coins,
            ),
        )
        .await
    }

    /// Peer-backed via `RequestChildren` / `RespondChildren` which returns
    /// child coin states for a given parent coin ID.  Falls back to coinset
    /// for batched queries or when peers fail.
    pub async fn get_coin_records_by_parent_ids(
        &self,
        parent_ids: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        // Try peer: query each parent ID via RequestChildren, combine results.
        let peer_attempt = async {
            let mut all_records = Vec::new();
            for parent_id in parent_ids {
                let children = self.peer.try_get_children(parent_id).await?;
                all_records.extend(children);
            }
            // Apply client-side height and spent filters.
            all_records.retain(|r| {
                let height_ok = match (start_height, end_height) {
                    (Some(s), Some(e)) => {
                        r.confirmed_block_index >= s && r.confirmed_block_index <= e
                    }
                    (Some(s), None) => r.confirmed_block_index >= s,
                    (None, Some(e)) => r.confirmed_block_index <= e,
                    (None, None) => true,
                };
                let spent_ok = include_spent_coins || !r.spent;
                height_ok && spent_ok
            });
            Ok(all_records)
        };

        match peer_attempt.await {
            Ok(r) => Ok(r),
            Err(peer_err) => {
                if self.coinset_fallback_enabled {
                    self.coinset
                        .get_coin_records_by_parent_ids(
                            parent_ids,
                            start_height,
                            end_height,
                            include_spent_coins,
                        )
                        .await
                        .map_err(|ce| ChiaQueryError::AllSourcesFailed {
                            peer_error: Box::new(peer_err),
                            coinset_error: Some(Box::new(ce)),
                        })
                } else {
                    Err(peer_err)
                }
            }
        }
    }

    pub async fn get_coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset(
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
            self.coinset.get_coin_records_by_puzzle_hash(
                puzzle_hash,
                start_height,
                end_height,
                include_spent_coins,
            ),
        )
        .await
    }

    pub async fn get_coin_records_by_puzzle_hashes(
        &self,
        puzzle_hashes: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.peer_then_coinset(
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
            self.coinset.get_coin_records_by_puzzle_hashes(
                puzzle_hashes,
                start_height,
                end_height,
                include_spent_coins,
            ),
        )
        .await
    }

    /// No peer equivalent -- always coinset.
    pub async fn get_memos_by_coin_name(&self, name: &str) -> Result<Value, ChiaQueryError> {
        self.require_coinset("get_memos_by_coin_name")?;
        self.coinset.get_memos_by_coin_name(name).await
    }

    pub async fn get_puzzle_and_solution(
        &self,
        coin_id: &str,
        height: Option<u32>,
    ) -> Result<CoinSpend, ChiaQueryError> {
        if let Some(h) = height {
            self.peer_then_coinset(
                self.peer.try_get_puzzle_and_solution(coin_id, h),
                self.peer.try_get_puzzle_and_solution(coin_id, h),
                self.coinset.get_puzzle_and_solution(coin_id, height),
            )
            .await
        } else {
            // No height provided -- peer can resolve it via coin state.
            self.peer_then_coinset(
                self.peer.try_get_puzzle_and_solution_auto(coin_id),
                self.peer.try_get_puzzle_and_solution_auto(coin_id),
                self.coinset.get_puzzle_and_solution(coin_id, None),
            )
            .await
        }
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
        let peak = self.peer.peak_height();
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

// ---------------------------------------------------------------------------
// Source-order tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A source that records that it was ASKED, distinct from what it answered.
    ///
    /// The property under test is which sources a read consults, and a source that only reports
    /// its answer cannot express "never asked" — the exact state the short-circuit creates.
    /// Rust futures are lazy, so a future that is built and dropped un-awaited never increments.
    #[derive(Default)]
    struct Calls(AtomicUsize);

    impl Calls {
        async fn answering(&self, value: u8) -> Result<u8, ChiaQueryError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(value)
        }

        async fn failing(&self) -> Result<u8, ChiaQueryError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(ChiaQueryError::PeerConnection("no peers available".into()))
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// With nothing in the pool and a fallback configured, the peer tier is not consulted AT ALL.
    ///
    /// Not merely "the answer came from coinset": both peer futures had to go un-awaited. Awaiting
    /// them selects a peer, which on an empty pool runs a refill IN FRONT of the read — up to two
    /// connect-timeout sweeps, twice over, for an answer the fallback gives in milliseconds. The
    /// zero is what distinguishes the short-circuit from a peer attempt that merely failed fast.
    #[tokio::test]
    async fn an_empty_pool_does_not_delay_a_fallback_read() {
        let (peer, retry, coinset) = (Calls::default(), Calls::default(), Calls::default());

        let answer = dispatch_read(
            false,
            true,
            peer.failing(),
            retry.failing(),
            coinset.answering(7),
        )
        .await;

        assert_eq!(answer.expect("the fallback answered"), 7);
        assert_eq!(
            peer.count(),
            0,
            "the peer tier was consulted on an empty pool"
        );
        assert_eq!(retry.count(), 0, "and retried on it");
        assert_eq!(coinset.count(), 1, "the fallback answered exactly once");
    }

    /// The control: with a peer available, both peer attempts run before the fallback does.
    ///
    /// Without this the short-circuit above is satisfied by a router that never asks a peer
    /// anything, which would delete the peer tier rather than reorder it.
    #[tokio::test]
    async fn a_reachable_peer_tier_is_still_tried_first_and_twice() {
        let (peer, retry, coinset) = (Calls::default(), Calls::default(), Calls::default());

        let answer = dispatch_read(
            true,
            true,
            peer.failing(),
            retry.failing(),
            coinset.answering(7),
        )
        .await;

        assert_eq!(answer.expect("the fallback answered"), 7);
        assert_eq!(peer.count(), 1, "the first peer attempt did not run");
        assert_eq!(retry.count(), 1, "the retry did not run");
        assert_eq!(coinset.count(), 1, "and the fallback ran after both");
    }

    /// The other control: with NO fallback configured there is nothing to be fast with, so the
    /// read keeps waiting on the peer tier however empty the pool is.
    ///
    /// `QueryRouter::peer_tier_is_worth_waiting_for` never reports the tier unreachable in that
    /// configuration; this pins the behaviour `dispatch_read` must have when it does not.
    #[tokio::test]
    async fn without_a_fallback_the_peer_tier_is_always_waited_on() {
        let (peer, retry, coinset) = (Calls::default(), Calls::default(), Calls::default());

        let answer = dispatch_read(
            true,
            false,
            peer.failing(),
            retry.answering(9),
            coinset.answering(7),
        )
        .await;

        assert_eq!(answer.expect("the retry answered"), 9);
        assert_eq!(peer.count(), 1, "the first peer attempt did not run");
        assert_eq!(
            coinset.count(),
            0,
            "the fallback answered a read it was not needed for"
        );
    }
}
