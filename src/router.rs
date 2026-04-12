//! QueryRouter -- dispatches each request to the peer backend first (with one
//! retry on a different peer) and falls back to the coinset.org HTTP API if
//! both peer attempts fail.

use std::collections::HashMap;

use chia::consensus::consensus_constants::ConsensusConstants;
use chia::consensus::flags::DONT_VALIDATE_SIGNATURE;
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

    let mut allocator = chia::consensus::allocator::make_allocator(flags);

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
