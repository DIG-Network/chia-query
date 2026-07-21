//! [`ChiaQueryProvider`] — a synchronous [`ChainSource`] facade over the asynchronous
//! [`ChiaQuery`](crate::ChiaQuery) router.
//!
//! It bridges each sync read to the async router with [`run_blocking`](super::bridge::run_blocking)
//! (which fails closed with a clear error on a current-thread runtime — SPEC §7), maps the router's
//! outcome onto the fail-closed `Ok(None)`-vs-`Err` contract via
//! [`convert`](super::convert), and walks singleton lineages with
//! [`walk_singleton_lineage`](super::lineage_walk::walk_singleton_lineage).
//!
//! ## Runtime requirement (SPEC §7)
//!
//! The facade MUST run on a **multi-thread** tokio runtime. Building it there and calling it from
//! synchronous code is the intended pattern; an async consumer must instead wrap each call in
//! [`tokio::task::spawn_blocking`] so the blocking read never runs on an async worker thread.

use std::sync::Arc;

use chia_protocol::{Bytes32, CoinSpend};
use dig_chainsource_interface::{
    ChainSource, ChainSourceError, ChainSourceProvider, CoinRecord, ProviderInfo, SingletonLineage,
};
use tokio::runtime::Handle;

use super::bridge::run_blocking;
use super::convert::{bytes32_to_hex, coin_record_from_chq, coin_spend_from_chq, map_query_error};
use super::lineage_walk::{singleton_child_from_spend, walk_singleton_lineage};
use crate::ChiaQuery;

/// A [`ChainSource`] provider backed by chia-query's async peer+coinset router.
///
/// Cloning shares the underlying [`ChiaQuery`] and runtime handle.
#[derive(Clone)]
pub struct ChiaQueryProvider {
    inner: Arc<ChiaQuery>,
    handle: Handle,
    info: ProviderInfo,
}

impl ChiaQueryProvider {
    /// Builds a provider over `inner`, driving its async reads on `handle` (which MUST belong to a
    /// multi-thread runtime — see the module docs), and describing itself with `info`.
    pub fn new(inner: Arc<ChiaQuery>, handle: Handle, info: ProviderInfo) -> Self {
        Self {
            inner,
            handle,
            info,
        }
    }
}

impl ChainSource for ChiaQueryProvider {
    type Error = ChainSourceError;

    fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
        let name = bytes32_to_hex(coin_id);
        let record = run_blocking(&self.handle, self.inner.get_coin_record_by_name_opt(&name))?
            .map_err(map_query_error)?;
        record.as_ref().map(coin_record_from_chq).transpose()
    }

    fn coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: Bytes32,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        let hash = bytes32_to_hex(puzzle_hash);
        let records = run_blocking(
            &self.handle,
            self.inner
                .get_coin_records_by_puzzle_hash(&hash, None, None, include_spent),
        )?
        .map_err(map_query_error)?;
        records.iter().map(coin_record_from_chq).collect()
    }

    fn coin_records_by_parent(
        &self,
        parent_coin_id: Bytes32,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        // The interface's `coin_records_by_parent` wants every child, so include spent coins.
        let parent_ids = [bytes32_to_hex(parent_coin_id)];
        let records = run_blocking(
            &self.handle,
            self.inner
                .get_coin_records_by_parent_ids(&parent_ids, None, None, true),
        )?
        .map_err(map_query_error)?;
        records.iter().map(coin_record_from_chq).collect()
    }

    fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
        let id = bytes32_to_hex(coin_id);
        let spend = run_blocking(&self.handle, self.inner.get_coin_spend_opt(&id))?
            .map_err(map_query_error)?;
        spend.as_ref().map(coin_spend_from_chq).transpose()
    }

    fn resolve_singleton_lineage(
        &self,
        launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        let inner = self.inner.clone();
        let walk = async move {
            walk_singleton_lineage(
                launcher_id,
                |coin_id| {
                    let inner = inner.clone();
                    async move {
                        let id = bytes32_to_hex(coin_id);
                        match inner.get_coin_spend_opt(&id).await {
                            Ok(Some(spend)) => coin_spend_from_chq(&spend).map(Some),
                            Ok(None) => Ok(None),
                            Err(error) => Err(map_query_error(error)),
                        }
                    }
                },
                move |spend| singleton_child_from_spend(spend, launcher_id),
            )
            .await
        };
        run_blocking(&self.handle, walk)?
    }

    fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
        run_blocking(&self.handle, self.inner.peak_height_opt())?.map_err(map_query_error)
    }

    fn block_timestamp(&self, height: u32) -> Result<Option<u64>, Self::Error> {
        run_blocking(&self.handle, self.inner.block_timestamp_opt(height))?.map_err(map_query_error)
    }
}

impl ChainSourceProvider for ChiaQueryProvider {
    fn provider_info(&self) -> ProviderInfo {
        self.info.clone()
    }
}
