//! [`ChiaQueryProvider`] — a synchronous [`ChainSource`] facade over the asynchronous
//! [`ChiaQuery`](crate::ChiaQuery) router.
//!
//! It bridges each sync read to the async router with [`run_blocking`](super::bridge::run_blocking)
//! (which fails closed with a clear error on a current-thread runtime — SPEC §7), maps the router's
//! outcome onto the fail-closed `Ok(None)`-vs-`Err` contract via
//! [`convert`](super::convert), and delegates singleton-lineage resolution WHOLE to the canonical
//! [`resolve_singleton_lineage_via_walk`] -- this crate keeps no walk of its own (#28).
//!
//! ## Runtime requirement (SPEC §7)
//!
//! The facade MUST run on a **multi-thread** tokio runtime. Building it there and calling it from
//! synchronous code is the intended pattern; an async consumer must instead wrap each call in
//! [`tokio::task::spawn_blocking`] so the blocking read never runs on an async worker thread.

use std::sync::Arc;

use chia_protocol::{Bytes32, CoinSpend};
use dig_chainsource_interface::{
    resolve_singleton_lineage_via_walk, ChainSource, ChainSourceError, ChainSourceProvider,
    CoinRecord, ProviderInfo, SingletonLineage,
};
use tokio::runtime::Handle;

use super::bridge::run_blocking;
use super::convert::{bytes32_to_hex, coin_record_from_chq, coin_spend_from_chq, map_query_error};
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

    /// The independence group this provider must be registered under, derived from the fabric the
    /// [`ChiaQuery`] behind it can REACH.
    ///
    /// Delegates to [`ChiaQuery::independence_group`], which records why the derivation is
    /// conservative and why a literal at the registration site is the defect this replaces
    /// (dig-node#354). Pass this to
    /// [`ProviderRegistry::register`](super::ProviderRegistry::register).
    pub fn independence_group(&self) -> &'static str {
        self.inner.independence_group()
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

    /// The ONE canonical launcher -> tip singleton walk, delegated whole to
    /// [`dig_chainsource_interface`] (feature `lineage-walk`).
    ///
    /// # Why this is a delegation and not an implementation
    ///
    /// This method's result IS the authority set consumers test membership against, so a
    /// second hand-rolled copy of the walk is a byte-drift bug on a money path. chia-query kept one
    /// until #28: it was driven by a spend-fetcher alone, and [`ChainSource::coin_spend`] answers
    /// `Ok(None)` for "unspent **OR unknown**". Having no other read, that walk could not tell the
    /// two apart and returned a DERIVED successor the chain may never have created as the
    /// authenticated live tip. The check that closes it -- requiring each derived successor to have
    /// a coin record -- was not merely omitted, it was inexpressible in that seam.
    ///
    /// The canonical walk reads `coin_record` as well as `coin_spend`, so it fails closed (NC-9:
    /// chain-anchored data needs on-chain proof), bounds each hop's CLVM evaluation at
    /// [`MAX_HOP_CLVM_COST`](dig_chainsource_interface::MAX_HOP_CLVM_COST) rather than a whole
    /// block, and seeds the member set with the launcher so `contains(launcher_id)` answers the same
    /// here as it does for every other provider.
    ///
    /// # Why the synchronous walk is sound behind this async-backed facade
    ///
    /// The walk drives `self`, whose reads each bridge to the router through
    /// [`run_blocking`]. Those calls are SEQUENTIAL, not nested: this method is itself synchronous
    /// and holds no `block_on` of its own, so each read enters and leaves the runtime cleanly. The
    /// per-walk wall-clock budget is `dig_chainsource_interface`'s
    /// [`DEFAULT_WALK_BUDGET`](dig_chainsource_interface::DEFAULT_WALK_BUDGET), checked between
    /// hops; time spent INSIDE one hop stays bounded by the router's own per-request timeouts.
    fn resolve_singleton_lineage(
        &self,
        launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        resolve_singleton_lineage_via_walk(self, launcher_id)
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
