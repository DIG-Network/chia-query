//! [`CoinsetChainSource`] — a lightweight, no-handshake [`ChainSource`] served entirely by the
//! coinset.org HTTP tier, plus the [`CoinsetProvider::from_url`] / [`CoinsetProvider::from_env`]
//! constructors that wrap it.
//!
//! ## Why this exists (#1354)
//!
//! chia-query's full [`ChiaQueryProvider`](super::ChiaQueryProvider) races decentralized Chia peers
//! against the coinset fallback, so constructing it needs a LIVE peer handshake + TLS certs — far
//! too heavy for a consumer that only needs coinset HTTP point-reads (a coin record, a coin spend,
//! the peak height, a block timestamp). This source constructs from JUST a coinset base URL: no
//! Chia peer, no certificate, no sync. It owns its own multi-thread tokio runtime so a synchronous
//! consumer can build it and call it directly.
//!
//! ## Trust posture
//!
//! coinset.org is a single public oracle that can lie, so [`CoinsetProvider`] labels this source
//! [`ProviderKind::PublicOracle`](dig_chainsource_interface::ProviderKind::PublicOracle) with
//! `trustless: false`. The registry's operator-assigned trust + quorum — never this source alone —
//! gates custody.
//!
//! ## What it serves vs. refuses (fail-closed)
//!
//! Every point-read maps to one coinset REST round-trip and preserves the money-critical
//! `Ok(None)`-vs-`Err` contract (SPEC §3): a provable absence is `Ok(None)`, a transport/parse
//! failure is `Err` — never collapsed into a false absence.
//!
//! [`resolve_singleton_lineage`](ChainSource::resolve_singleton_lineage) is deliberately
//! [`Unsupported`](dig_chainsource_interface::ChainSourceError::Unsupported): a genuine forward walk
//! launcher → tip needs the CLVM singleton-shape machinery (per hop) that would make this "point-read"
//! source anything but lightweight. A consumer needing lineage uses the full
//! [`ChiaQueryProvider`](super::ChiaQueryProvider), or composes
//! [`parent_spend`](ChainSource::parent_spend) walks itself. This is a fail-closed `Err`, not a false
//! `Ok(None)`.

use std::future::Future;
use std::sync::Arc;

use chia_protocol::{Bytes32, CoinSpend};
use dig_chainsource_interface::{ChainSource, ChainSourceError, CoinRecord, SingletonLineage};
use tokio::runtime::Runtime;

use super::bridge::run_blocking;
use super::convert::{bytes32_to_hex, coin_record_from_chq, coin_spend_from_chq, map_query_error};
use super::providers::CoinsetProvider;
use crate::coinset::transport::HttpTransport;
use crate::coinset::CoinsetClient;

/// The canonical coinset.org base URL — the ecosystem's default chain-read tier (the same host the
/// full router and the drift monitor use). Overridable via [`CoinsetChainSource::from_env`] or an
/// explicit [`from_url`](CoinsetChainSource::from_url).
pub const DEFAULT_COINSET_URL: &str = "https://api.coinset.org";

/// The environment variable that overrides the coinset base URL (canonical: the chain-read tier's
/// `$DIG_COINSET_URL` / `--coinset-url` override, distinct from the §5.3 content-read ladder).
pub const COINSET_URL_ENV: &str = "DIG_COINSET_URL";

/// The default per-request timeout for the lightweight source, matching the full router's coinset
/// timeout so behaviour is consistent across chia-query's two coinset paths.
#[cfg(feature = "native")]
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The try-order priority [`CoinsetProvider::from_url`] registers coinset at — a public oracle is
/// tried after an operator's local node (priority 0) and any DIG-peers tier, matching how the
/// ecosystem orders the coinset fallback last.
const COINSET_PROVIDER_PRIORITY: i32 = 30;

/// The upper bound on coin records the source accepts from a single list read. A misbehaving or
/// hostile coinset endpoint could answer a puzzle-hash/parent query with an unbounded list; capping
/// it fails closed ([`ChainSourceError::TooManyRecords`]) rather than letting the record count drive
/// unbounded DOWNSTREAM work. This record cap is complementary to — not a substitute for — the
/// transport-level byte cap (`MAX_RESPONSE_BYTES` in [`crate::coinset::transport`]), which bounds the
/// RECEIVE/PARSE peak by rejecting an over-large body before it is fully buffered and deserialized;
/// this cap then bounds the work done on the records that survive that parse.
const MAX_COIN_RECORDS: usize = 100_000;

/// A synchronous, no-handshake [`ChainSource`] served entirely by coinset.org HTTP.
///
/// Generic over the [`HttpTransport`] so production uses the native `reqwest` transport while tests
/// inject a mock; [`from_url`](Self::from_url) / [`from_env`](Self::from_env) build the native
/// variant. It owns its runtime, so cloning shares one runtime + client via [`Arc`].
#[derive(Clone)]
pub struct CoinsetChainSource<T: HttpTransport> {
    client: Arc<CoinsetClient<T>>,
    runtime: Arc<Runtime>,
}

#[cfg(feature = "native")]
impl CoinsetChainSource<crate::coinset::transport::ReqwestTransport> {
    /// Builds a lightweight coinset source against `coinset_url`, with NO peer handshake or certs.
    ///
    /// Fails closed with [`ChainSourceError::Transport`] if the HTTP client or the owned runtime
    /// cannot be constructed.
    pub fn from_url(coinset_url: &str) -> Result<Self, ChainSourceError> {
        let client = CoinsetClient::new(coinset_url, DEFAULT_REQUEST_TIMEOUT)
            .map_err(|e| ChainSourceError::Transport(e.to_string()))?;
        Self::with_client(client)
    }

    /// Builds a lightweight coinset source from the environment: `$DIG_COINSET_URL` when set and
    /// non-empty, else [`DEFAULT_COINSET_URL`].
    pub fn from_env() -> Result<Self, ChainSourceError> {
        Self::from_url(&coinset_url_from_env())
    }
}

impl<T: HttpTransport> CoinsetChainSource<T> {
    /// Builds a source from an already-constructed [`CoinsetClient`], provisioning the owned
    /// multi-thread runtime the sync facade blocks on. Used by [`from_url`](Self::from_url) and, in
    /// tests, with a mock transport.
    pub fn with_client(client: CoinsetClient<T>) -> Result<Self, ChainSourceError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| {
                ChainSourceError::Transport(format!("failed to build coinset source runtime: {e}"))
            })?;
        Ok(Self {
            client: Arc::new(client),
            runtime: Arc::new(runtime),
        })
    }

    /// Drives an async coinset read to completion on the owned runtime, translating a runtime-misuse
    /// panic into a clear [`ChainSourceError`] (see [`run_blocking`]).
    fn block_on<F>(&self, fut: F) -> Result<F::Output, ChainSourceError>
    where
        F: Future,
    {
        run_blocking(self.runtime.handle(), fut)
    }
}

impl<T: HttpTransport> ChainSource for CoinsetChainSource<T> {
    type Error = ChainSourceError;

    fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
        let name = bytes32_to_hex(coin_id);
        let record = self
            .block_on(self.client.get_coin_record_by_name_opt(&name))?
            .map_err(map_query_error)?;
        record.as_ref().map(coin_record_from_chq).transpose()
    }

    fn coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: Bytes32,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        let hash = bytes32_to_hex(puzzle_hash);
        let records = self
            .block_on(self.client.get_coin_records_by_puzzle_hash(
                &hash,
                None,
                None,
                include_spent,
            ))?
            .map_err(map_query_error)?;
        convert_records(records)
    }

    fn coin_records_by_parent(
        &self,
        parent_coin_id: Bytes32,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        // The interface wants every child, so include spent coins.
        let parent_ids = [bytes32_to_hex(parent_coin_id)];
        let records = self
            .block_on(
                self.client
                    .get_coin_records_by_parent_ids(&parent_ids, None, None, true),
            )?
            .map_err(map_query_error)?;
        convert_records(records)
    }

    fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
        let id = bytes32_to_hex(coin_id);
        let spend = self
            .block_on(self.client.get_puzzle_and_solution_opt(&id, None))?
            .map_err(map_query_error)?;
        spend.as_ref().map(coin_spend_from_chq).transpose()
    }

    /// Deliberately unsupported: a genuine launcher → tip walk is not a lightweight coinset
    /// point-read (see the module docs). Fails closed rather than returning a misleading `Ok(None)`.
    fn resolve_singleton_lineage(
        &self,
        _launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        Err(ChainSourceError::Unsupported(
            "resolve_singleton_lineage is not served by the lightweight coinset source; use \
             ChiaQueryProvider or walk parent_spend",
        ))
    }

    fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
        let state = self
            .block_on(self.client.get_blockchain_state())?
            .map_err(map_query_error)?;
        Ok(state.peak.map(|peak| peak.height))
    }

    fn block_timestamp(&self, height: u32) -> Result<Option<u64>, Self::Error> {
        let record = self
            .block_on(self.client.get_block_record_by_height_opt(height))?
            .map_err(map_query_error)?;
        Ok(record.and_then(|record| record.timestamp))
    }
}

/// Converts a coinset list response into interface records, failing closed if the list exceeds
/// [`MAX_COIN_RECORDS`] (hostile-input bound, reported as [`ChainSourceError::TooManyRecords`]) or if
/// any record is malformed.
fn convert_records(
    records: Vec<crate::types::CoinRecord>,
) -> Result<Vec<CoinRecord>, ChainSourceError> {
    if records.len() > MAX_COIN_RECORDS {
        return Err(ChainSourceError::TooManyRecords {
            count: records.len(),
            limit: MAX_COIN_RECORDS,
        });
    }
    records.iter().map(coin_record_from_chq).collect()
}

/// The coinset base URL from the environment: `$DIG_COINSET_URL` when set and non-empty, else the
/// canonical [`DEFAULT_COINSET_URL`].
fn coinset_url_from_env() -> String {
    std::env::var(COINSET_URL_ENV)
        .ok()
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| DEFAULT_COINSET_URL.to_string())
}

#[cfg(feature = "native")]
impl CoinsetProvider<CoinsetChainSource<crate::coinset::transport::ReqwestTransport>> {
    /// Builds a registry-ready coinset provider against `coinset_url`, with NO peer handshake.
    ///
    /// The provider registers as a [`ProviderKind::PublicOracle`] (`trustless: false`) at the
    /// coinset try-order priority; hand it to a
    /// [`ProviderRegistry`](super::ProviderRegistry) by dependency injection like any other source.
    pub fn from_url(coinset_url: &str) -> Result<Self, ChainSourceError> {
        let source = CoinsetChainSource::from_url(coinset_url)?;
        Ok(CoinsetProvider::new(
            "coinset.org",
            COINSET_PROVIDER_PRIORITY,
            source,
        ))
    }

    /// Builds a registry-ready coinset provider from `$DIG_COINSET_URL` (or [`DEFAULT_COINSET_URL`]).
    pub fn from_env() -> Result<Self, ChainSourceError> {
        let source = CoinsetChainSource::from_env()?;
        Ok(CoinsetProvider::new(
            "coinset.org",
            COINSET_PROVIDER_PRIORITY,
            source,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use chia_protocol::Coin;
    use dig_chainsource_interface::{ChainSourceProvider, ProviderKind};
    use serde_json::{json, Value};

    use crate::coinset::transport::HttpTransport;
    use crate::types::ChiaQueryError;

    /// A scripted [`HttpTransport`] that returns a canned JSON body per endpoint (the trailing path
    /// segment of the POST URL), or a scripted transport error — so the source is exercised with no
    /// network and no peer handshake.
    #[derive(Default)]
    struct MockTransport {
        responses: Mutex<std::collections::HashMap<String, Value>>,
        fail: Mutex<Option<String>>,
    }

    impl MockTransport {
        fn with(endpoint: &str, body: Value) -> Self {
            let t = MockTransport::default();
            t.responses
                .lock()
                .unwrap()
                .insert(endpoint.to_string(), body);
            t
        }

        fn add(self, endpoint: &str, body: Value) -> Self {
            self.responses
                .lock()
                .unwrap()
                .insert(endpoint.to_string(), body);
            self
        }

        fn failing(msg: &str) -> Self {
            let t = MockTransport::default();
            *t.fail.lock().unwrap() = Some(msg.to_string());
            t
        }
    }

    impl HttpTransport for MockTransport {
        async fn post_json(&self, url: String, _body: Value) -> Result<Value, ChiaQueryError> {
            if let Some(msg) = self.fail.lock().unwrap().clone() {
                return Err(ChiaQueryError::CoinsetHttp(msg));
            }
            let endpoint = url.rsplit('/').next().unwrap_or_default().to_string();
            self.responses
                .lock()
                .unwrap()
                .get(&endpoint)
                .cloned()
                .ok_or_else(|| ChiaQueryError::CoinsetHttp(format!("no mock for `{endpoint}`")))
        }
    }

    fn source(transport: MockTransport) -> CoinsetChainSource<MockTransport> {
        let client = CoinsetClient::with_transport("https://coinset.test", transport);
        CoinsetChainSource::with_client(client).expect("build source")
    }

    fn coin_id() -> Bytes32 {
        Coin::new(Bytes32::new([0x11; 32]), Bytes32::new([0x22; 32]), 1).coin_id()
    }

    fn hex32(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 32]))
    }

    fn coin_record_json(spent: bool) -> Value {
        json!({
            "coin": { "parent_coin_info": hex32(0x11), "puzzle_hash": hex32(0x22), "amount": 1 },
            "confirmed_block_index": 100,
            "spent_block_index": if spent { 200 } else { 0 },
            "spent": spent,
            "coinbase": false,
            "timestamp": 1_700_000_000_u64
        })
    }

    // ---- construction ----

    #[test]
    fn coinset_provider_from_url_constructs_without_handshake() {
        // No Peer, no cert, no sync — just a base URL.
        let provider =
            CoinsetProvider::from_url("https://coinset.test").expect("construct from url");
        let info = provider.provider_info();
        assert_eq!(info.kind, ProviderKind::PublicOracle);
        assert!(!info.trustless, "a public oracle is never trustless");
        assert_eq!(info.priority, COINSET_PROVIDER_PRIORITY);
    }

    #[test]
    fn from_env_reads_dig_coinset_url_then_defaults() {
        // Env override wins.
        std::env::set_var(COINSET_URL_ENV, "https://env.coinset.test");
        assert_eq!(coinset_url_from_env(), "https://env.coinset.test");

        // Blank/unset falls back to the canonical default.
        std::env::set_var(COINSET_URL_ENV, "   ");
        assert_eq!(coinset_url_from_env(), DEFAULT_COINSET_URL);
        std::env::remove_var(COINSET_URL_ENV);
        assert_eq!(coinset_url_from_env(), DEFAULT_COINSET_URL);
    }

    // ---- point-reads ----

    #[test]
    fn coin_record_reads_over_coinset_http() {
        let src = source(MockTransport::with(
            "get_coin_record_by_name",
            json!({ "success": true, "coin_record": coin_record_json(false) }),
        ));
        let record = src.coin_record(coin_id()).unwrap().expect("record present");
        assert_eq!(record.confirmed_height, Some(100));
    }

    #[test]
    fn coin_record_absence_is_ok_none_not_err() {
        let src = source(MockTransport::with(
            "get_coin_record_by_name",
            json!({ "success": true, "coin_record": null }),
        ));
        assert_eq!(src.coin_record(coin_id()).unwrap(), None);
    }

    #[test]
    fn coin_record_transport_error_fails_closed_never_false_absence() {
        let src = source(MockTransport::failing("socket reset"));
        let err = src.coin_record(coin_id()).unwrap_err();
        assert!(
            matches!(err, ChainSourceError::Transport(_)),
            "a transport failure MUST be Err, never Ok(None)"
        );
    }

    #[test]
    fn coin_spend_reads_the_spend_that_spent_the_coin() {
        let src = source(MockTransport::with(
            "get_puzzle_and_solution",
            json!({
                "success": true,
                "coin_solution": {
                    "coin": { "parent_coin_info": hex32(0x11), "puzzle_hash": hex32(0x22), "amount": 1 },
                    "puzzle_reveal": "0xff",
                    "solution": "0x80"
                }
            }),
        ));
        assert!(src.coin_spend(coin_id()).unwrap().is_some());
    }

    #[test]
    fn coin_spend_unspent_is_ok_none() {
        let src = source(MockTransport::with(
            "get_puzzle_and_solution",
            json!({ "success": true, "coin_solution": null }),
        ));
        assert_eq!(src.coin_spend(coin_id()).unwrap(), None);
    }

    #[test]
    fn coin_records_by_puzzle_hash_maps_the_list() {
        let src = source(MockTransport::with(
            "get_coin_records_by_puzzle_hash",
            json!({ "success": true, "coin_records": [coin_record_json(false)] }),
        ));
        let records = src
            .coin_records_by_puzzle_hash(Bytes32::new([0x22; 32]), true)
            .unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn coin_records_by_parent_maps_the_list() {
        let src = source(MockTransport::with(
            "get_coin_records_by_parent_ids",
            json!({ "success": true, "coin_records": [coin_record_json(true)] }),
        ));
        let records = src
            .coin_records_by_parent(Bytes32::new([0x11; 32]))
            .unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn peak_height_reads_the_blockchain_state_peak() {
        let src = source(MockTransport::with(
            "get_blockchain_state",
            json!({
                "success": true,
                "blockchain_state": { "peak": { "height": 5_000_123 } }
            }),
        ));
        assert_eq!(src.peak_height().unwrap(), Some(5_000_123));
    }

    #[test]
    fn block_timestamp_reads_the_block_record() {
        let src = source(MockTransport::with(
            "get_block_record_by_height",
            json!({
                "success": true,
                "block_record": { "height": 42, "timestamp": 1_700_000_000_u64 }
            }),
        ));
        assert_eq!(src.block_timestamp(42).unwrap(), Some(1_700_000_000));
    }

    #[test]
    fn block_timestamp_absent_block_is_ok_none() {
        let src = source(MockTransport::with(
            "get_block_record_by_height",
            json!({ "success": true, "block_record": null }),
        ));
        assert_eq!(src.block_timestamp(999).unwrap(), None);
    }

    // ---- unsupported (fail-closed) ----

    #[test]
    fn resolve_singleton_lineage_is_unsupported_not_false_absence() {
        let src = source(MockTransport::default());
        let err = src
            .resolve_singleton_lineage(Bytes32::new([0x33; 32]))
            .unwrap_err();
        assert!(
            matches!(err, ChainSourceError::Unsupported(_)),
            "lineage MUST fail closed as Unsupported, never Ok(None)"
        );
    }

    // ---- hostile-input bound ----

    #[test]
    fn oversized_coin_record_list_fails_closed() {
        let flood: Vec<Value> = (0..=MAX_COIN_RECORDS)
            .map(|_| coin_record_json(false))
            .collect();
        let src = source(MockTransport::default().add(
            "get_coin_records_by_puzzle_hash",
            json!({ "success": true, "coin_records": flood }),
        ));
        let err = src
            .coin_records_by_puzzle_hash(Bytes32::new([0x22; 32]), true)
            .unwrap_err();
        assert!(
            matches!(
                err,
                ChainSourceError::TooManyRecords { count, limit }
                    if count == MAX_COIN_RECORDS + 1 && limit == MAX_COIN_RECORDS
            ),
            "an unbounded coinset list MUST fail closed as TooManyRecords, not be returned"
        );
    }
}
