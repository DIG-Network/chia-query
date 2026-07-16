//! Thin HTTP wrapper around the coinset.org REST API.
//!
//! Every endpoint is a simple POST-JSON / parse-JSON round-trip.  The only
//! cleverness is the shared `post()` helper that checks the `success` flag in
//! every response.

use std::collections::HashMap;
#[cfg(feature = "native")]
use std::time::Duration;

use serde_json::{json, Value};

use crate::types::*;

pub mod transport;

use transport::HttpTransport;

/// The transport chia-query uses by default for the target it is built for:
/// `reqwest` on native, an injected `fetch` on wasm. Named so the common type
/// `CoinsetClient` (without a generic argument) resolves correctly on both.
#[cfg(feature = "native")]
pub type DefaultTransport = transport::ReqwestTransport;
#[cfg(all(target_arch = "wasm32", feature = "coinset", not(feature = "native")))]
pub type DefaultTransport = transport::FetchTransport;

/// A thin, transport-generic client over the coinset.org REST API.
///
/// Every endpoint is a `POST`-JSON / parse-JSON round-trip; the only cleverness
/// is [`post`](Self::post), which checks the `success` flag on every response.
pub struct CoinsetClient<T = DefaultTransport> {
    transport: T,
    base_url: String,
}

#[cfg(feature = "native")]
impl CoinsetClient<transport::ReqwestTransport> {
    /// Build a native coinset client backed by a `reqwest` transport.
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, ChiaQueryError> {
        Ok(Self {
            transport: transport::ReqwestTransport::new(timeout)?,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }
}

impl<T: HttpTransport> CoinsetClient<T> {
    /// Build a client from an explicit transport (used by wasm consumers that
    /// inject `fetch`, and by tests supplying a mock transport).
    pub fn with_transport(base_url: &str, transport: T) -> Self {
        Self {
            transport,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Generic POST helper
    // -----------------------------------------------------------------------

    /// POST to `endpoint` and return the response, mapping a `success: false`
    /// envelope to [`ChiaQueryError::CoinsetApiError`] (preferring the
    /// structured error message).
    pub async fn post(&self, endpoint: &str, body: &Value) -> Result<Value, ChiaQueryError> {
        let json = self.post_raw(endpoint, body).await?;
        if json.get("success").and_then(Value::as_bool) != Some(true) {
            return Err(ChiaQueryError::CoinsetApiError(coinset_error_message(
                &json,
            )));
        }
        Ok(json)
    }

    /// POST and return the raw JSON response *without* the `success` gate.
    ///
    /// Unlike [`post`](Self::post), this never converts a `success: false`
    /// envelope into an error — the caller sees the response verbatim. It
    /// exists for the drift-monitor, which must inspect the *shape* of every
    /// response (including error envelopes) rather than its meaning.
    pub async fn post_raw(&self, endpoint: &str, body: &Value) -> Result<Value, ChiaQueryError> {
        let url = format!("{}/{}", self.base_url, endpoint);
        self.transport.post_json(url, body.clone()).await
    }

    /// Convenience: post and then deserialise a single key out of the
    /// response object.
    async fn post_extract<D: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Value,
        key: &str,
    ) -> Result<D, ChiaQueryError> {
        let json = self.post(endpoint, body).await?;
        serde_json::from_value(json[key].clone())
            .map_err(|e| ChiaQueryError::CoinsetApiError(format!("parse `{key}`: {e}")))
    }

    // =======================================================================
    // Blocks
    // =======================================================================

    pub async fn get_additions_and_removals(
        &self,
        header_hash: &str,
    ) -> Result<AdditionsAndRemovals, ChiaQueryError> {
        let json = self
            .post(
                "get_additions_and_removals",
                &json!({ "header_hash": header_hash }),
            )
            .await?;
        let additions = serde_json::from_value(json["additions"].clone())
            .map_err(|e| ChiaQueryError::CoinsetApiError(e.to_string()))?;
        let removals = serde_json::from_value(json["removals"].clone())
            .map_err(|e| ChiaQueryError::CoinsetApiError(e.to_string()))?;
        Ok(AdditionsAndRemovals {
            additions,
            removals,
        })
    }

    pub async fn get_block(&self, header_hash: &str) -> Result<FullBlock, ChiaQueryError> {
        self.post_extract("get_block", &json!({ "header_hash": header_hash }), "block")
            .await
    }

    pub async fn get_block_count_metrics(&self) -> Result<BlockCountMetrics, ChiaQueryError> {
        self.post_extract("get_block_count_metrics", &json!({}), "metrics")
            .await
    }

    pub async fn get_block_record(&self, header_hash: &str) -> Result<BlockRecord, ChiaQueryError> {
        self.post_extract(
            "get_block_record",
            &json!({ "header_hash": header_hash }),
            "block_record",
        )
        .await
    }

    pub async fn get_block_record_by_height(
        &self,
        height: u32,
    ) -> Result<BlockRecord, ChiaQueryError> {
        self.post_extract(
            "get_block_record_by_height",
            &json!({ "height": height }),
            "block_record",
        )
        .await
    }

    pub async fn get_block_records(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Vec<BlockRecord>, ChiaQueryError> {
        self.post_extract(
            "get_block_records",
            &json!({ "start": start, "end": end }),
            "block_records",
        )
        .await
    }

    pub async fn get_block_spends(
        &self,
        header_hash: &str,
    ) -> Result<Vec<CoinSpend>, ChiaQueryError> {
        self.post_extract(
            "get_block_spends",
            &json!({ "header_hash": header_hash }),
            "block_spends",
        )
        .await
    }

    pub async fn get_block_spends_with_conditions(
        &self,
        header_hash: &str,
    ) -> Result<Vec<CoinSpendWithConditions>, ChiaQueryError> {
        self.post_extract(
            "get_block_spends_with_conditions",
            &json!({ "header_hash": header_hash }),
            "block_spends_with_conditions",
        )
        .await
    }

    pub async fn get_blocks(
        &self,
        start: u32,
        end: u32,
        exclude_header_hash: bool,
        exclude_reorged: bool,
    ) -> Result<Vec<FullBlock>, ChiaQueryError> {
        self.post_extract(
            "get_blocks",
            &json!({
                "start": start,
                "end": end,
                "exclude_header_hash": exclude_header_hash,
                "exclude_reorged": exclude_reorged,
            }),
            "blocks",
        )
        .await
    }

    pub async fn get_unfinished_block_headers(
        &self,
    ) -> Result<Vec<UnfinishedBlockHeader>, ChiaQueryError> {
        self.post_extract("get_unfinished_block_headers", &json!({}), "headers")
            .await
    }

    // =======================================================================
    // Coins
    // =======================================================================

    pub async fn get_coin_record_by_name(&self, name: &str) -> Result<CoinRecord, ChiaQueryError> {
        self.post_extract(
            "get_coin_record_by_name",
            &json!({ "name": name }),
            "coin_record",
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
        self.post_extract(
            "get_coin_records_by_hint",
            &json!({
                "hint": hint,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
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
        self.post_extract(
            "get_coin_records_by_hints",
            &json!({
                "hints": hints,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
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
        self.post_extract(
            "get_coin_records_by_names",
            &json!({
                "names": names,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
        )
        .await
    }

    pub async fn get_coin_records_by_parent_ids(
        &self,
        parent_ids: &[String],
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.post_extract(
            "get_coin_records_by_parent_ids",
            &json!({
                "parent_ids": parent_ids,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
        )
        .await
    }

    pub async fn get_coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: &str,
        start_height: Option<u32>,
        end_height: Option<u32>,
        include_spent_coins: bool,
    ) -> Result<Vec<CoinRecord>, ChiaQueryError> {
        self.post_extract(
            "get_coin_records_by_puzzle_hash",
            &json!({
                "puzzle_hash": puzzle_hash,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
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
        self.post_extract(
            "get_coin_records_by_puzzle_hashes",
            &json!({
                "puzzle_hashes": puzzle_hashes,
                "start_height": start_height,
                "end_height": end_height,
                "include_spent_coins": include_spent_coins,
            }),
            "coin_records",
        )
        .await
    }

    pub async fn get_memos_by_coin_name(&self, name: &str) -> Result<Value, ChiaQueryError> {
        self.post_extract("get_memos_by_coin_name", &json!({ "name": name }), "memos")
            .await
    }

    pub async fn get_puzzle_and_solution(
        &self,
        coin_id: &str,
        height: Option<u32>,
    ) -> Result<CoinSpend, ChiaQueryError> {
        self.post_extract(
            "get_puzzle_and_solution",
            &json!({ "coin_id": coin_id, "height": height }),
            "coin_solution",
        )
        .await
    }

    pub async fn get_puzzle_and_solution_with_conditions(
        &self,
        coin_id: &str,
        height: Option<u32>,
    ) -> Result<CoinSpendWithConditions, ChiaQueryError> {
        let json = self
            .post(
                "get_puzzle_and_solution_with_conditions",
                &json!({ "coin_id": coin_id, "height": height }),
            )
            .await?;
        let coin_spend: CoinSpend = serde_json::from_value(json["coin_solution"].clone())
            .map_err(|e| ChiaQueryError::CoinsetApiError(e.to_string()))?;
        let conditions: Vec<Condition> =
            serde_json::from_value(json["conditions"].clone()).unwrap_or_default();
        Ok(CoinSpendWithConditions {
            coin_spend,
            conditions,
        })
    }

    pub async fn push_tx(&self, bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
        let body = json!({ "spend_bundle": bundle });
        let json = self.post("push_tx", &body).await?;
        let status = json["status"].as_str().unwrap_or("UNKNOWN").to_string();
        Ok(TxStatus {
            status,
            success: true,
        })
    }

    // =======================================================================
    // Fees
    // =======================================================================

    pub async fn get_fee_estimate(
        &self,
        spend_bundle: Option<&SpendBundle>,
        target_times: Option<&[u64]>,
        spend_count: Option<u64>,
    ) -> Result<FeeEstimate, ChiaQueryError> {
        let mut body = json!({ "cost": 1 });
        if let Some(sb) = spend_bundle {
            body["spend_bundle"] = serde_json::to_value(sb)
                .map_err(|e| ChiaQueryError::InvalidRequest(e.to_string()))?;
        }
        if let Some(tt) = target_times {
            body["target_times"] = json!(tt);
        }
        if let Some(sc) = spend_count {
            body["spend_count"] = json!(sc);
        }
        let json = self.post("get_fee_estimate", &body).await?;
        serde_json::from_value(json)
            .map_err(|e| ChiaQueryError::CoinsetApiError(format!("parse fee_estimate: {e}")))
    }

    // =======================================================================
    // Full node / network
    // =======================================================================

    pub async fn get_aggsig_additional_data(&self) -> Result<String, ChiaQueryError> {
        self.post_extract("get_aggsig_additional_data", &json!({}), "additional_data")
            .await
    }

    pub async fn get_network_info(&self) -> Result<NetworkInfo, ChiaQueryError> {
        let json = self.post("get_network_info", &json!({})).await?;
        serde_json::from_value(json)
            .map_err(|e| ChiaQueryError::CoinsetApiError(format!("parse network_info: {e}")))
    }

    pub async fn get_blockchain_state(&self) -> Result<BlockchainState, ChiaQueryError> {
        self.post_extract("get_blockchain_state", &json!({}), "blockchain_state")
            .await
    }

    pub async fn get_network_space(
        &self,
        newer_block_header_hash: &str,
        older_block_header_hash: &str,
    ) -> Result<u64, ChiaQueryError> {
        self.post_extract(
            "get_network_space",
            &json!({
                "newer_block_header_hash": newer_block_header_hash,
                "older_block_header_hash": older_block_header_hash,
            }),
            "space",
        )
        .await
    }

    // =======================================================================
    // Mempool
    // =======================================================================

    pub async fn get_all_mempool_items(
        &self,
    ) -> Result<HashMap<String, MempoolItem>, ChiaQueryError> {
        self.post_extract("get_all_mempool_items", &json!({}), "mempool_items")
            .await
    }

    pub async fn get_all_mempool_tx_ids(&self) -> Result<Vec<String>, ChiaQueryError> {
        self.post_extract("get_all_mempool_tx_ids", &json!({}), "tx_ids")
            .await
    }

    pub async fn get_mempool_item_by_tx_id(
        &self,
        tx_id: &str,
    ) -> Result<MempoolItem, ChiaQueryError> {
        self.post_extract(
            "get_mempool_item_by_tx_id",
            &json!({ "tx_id": tx_id }),
            "mempool_item",
        )
        .await
    }

    pub async fn get_mempool_items_by_coin_name(
        &self,
        coin_name: &str,
        include_spent_coins: Option<bool>,
    ) -> Result<Vec<MempoolItem>, ChiaQueryError> {
        let mut body = json!({ "coin_name": coin_name });
        if let Some(inc) = include_spent_coins {
            body["include_spent_coins"] = json!(inc);
        }
        self.post_extract("get_mempool_items_by_coin_name", &body, "mempool_items")
            .await
    }
}

// ---------------------------------------------------------------------------
// Error-envelope parsing
// ---------------------------------------------------------------------------

/// Extract a human-readable message from a coinset.org error envelope.
///
/// A failing coinset.org response carries
/// `{ "error", "structuredError", "traceback", "success": false }`. The
/// `structuredError` field — an object `{ code, data, message }` (or, on some
/// endpoints, a bare string) — is the newer, stable summary and is preferred;
/// the legacy `error` string is the fallback; then a generic message.
///
/// `traceback` is deliberately IGNORED: it is opaque server-internal detail
/// (stack frames) and must never surface into user-facing output.
pub fn coinset_error_message(json: &Value) -> String {
    structured_error_message(json.get("structuredError"))
        .or_else(|| non_empty_str(json.get("error")))
        .unwrap_or_else(|| "unknown error".to_string())
}

/// Pull the message out of a `structuredError` value, accepting either a bare
/// string or an object exposing `message` (preferred) or `error`.
fn structured_error_message(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
        Value::Object(map) => {
            non_empty_str(map.get("message")).or_else(|| non_empty_str(map.get("error")))
        }
        _ => None,
    }
}

/// A trimmed, owned copy of a JSON string value, or `None` when absent/blank.
fn non_empty_str(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prefers_structured_error_message_over_legacy_error() {
        // The real coinset.org error envelope (probed 2026-07-16): `structuredError`
        // is an object and rides beside the legacy `error` string + a `traceback`.
        let envelope = json!({
            "error": "Coin record 0xnothex not found",
            "structuredError": {
                "code": "COIN_RECORD_NOT_FOUND",
                "data": { "name": "nothex" },
                "message": "Coin record not found"
            },
            "success": false,
            "traceback": null
        });
        assert_eq!(coinset_error_message(&envelope), "Coin record not found");
    }

    #[test]
    fn traceback_is_never_surfaced() {
        let envelope = json!({
            "error": "boom",
            "structuredError": { "message": "clean summary" },
            "traceback": "Traceback (most recent call last): secret internals",
            "success": false
        });
        let msg = coinset_error_message(&envelope);
        assert_eq!(msg, "clean summary");
        assert!(!msg.contains("Traceback"));
    }

    #[test]
    fn accepts_structured_error_as_bare_string() {
        let envelope = json!({ "structuredError": "just a string", "success": false });
        assert_eq!(coinset_error_message(&envelope), "just a string");
    }

    #[test]
    fn falls_back_to_legacy_error_when_no_structured_error() {
        let envelope = json!({ "error": "legacy only", "success": false });
        assert_eq!(coinset_error_message(&envelope), "legacy only");
    }

    #[test]
    fn falls_back_to_legacy_when_structured_error_is_empty() {
        let envelope = json!({
            "error": "legacy wins",
            "structuredError": { "message": "   " },
            "success": false
        });
        assert_eq!(coinset_error_message(&envelope), "legacy wins");
    }

    #[test]
    fn generic_message_when_nothing_usable() {
        let envelope = json!({ "success": false });
        assert_eq!(coinset_error_message(&envelope), "unknown error");
    }
}
