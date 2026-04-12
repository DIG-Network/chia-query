//! Thin HTTP wrapper around the coinset.org REST API.
//!
//! Every endpoint is a simple POST-JSON / parse-JSON round-trip.  The only
//! cleverness is the shared `post()` helper that checks the `success` flag in
//! every response.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::Client;
use serde_json::{json, Value};

use crate::types::*;

pub struct CoinsetClient {
    http: Client,
    base_url: String,
}

impl CoinsetClient {
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, ChiaQueryError> {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(ChiaQueryError::CoinsetHttp)?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    // -----------------------------------------------------------------------
    // Generic POST helper
    // -----------------------------------------------------------------------

    async fn post(&self, endpoint: &str, body: &Value) -> Result<Value, ChiaQueryError> {
        let url = format!("{}/{}", self.base_url, endpoint);
        let resp = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(ChiaQueryError::CoinsetHttp)?;

        let json: Value = resp.json().await.map_err(ChiaQueryError::CoinsetHttp)?;

        if json.get("success").and_then(Value::as_bool) != Some(true) {
            let msg = json
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
                .to_string();
            return Err(ChiaQueryError::CoinsetApiError(msg));
        }

        Ok(json)
    }

    /// Convenience: post and then deserialise a single key out of the
    /// response object.
    async fn post_extract<T: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Value,
        key: &str,
    ) -> Result<T, ChiaQueryError> {
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
            .post("get_additions_and_removals", &json!({ "header_hash": header_hash }))
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

    pub async fn get_block_record(
        &self,
        header_hash: &str,
    ) -> Result<BlockRecord, ChiaQueryError> {
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

    pub async fn get_coin_record_by_name(
        &self,
        name: &str,
    ) -> Result<CoinRecord, ChiaQueryError> {
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

    pub async fn get_memos_by_coin_name(
        &self,
        name: &str,
    ) -> Result<Value, ChiaQueryError> {
        self.post_extract(
            "get_memos_by_coin_name",
            &json!({ "name": name }),
            "memos",
        )
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
        let conditions: Vec<Condition> = serde_json::from_value(json["conditions"].clone())
            .unwrap_or_default();
        Ok(CoinSpendWithConditions {
            coin_spend,
            conditions,
        })
    }

    pub async fn push_tx(&self, bundle: &SpendBundle) -> Result<TxStatus, ChiaQueryError> {
        let body = json!({ "spend_bundle": bundle });
        let json = self.post("push_tx", &body).await?;
        let status = json["status"]
            .as_str()
            .unwrap_or("UNKNOWN")
            .to_string();
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
