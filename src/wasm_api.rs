//! wasm-bindgen facade over the coinset.org REST tier.
//!
//! This is the `@dignetwork/chia-query-wasm` surface: a browser/Node consumer
//! (dig-urn-resolver's chain-verify, dApps, the extension) constructs a
//! [`CoinsetClient`] with an injected `fetch`, then reads the chain through the
//! same canonical coinset layer the native crate uses — no direct coinset
//! access anywhere else.
//!
//! Only compiled for the wasm coinset-only build
//! (`--target wasm32 --no-default-features --features coinset`).

use js_sys::Function;
use serde_json::Value;
use wasm_bindgen::prelude::*;

use crate::coinset::transport::FetchTransport;
use crate::coinset::CoinsetClient as CoreClient;

/// A coinset.org client for wasm hosts.
///
/// The single trust note (see SPEC): this build drops chia-query's peer tier,
/// so a wasm consumer reads from ONE coinset endpoint with no peer cross-check.
/// Verification must stay client-side (e.g. singleton-lineage / anti-rollback
/// checks on top), the endpoint is configurable, and multi-endpoint
/// cross-checking is left open for the future.
#[wasm_bindgen]
pub struct CoinsetClient {
    inner: CoreClient<FetchTransport>,
}

#[wasm_bindgen]
impl CoinsetClient {
    /// Create a client for `base_url` (e.g. `"https://api.coinset.org"`) that
    /// issues requests through the injected `fetch` function — pass the host's
    /// `fetch` (`window.fetch` in a browser, the global `fetch` in Node 18+).
    #[wasm_bindgen(constructor)]
    pub fn new(base_url: &str, fetch: Function) -> CoinsetClient {
        CoinsetClient {
            inner: CoreClient::with_transport(base_url, FetchTransport::new(fetch)),
        }
    }

    /// POST `body` (a JS object) to `endpoint` and resolve with the parsed JSON
    /// response. Rejects when coinset returns `success: false` (the rejection
    /// carries the structured error message).
    #[wasm_bindgen]
    pub async fn request(&self, endpoint: String, body: JsValue) -> Result<JsValue, JsValue> {
        let body = to_json(body)?;
        let json = self
            .inner
            .post(&endpoint, &body)
            .await
            .map_err(to_js_error)?;
        to_js(&json)
    }

    /// POST `body` to `endpoint` and resolve with the raw JSON response,
    /// without the `success` gate — useful for inspecting error envelopes.
    #[wasm_bindgen(js_name = requestRaw)]
    pub async fn request_raw(&self, endpoint: String, body: JsValue) -> Result<JsValue, JsValue> {
        let body = to_json(body)?;
        let json = self
            .inner
            .post_raw(&endpoint, &body)
            .await
            .map_err(to_js_error)?;
        to_js(&json)
    }
}

/// Convert an injected JS value into a `serde_json::Value` request body,
/// treating `undefined`/`null` as an empty object.
fn to_json(value: JsValue) -> Result<Value, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(Value::Object(Default::default()));
    }
    serde_wasm_bindgen::from_value(value).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Serialize a `serde_json::Value` back to a JS value.
fn to_js(value: &Value) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Render a `ChiaQueryError` as a JS `Error`.
fn to_js_error(err: crate::types::ChiaQueryError) -> JsValue {
    js_sys::Error::new(&err.to_string()).into()
}
