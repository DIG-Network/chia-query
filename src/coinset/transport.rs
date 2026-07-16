//! Pluggable HTTP transport for the coinset.org REST client.
//!
//! The coinset tier is the wasm-safe core of chia-query, so its HTTP layer is
//! abstracted behind [`HttpTransport`]: native builds fill it with a `reqwest`
//! client, while a `wasm32` build (feature `coinset`, no `native`) fills it
//! with an injected JavaScript `fetch`. Keeping the transport behind a trait
//! is what lets the same coinset client compile for both targets — the heavy
//! native stack (peer WebSockets, CLVM, `chia-wallet-sdk`, `native-tls`) never
//! reaches the wasm build.

use std::future::Future;

use serde_json::Value;

use crate::types::ChiaQueryError;

/// An async `POST` of a JSON body that yields the parsed JSON response.
///
/// Implementations own their own client/timeout configuration; the coinset
/// client only supplies the absolute URL and the request body.
pub trait HttpTransport {
    /// POST `body` as JSON to `url` and deserialize the JSON response.
    fn post_json(
        &self,
        url: String,
        body: Value,
    ) -> impl Future<Output = Result<Value, ChiaQueryError>>;
}

// ---------------------------------------------------------------------------
// Native transport (reqwest)
// ---------------------------------------------------------------------------

#[cfg(feature = "native")]
mod native {
    use std::time::Duration;

    use serde_json::Value;

    use super::HttpTransport;
    use crate::types::ChiaQueryError;

    /// The native transport: a shared `reqwest` client with a fixed timeout.
    #[derive(Clone)]
    pub struct ReqwestTransport {
        http: reqwest::Client,
    }

    impl ReqwestTransport {
        /// Build a transport whose requests time out after `timeout`.
        pub fn new(timeout: Duration) -> Result<Self, ChiaQueryError> {
            let http = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))?;
            Ok(Self { http })
        }
    }

    impl HttpTransport for ReqwestTransport {
        async fn post_json(&self, url: String, body: Value) -> Result<Value, ChiaQueryError> {
            let resp = self
                .http
                .post(&url)
                .json(&body)
                .send()
                .await
                .map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))?;
            resp.json()
                .await
                .map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))
        }
    }
}

#[cfg(feature = "native")]
pub use native::ReqwestTransport;

// ---------------------------------------------------------------------------
// wasm transport (injected fetch)
// ---------------------------------------------------------------------------

#[cfg(all(target_arch = "wasm32", feature = "coinset", not(feature = "native")))]
mod wasm {
    use serde_json::Value;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    use super::HttpTransport;
    use crate::types::ChiaQueryError;

    /// The wasm transport: an injected JavaScript `fetch` function.
    ///
    /// The consumer passes the host's `fetch` (browser `window.fetch` or Node's
    /// global `fetch`), so the crate stays environment-agnostic and pulls in no
    /// native HTTP stack.
    pub struct FetchTransport {
        fetch: js_sys::Function,
    }

    impl FetchTransport {
        /// Wrap an injected `fetch` function.
        pub fn new(fetch: js_sys::Function) -> Self {
            Self { fetch }
        }

        fn err(context: &str, value: &JsValue) -> ChiaQueryError {
            ChiaQueryError::CoinsetHttp(format!(
                "{context}: {}",
                value.as_string().unwrap_or_else(|| "js error".into())
            ))
        }
    }

    impl HttpTransport for FetchTransport {
        async fn post_json(&self, url: String, body: Value) -> Result<Value, ChiaQueryError> {
            // Build the fetch init object: { method, headers, body }.
            let init = js_sys::Object::new();
            js_sys::Reflect::set(&init, &"method".into(), &"POST".into())
                .map_err(|e| Self::err("set method", &e))?;

            let headers = js_sys::Object::new();
            js_sys::Reflect::set(&headers, &"Content-Type".into(), &"application/json".into())
                .map_err(|e| Self::err("set header", &e))?;
            js_sys::Reflect::set(&init, &"headers".into(), &headers)
                .map_err(|e| Self::err("set headers", &e))?;

            let payload = serde_json::to_string(&body)
                .map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))?;
            js_sys::Reflect::set(&init, &"body".into(), &JsValue::from_str(&payload))
                .map_err(|e| Self::err("set body", &e))?;

            let promise = self
                .fetch
                .call2(&JsValue::NULL, &JsValue::from_str(&url), &init)
                .map_err(|e| Self::err("call fetch", &e))?;
            let response = JsFuture::from(js_sys::Promise::from(promise))
                .await
                .map_err(|e| Self::err("await fetch", &e))?;

            // response.json() -> Promise -> value.
            let json_fn = js_sys::Reflect::get(&response, &"json".into())
                .map_err(|e| Self::err("get json()", &e))?
                .dyn_into::<js_sys::Function>()
                .map_err(|e| Self::err("json not callable", &e))?;
            let json_promise = json_fn
                .call0(&response)
                .map_err(|e| Self::err("call json()", &e))?;
            let value = JsFuture::from(js_sys::Promise::from(json_promise))
                .await
                .map_err(|e| Self::err("await json()", &e))?;

            serde_wasm_bindgen::from_value(value)
                .map_err(|e| ChiaQueryError::CoinsetHttp(format!("parse json: {e}")))
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "coinset", not(feature = "native")))]
pub use wasm::FetchTransport;
