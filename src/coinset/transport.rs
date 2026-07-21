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

    use futures_util::StreamExt;
    use serde_json::Value;

    use super::HttpTransport;
    use crate::types::ChiaQueryError;

    /// Hard ceiling on a coinset response body, enforced at the transport layer so an over-large
    /// (accidental or hostile) body is rejected while streaming, before it is fully buffered or
    /// deserialized.
    ///
    /// A legitimate maximal answer — a 100k-record list read (the `MAX_COIN_RECORDS` downstream cap) —
    /// serializes to roughly 50-60 MB, so 256 MiB leaves generous headroom for any honest response
    /// while still bounding a multi-GB hostile body to a fixed, survivable receive/parse peak. This
    /// caps RECEIVE/PARSE memory; the record-count cap then bounds downstream work on what survives.
    const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

    /// Whether an advertised `Content-Length` is within the cap. An absent length (chunked/unknown)
    /// passes this pre-check — the streamed running-size check ([`accumulate_within_cap`]) is the
    /// authoritative bound in that case.
    fn content_length_within_cap(len: Option<u64>, cap: usize) -> bool {
        match len {
            Some(len) => len <= cap as u64,
            None => true,
        }
    }

    /// Adds `chunk` bytes to the `current` running total, failing if the total would exceed `cap`.
    /// Uses a saturating add so a pathological chunk size can never wrap the counter below the cap.
    fn accumulate_within_cap(
        current: usize,
        chunk: usize,
        cap: usize,
    ) -> Result<usize, ChiaQueryError> {
        let next = current.saturating_add(chunk);
        if next > cap {
            return Err(ChiaQueryError::CoinsetHttp(format!(
                "coinset response exceeded the {cap}-byte cap"
            )));
        }
        Ok(next)
    }

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

            // Reject an over-large body up front when the server advertises its size, then bound the
            // actual bytes as they stream in (the advertised length may be absent or lie), so a
            // hostile endpoint can never make us buffer an unbounded body before parsing.
            if !content_length_within_cap(resp.content_length(), MAX_RESPONSE_BYTES) {
                return Err(ChiaQueryError::CoinsetHttp(format!(
                    "coinset response Content-Length exceeds the {MAX_RESPONSE_BYTES}-byte cap"
                )));
            }

            let mut buf: Vec<u8> = Vec::new();
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))?;
                accumulate_within_cap(buf.len(), chunk.len(), MAX_RESPONSE_BYTES)?;
                buf.extend_from_slice(&chunk);
            }

            serde_json::from_slice(&buf).map_err(|e| ChiaQueryError::CoinsetHttp(e.to_string()))
        }
    }

    #[cfg(all(test, feature = "native"))]
    mod tests {
        use std::time::Duration;

        use super::super::HttpTransport;
        use super::{accumulate_within_cap, content_length_within_cap, ReqwestTransport};

        const CAP: usize = 1_000;

        #[test]
        fn content_length_under_or_at_cap_is_allowed() {
            assert!(content_length_within_cap(Some(0), CAP));
            assert!(content_length_within_cap(Some(CAP as u64 - 1), CAP));
            assert!(content_length_within_cap(Some(CAP as u64), CAP));
        }

        #[test]
        fn content_length_over_cap_is_rejected() {
            assert!(!content_length_within_cap(Some(CAP as u64 + 1), CAP));
        }

        #[test]
        fn absent_content_length_passes_the_precheck() {
            // Unknown length defers to the streamed running-size check.
            assert!(content_length_within_cap(None, CAP));
        }

        #[test]
        fn accumulate_under_and_at_cap_is_ok() {
            assert_eq!(accumulate_within_cap(0, 500, CAP).unwrap(), 500);
            assert_eq!(accumulate_within_cap(500, 500, CAP).unwrap(), CAP);
        }

        #[test]
        fn accumulate_over_cap_errors() {
            assert!(accumulate_within_cap(CAP, 1, CAP).is_err());
            assert!(accumulate_within_cap(600, 500, CAP).is_err());
        }

        #[test]
        fn accumulate_saturates_instead_of_wrapping() {
            // A pathological chunk size must never wrap the counter back under the cap.
            assert!(accumulate_within_cap(1, usize::MAX, CAP).is_err());
        }

        /// Serves one canned raw HTTP/1.1 response on a fresh loopback port, returning its URL. The
        /// listener answers exactly one request then closes — enough to drive `post_json` end-to-end
        /// over a real `reqwest` client (the streaming/parse path the pure helpers only cover in part).
        async fn serve_once(raw_response: &'static [u8]) -> String {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpListener;

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                if let Ok((mut sock, _)) = listener.accept().await {
                    // Drain the request head enough to unblock the client's write, then reply.
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf).await;
                    let _ = sock.write_all(raw_response).await;
                    let _ = sock.flush().await;
                }
            });
            format!("http://{addr}/")
        }

        #[tokio::test]
        async fn post_json_streams_and_parses_a_bounded_body() {
            let url = serve_once(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"peak\":1234}",
            )
            .await;

            let transport = ReqwestTransport::new(Duration::from_secs(5)).unwrap();
            let value = transport
                .post_json(url, serde_json::json!({"q": 1}))
                .await
                .expect("a bounded body must stream and parse");
            assert_eq!(value["peak"], 1234);
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
