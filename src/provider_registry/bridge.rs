//! The async->sync bridge that lets an async [`QueryRouter`](crate::router::QueryRouter) present a
//! synchronous [`ChainSource`](dig_chainsource_interface::ChainSource) facade.
//!
//! The synchronous trait must drive async work to completion on the calling thread. That is only
//! sound on a **multi-thread** tokio runtime: [`tokio::task::block_in_place`] panics on a
//! current-thread runtime, and [`Handle::block_on`] panics if called while already inside a runtime.
//! [`run_blocking`] turns every such misuse into a CLEAR [`ChainSourceError`] instead of an opaque
//! tokio panic (SPEC §7).
//!
//! ## Consumer guidance (SPEC §7)
//!
//! A consumer that is itself async MUST NOT call the sync facade directly on its runtime thread —
//! it either builds the provider on a multi-thread runtime, or wraps the call in
//! [`tokio::task::spawn_blocking`] so the blocking read runs off the async worker.

use std::future::Future;

use dig_chainsource_interface::ChainSourceError;
use tokio::runtime::{Handle, RuntimeFlavor};

/// The message returned when the sync facade is driven from a current-thread runtime, where
/// blocking would otherwise panic inside tokio.
pub(crate) const CURRENT_THREAD_MSG: &str =
    "chia-query ChainSource facade requires a multi-thread tokio runtime; it was invoked from a \
     current-thread runtime. Build the provider on a multi-thread runtime, or wrap the call in \
     tokio::task::spawn_blocking (see SPEC §7).";

/// Drives `fut` to completion synchronously on `handle`, failing closed with a clear
/// [`ChainSourceError`] rather than panicking when the ambient runtime cannot support blocking.
///
/// Resolution:
/// - **Inside a multi-thread runtime** — offload via [`block_in_place`](tokio::task::block_in_place)
///   so the async worker is not starved, then [`block_on`](Handle::block_on) on `handle`.
/// - **Inside a current-thread runtime** — blocking would panic, so return
///   [`ChainSourceError::Transport`] carrying [`CURRENT_THREAD_MSG`].
/// - **Outside any runtime** — [`block_on`](Handle::block_on) directly on `handle`.
///
/// A [`catch_unwind`](std::panic::catch_unwind) backstop converts any residual tokio panic into an
/// error, so no misuse can ever unwind out of the synchronous trait boundary.
pub(crate) fn run_blocking<F>(handle: &Handle, fut: F) -> Result<F::Output, ChainSourceError>
where
    F: Future,
{
    match Handle::try_current() {
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
            Err(ChainSourceError::Transport(CURRENT_THREAD_MSG.to_string()))
        }
        Ok(_) => guard_panics(|| tokio::task::block_in_place(|| handle.block_on(fut))),
        Err(_) => guard_panics(|| handle.block_on(fut)),
    }
}

/// Runs `f`, converting a panic into a [`ChainSourceError::Transport`] so the sync trait boundary
/// never unwinds. `f` is `AssertUnwindSafe` because the future is consumed exactly once and a panic
/// leaves no observable half-mutated state behind the facade.
fn guard_panics<T>(f: impl FnOnce() -> T) -> Result<T, ChainSourceError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|_| {
        ChainSourceError::Transport(
            "chia-query ChainSource facade caught a panic while blocking on the async runtime"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test #8 — the sync facade, driven from a CURRENT-THREAD runtime, yields a CLEAR error rather
    /// than the opaque `block_in_place`/`block_on` panic tokio would otherwise raise.
    #[test]
    fn current_thread_runtime_yields_clear_error_not_panic() {
        // A multi-thread handle the facade would normally block on.
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let handle = mt.handle().clone();

        // Drive the facade from WITHIN a current-thread runtime — the misuse case.
        let ct = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        let result: Result<u32, ChainSourceError> =
            ct.block_on(async { run_blocking(&handle, async { 7u32 }) });

        match result {
            Err(ChainSourceError::Transport(msg)) => {
                assert!(
                    msg.contains("multi-thread tokio runtime"),
                    "error must name the multi-thread requirement, got: {msg}"
                );
            }
            other => panic!("expected a clear Transport error, got {other:?}"),
        }
    }

    /// From a multi-thread runtime the facade blocks and returns the value.
    #[test]
    fn multi_thread_runtime_blocks_and_returns_value() {
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let handle = mt.handle().clone();

        let result: Result<u32, ChainSourceError> =
            mt.block_on(async { run_blocking(&handle, async { 42u32 }) });

        assert_eq!(result, Ok(42));
    }

    /// Outside any runtime the facade blocks directly on the stored handle.
    #[test]
    fn outside_runtime_blocks_directly() {
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let handle = mt.handle().clone();

        let result = run_blocking(&handle, async { 100u32 });
        assert_eq!(result, Ok(100));
    }
}
