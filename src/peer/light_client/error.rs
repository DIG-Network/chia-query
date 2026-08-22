//! [`LightClientError`] — why a light-client read could not complete, and its fail-closed mapping
//! onto the canonical [`dig_chainsource_interface::ChainSourceError`].
//!
//! Deliberately NOT [`ChiaQueryError`](crate::ChiaQueryError). That type is the async router's, and
//! it is neither `Clone` nor `PartialEq` because it carries a boxed cause chain across the
//! peer/coinset race. This one crosses a SYNCHRONOUS [`ChainSource`] boundary where every variant
//! must answer one question — *may this be reported as an absence?* — and the answer is always no.
//!
//! Every variant means the same thing to a consumer: **the peer could not reliably answer**. None of
//! them is ever collapsed into an absence (`Ok(None)`) — that distinction is the crux of the
//! [`ChainSource`](dig_chainsource_interface::ChainSource) fail-closed contract, so the mapping below
//! turns every `LightClientError` into an `Err`, never a value.

use dig_chainsource_interface::ChainSourceError;
use thiserror::Error;

/// The reason a Chia light-client peer read could not complete reliably.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LightClientError {
    /// A transport/connection failure reaching the peer (socket, websocket, TLS). Carries the
    /// backend's own message for diagnostics.
    #[error("chia peer transport error: {0}")]
    Transport(String),

    /// The peer explicitly rejected the request (e.g. a reorg or subscription-limit rejection). The
    /// answer is unknown, so the consumer must fail closed.
    #[error("chia peer rejected the request: {0}")]
    Rejected(String),

    /// The peer responded but the payload could not be parsed into the expected chain type. The read
    /// is untrustworthy, so fail closed.
    #[error("malformed chia peer response: {0}")]
    Malformed(String),

    /// A request did not complete within the configured deadline. Whether the answer would have been
    /// present is unknown, so fail closed.
    #[error("chia peer request timed out")]
    Timeout,

    /// No usable full-node peer could be discovered/connected.
    #[error("chia peer discovery failed")]
    PeerDiscoveryFailed,

    /// The light client is not currently connected to any peer.
    #[error("chia light client is not connected")]
    NotConnected,
}

impl LightClientError {
    /// Whether this error's message names a timeout, so a timed-out transport string classifies as
    /// [`ChainSourceError::Timeout`] rather than a generic transport failure.
    fn looks_like_timeout(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("timed out") || lower.contains("timeout")
    }
}

impl From<LightClientError> for ChainSourceError {
    /// Maps every peer error to a fail-closed [`ChainSourceError`]. Each variant is a "could not
    /// reliably answer" signal — NEVER an `Ok(None)`; only the reason class differs, for diagnostics.
    fn from(error: LightClientError) -> Self {
        match error {
            LightClientError::Timeout => ChainSourceError::Timeout,
            LightClientError::Transport(msg) if LightClientError::looks_like_timeout(&msg) => {
                ChainSourceError::Timeout
            }
            LightClientError::Transport(msg) => ChainSourceError::Transport(msg),
            LightClientError::Rejected(msg) => ChainSourceError::Transport(msg),
            LightClientError::Malformed(msg) => ChainSourceError::Malformed(msg),
            LightClientError::PeerDiscoveryFailed => {
                ChainSourceError::Transport("peer discovery failed".to_string())
            }
            LightClientError::NotConnected => {
                ChainSourceError::Transport("light client is not connected".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_variant_maps_to_timeout() {
        assert_eq!(
            ChainSourceError::from(LightClientError::Timeout),
            ChainSourceError::Timeout
        );
    }

    #[test]
    fn timed_out_transport_message_maps_to_timeout() {
        let mapped =
            ChainSourceError::from(LightClientError::Transport("request timed out".into()));
        assert_eq!(mapped, ChainSourceError::Timeout);
    }

    #[test]
    fn plain_transport_maps_to_transport_never_absence() {
        let mapped = ChainSourceError::from(LightClientError::Transport("socket reset".into()));
        assert_eq!(mapped, ChainSourceError::Transport("socket reset".into()));
    }

    #[test]
    fn rejected_maps_to_transport() {
        let mapped = ChainSourceError::from(LightClientError::Rejected("reorg".into()));
        assert!(matches!(mapped, ChainSourceError::Transport(_)));
    }

    #[test]
    fn malformed_maps_to_malformed() {
        let mapped = ChainSourceError::from(LightClientError::Malformed("bad bytes".into()));
        assert!(matches!(mapped, ChainSourceError::Malformed(_)));
    }

    #[test]
    fn not_connected_and_discovery_map_to_transport() {
        assert!(matches!(
            ChainSourceError::from(LightClientError::NotConnected),
            ChainSourceError::Transport(_)
        ));
        assert!(matches!(
            ChainSourceError::from(LightClientError::PeerDiscoveryFailed),
            ChainSourceError::Transport(_)
        ));
    }
}
