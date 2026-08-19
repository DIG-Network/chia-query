use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChiaQueryError {
    #[error("peer rejected request: {0}")]
    PeerRejection(String),

    #[error("peer connection error: {0}")]
    PeerConnection(String),

    #[error("all sources failed")]
    AllSourcesFailed {
        peer_error: Box<ChiaQueryError>,
        coinset_error: Option<Box<ChiaQueryError>>,
    },

    #[error("coinset API error: {0}")]
    CoinsetApiError(String),

    #[error("coinset HTTP error: {0}")]
    CoinsetHttp(String),

    #[error("not supported without coinset: {0}")]
    UnsupportedWithoutCoinset(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("peer discovery failed: no peers found via DNS introducers")]
    PeerDiscoveryFailed,

    /// A source reported that something is ABSENT and no second, independent source could be
    /// reached to say the same.
    ///
    /// Absence is not self-verifying. A returned record can be checked against its own fields —
    /// a coin id is the hash of the coin's own contents — but there is nothing about an empty
    /// answer to check, so it is worth exactly as much as the source's word. The pool is up to
    /// five DNS-discovered, unauthenticated full nodes, any of which may be a block behind,
    /// mid-reorg, pruning, rate-limiting by omission, or hostile.
    ///
    /// This is NOT "the thing is present". It is "whether it exists could not be established",
    /// and a caller must treat it as the unknown it is (dig_ecosystem#2456).
    #[error("absence could not be corroborated: {0}")]
    UncorroboratedAbsence(String),

    /// Two independent sources gave contradictory answers: one says present, the other absent.
    ///
    /// Reported rather than resolved. There is no basis in the answers themselves for preferring
    /// either source, so picking a winner would be inventing a fact; a disagreement is evidence
    /// about the sources and belongs with the caller (NC-12).
    #[error("sources disagree: {0}")]
    SourcesDisagree(String),
}
