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
}
