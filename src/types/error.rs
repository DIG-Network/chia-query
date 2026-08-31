use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChiaQueryError {
    #[error("peer rejected request: {0}")]
    PeerRejection(String),

    #[error("peer connection error: {0}")]
    PeerConnection(String),

    /// Every source was tried and every source failed. Both causes are NAMED: a flat "all sources
    /// failed" tells an operator nothing about whether the peer tier was unreachable, the coinset
    /// tier rejected the request, or both said the same thing -- and the answers were already in
    /// hand when the variant was built (#48).
    #[error("all sources failed (peer: {peer_error}; coinset: {})",
        .coinset_error.as_ref().map_or_else(|| "not attempted".to_string(), |e| e.to_string()))]
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

    /// A source produced a RECORD and no second, independent source could be brought to make the
    /// same claim about the chain.
    ///
    /// A positive answer authenticates its own identity and nothing more: a coin id is the hash of
    /// `parent_coin_info ‖ puzzle_hash ‖ amount`, so `created_height` and `spent_height` — the
    /// fields the read exists to obtain — are the source's unproven word. A peer holding one slot
    /// in the pool can return a real coin's fields beside a height it invented, and every check
    /// the record can perform on itself passes.
    ///
    /// This is NOT "the thing is absent". It is "where it sits on the chain could not be
    /// established", and treating it as evidence is what records a mint that never happened
    /// (dig_ecosystem#2462).
    #[error("presence could not be corroborated: {0}")]
    UncorroboratedPresence(String),

    /// Two independent sources gave contradictory answers: one says present, the other absent.
    ///
    /// Reported rather than resolved. There is no basis in the answers themselves for preferring
    /// either source, so picking a winner would be inventing a fact; a disagreement is evidence
    /// about the sources and belongs with the caller (NC-12).
    #[error("sources disagree: {0}")]
    SourcesDisagree(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #48, defect 3. `AllSourcesFailed` rendered as a flat "all sources failed", so the two causes
    /// it was CARRYING never reached anyone -- the answer was in hand and thrown away.
    ///
    /// The fixture builds two failures that differ only in their inner causes and requires the two
    /// renderings to differ. A test asserting the message merely mentions one cause would pass
    /// against an implementation that named the peer error alone; requiring both texts to appear,
    /// and two different pairs to render differently, cannot be satisfied that way.
    #[test]
    fn all_sources_failed_names_both_of_its_causes() {
        let both = ChiaQueryError::AllSourcesFailed {
            peer_error: Box::new(ChiaQueryError::PeerConnection("request timed out".into())),
            coinset_error: Some(Box::new(ChiaQueryError::CoinsetHttp(
                "503 from edge".into(),
            ))),
        };
        let rendered = both.to_string();
        assert!(
            rendered.contains("request timed out"),
            "peer cause missing from: {rendered}"
        );
        assert!(
            rendered.contains("503 from edge"),
            "coinset cause missing from: {rendered}"
        );

        // A different pair of causes must read differently -- the pre-fix message was identical
        // for every failure, which is exactly why it carried no information.
        let other = ChiaQueryError::AllSourcesFailed {
            peer_error: Box::new(ChiaQueryError::PeerRejection("bad request".into())),
            coinset_error: Some(Box::new(ChiaQueryError::CoinsetApiError(
                "rate limited".into(),
            ))),
        };
        assert_ne!(rendered, other.to_string());

        // A tier that was never reached is said to be so, rather than silently omitted.
        let peer_only = ChiaQueryError::AllSourcesFailed {
            peer_error: Box::new(ChiaQueryError::PeerConnection("no route".into())),
            coinset_error: None,
        };
        let peer_only = peer_only.to_string();
        assert!(peer_only.contains("no route"));
        assert!(peer_only.contains("not attempted"), "got: {peer_only}");
    }
}
