//! Fail-closed conversion between chia-query's own String-hex response types and the
//! [`dig_chainsource_interface`] `chia_protocol` types, plus the [`ChiaQueryError`] ->
//! [`ChainSourceError`] mapping.
//!
//! Every conversion fails closed: an unparseable hex field becomes
//! [`ChainSourceError::Malformed`], NEVER a silent zero/default that could misrepresent chain state
//! to a money-routing consumer (SPEC §3).

use chia_protocol::{Bytes32, Coin, CoinSpend, Program};
use dig_chainsource_interface::{ChainSourceError, CoinRecord as IfaceCoinRecord};

use crate::types::{
    ChiaQueryError, Coin as ChqCoin, CoinRecord as ChqCoinRecord, CoinSpend as ChqCoinSpend,
};

/// Parses a `0x`-prefixed (or bare) hex string into a [`Bytes32`], failing closed with
/// [`ChainSourceError::Malformed`] on any decode/length error.
pub(crate) fn parse_bytes32(hex_str: &str) -> Result<Bytes32, ChainSourceError> {
    let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(trimmed)
        .map_err(|e| ChainSourceError::Malformed(format!("invalid hex `{hex_str}`: {e}")))?;
    let arr: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
        ChainSourceError::Malformed(format!("expected 32 bytes, got {}", v.len()))
    })?;
    Ok(Bytes32::new(arr))
}

/// Parses a `0x`-prefixed (or bare) hex string into serialized-CLVM [`Program`] bytes, failing
/// closed with [`ChainSourceError::Malformed`] on a decode error.
fn parse_program(hex_str: &str) -> Result<Program, ChainSourceError> {
    let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(trimmed)
        .map_err(|e| ChainSourceError::Malformed(format!("invalid program hex: {e}")))?;
    Ok(Program::from(bytes))
}

/// Renders a [`Bytes32`] as the `0x`-prefixed hex string chia-query's async API expects as input.
pub(crate) fn bytes32_to_hex(value: Bytes32) -> String {
    format!("0x{}", hex::encode(value))
}

/// Rebuilds a `chia_protocol` [`Coin`] from chia-query's String-hex coin, failing closed on a
/// malformed hash.
fn coin_from_chq(coin: &ChqCoin) -> Result<Coin, ChainSourceError> {
    Ok(Coin::new(
        parse_bytes32(&coin.parent_coin_info)?,
        parse_bytes32(&coin.puzzle_hash)?,
        coin.amount,
    ))
}

/// Converts a chia-query [`CoinRecord`](ChqCoinRecord) into the interface's
/// [`CoinRecord`](IfaceCoinRecord).
///
/// A `0` height/timestamp in chia-query's flat encoding means "not set"; it maps to `None` rather
/// than `Some(0)`, matching the interface's "not known by this source" semantics.
pub(crate) fn coin_record_from_chq(
    record: &ChqCoinRecord,
) -> Result<IfaceCoinRecord, ChainSourceError> {
    Ok(IfaceCoinRecord {
        coin: coin_from_chq(&record.coin)?,
        confirmed_height: none_if_zero_u32(record.confirmed_block_index),
        spent_height: record.spent.then_some(record.spent_block_index),
        timestamp: none_if_zero_u64(record.timestamp),
        coinbase: record.coinbase,
    })
}

/// Converts a chia-query [`CoinSpend`](ChqCoinSpend) into the interface's `chia_protocol`
/// [`CoinSpend`], failing closed on a malformed coin, puzzle, or solution.
pub(crate) fn coin_spend_from_chq(spend: &ChqCoinSpend) -> Result<CoinSpend, ChainSourceError> {
    Ok(CoinSpend::new(
        coin_from_chq(&spend.coin)?,
        parse_program(&spend.puzzle_reveal)?,
        parse_program(&spend.solution)?,
    ))
}

fn none_if_zero_u32(value: u32) -> Option<u32> {
    (value != 0).then_some(value)
}

fn none_if_zero_u64(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

/// Maps chia-query's transport/routing error onto the interface's typed error.
///
/// Every [`ChiaQueryError`] means "could not reliably answer", so every mapping is an `Err` a
/// consumer fails closed on — NEVER an `Ok(None)`. The mapping only classifies the *reason*
/// (transport / malformed / unsupported / timeout) for diagnostics; see SPEC §3 for the table.
pub(crate) fn map_query_error(error: ChiaQueryError) -> ChainSourceError {
    match error {
        // A deadline elapsed anywhere in the router surfaces as a timed-out connection string;
        // classify it as a timeout so consumers can reason about retryability.
        ChiaQueryError::PeerConnection(msg) if is_timeout(&msg) => ChainSourceError::Timeout,

        ChiaQueryError::PeerConnection(msg) => ChainSourceError::Transport(msg),
        ChiaQueryError::PeerRejection(msg) => ChainSourceError::Transport(msg),
        ChiaQueryError::PeerDiscoveryFailed => {
            ChainSourceError::Transport("peer discovery failed".to_string())
        }
        ChiaQueryError::TlsError(msg) => ChainSourceError::Transport(format!("TLS: {msg}")),
        ChiaQueryError::CoinsetHttp(msg) => ChainSourceError::Transport(msg),
        ChiaQueryError::CoinsetApiError(msg) => ChainSourceError::Transport(msg),
        ChiaQueryError::AllSourcesFailed { peer_error, .. } => {
            ChainSourceError::Transport(format!("all sources failed: {peer_error}"))
        }
        // We sent an input the backend rejected as malformed (e.g. bad hex) — the read is
        // untrustworthy, so fail closed as malformed.
        ChiaQueryError::InvalidRequest(msg) => ChainSourceError::Malformed(msg),
        ChiaQueryError::UnsupportedWithoutCoinset(_) => {
            ChainSourceError::Unsupported("operation requires the coinset fallback")
        }
        // Neither of these is an answer about the chain: one says the sources could not be brought
        // to agree that something is absent, the other that they actively contradict each other.
        // Both are Transport for the same reason every other variant here is — the consumer must
        // fail closed and retry, never read "unknown" as "not there" (dig_ecosystem#2456).
        ChiaQueryError::UncorroboratedAbsence(msg) => ChainSourceError::Transport(msg),
        ChiaQueryError::SourcesDisagree(msg) => {
            ChainSourceError::Transport(format!("sources disagree: {msg}"))
        }
    }
}

/// Whether an error message names a timeout, so `PeerConnection("... timed out")` maps to
/// [`ChainSourceError::Timeout`] rather than a generic transport error.
fn is_timeout(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("timed out") || lower.contains("timeout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Coin as ChqCoin;

    fn hex32(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 32]))
    }

    fn chq_record(spent: bool) -> ChqCoinRecord {
        ChqCoinRecord {
            coin: ChqCoin {
                parent_coin_info: hex32(0x11),
                puzzle_hash: hex32(0x22),
                amount: 5,
            },
            confirmed_block_index: 100,
            spent_block_index: if spent { 200 } else { 0 },
            spent,
            coinbase: true,
            timestamp: 1_700_000_000,
        }
    }

    // ---- Test #1: type conversion is fail-closed ----

    #[test]
    fn coin_record_conversion_maps_heights_and_zero_to_none() {
        let unspent = coin_record_from_chq(&chq_record(false)).unwrap();
        assert_eq!(unspent.confirmed_height, Some(100));
        assert_eq!(unspent.spent_height, None);
        assert_eq!(unspent.timestamp, Some(1_700_000_000));
        assert!(unspent.coinbase);

        let spent = coin_record_from_chq(&chq_record(true)).unwrap();
        assert_eq!(spent.spent_height, Some(200));
        assert!(spent.is_spent());
    }

    #[test]
    fn zero_confirmed_height_becomes_none_not_some_zero() {
        let mut record = chq_record(false);
        record.confirmed_block_index = 0;
        record.timestamp = 0;
        let converted = coin_record_from_chq(&record).unwrap();
        assert_eq!(converted.confirmed_height, None);
        assert_eq!(converted.timestamp, None);
    }

    #[test]
    fn malformed_hash_fails_closed_never_defaults_to_zero() {
        let mut record = chq_record(false);
        record.coin.puzzle_hash = "not-hex".to_string();
        let err = coin_record_from_chq(&record).unwrap_err();
        assert!(matches!(err, ChainSourceError::Malformed(_)));
    }

    #[test]
    fn short_hash_fails_closed() {
        let mut record = chq_record(false);
        record.coin.parent_coin_info = "0x1234".to_string();
        assert!(matches!(
            coin_record_from_chq(&record),
            Err(ChainSourceError::Malformed(_))
        ));
    }

    #[test]
    fn bytes32_roundtrips_through_hex() {
        let b = parse_bytes32(&hex32(0xAB)).unwrap();
        assert_eq!(bytes32_to_hex(b), hex32(0xAB));
    }

    // ---- Test #1: error mapping never collapses to absence ----

    #[test]
    fn transport_error_maps_to_transport_never_none() {
        let mapped = map_query_error(ChiaQueryError::PeerConnection("socket reset".into()));
        assert_eq!(mapped, ChainSourceError::Transport("socket reset".into()));
    }

    #[test]
    fn timeout_message_maps_to_timeout() {
        let mapped = map_query_error(ChiaQueryError::PeerConnection("request timed out".into()));
        assert_eq!(mapped, ChainSourceError::Timeout);
    }

    #[test]
    fn invalid_request_maps_to_malformed() {
        let mapped = map_query_error(ChiaQueryError::InvalidRequest("bad hex".into()));
        assert!(matches!(mapped, ChainSourceError::Malformed(_)));
    }

    #[test]
    fn unsupported_maps_to_unsupported() {
        let mapped = map_query_error(ChiaQueryError::UnsupportedWithoutCoinset("get_x".into()));
        assert!(matches!(mapped, ChainSourceError::Unsupported(_)));
    }

    #[test]
    fn all_sources_failed_maps_to_transport() {
        let mapped = map_query_error(ChiaQueryError::AllSourcesFailed {
            peer_error: Box::new(ChiaQueryError::PeerRejection("nope".into())),
            coinset_error: None,
        });
        assert!(matches!(mapped, ChainSourceError::Transport(_)));
    }
}
