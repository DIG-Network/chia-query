# chia-query

Rust crate for querying the Chia blockchain. Maintains a pool of decentralized peer connections as the primary data source and falls back to the coinset.org HTTP API when peers fail.

All hash parameters and return values use `0x`-prefixed hex strings. All methods are `async` and return `Result<T, ChiaQueryError>`.

## Installation

```toml
[dependencies]
chia-query = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Initialization

```rust
use chia_query::{ChiaQuery, ChiaQueryConfig, NetworkType};
use std::time::Duration;

// Default: mainnet, 5 peers, coinset.org fallback enabled
let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;

// Custom configuration
let client = ChiaQuery::new(ChiaQueryConfig {
    network: NetworkType::Testnet11,
    max_peers: 3,
    coinset_base_url: "https://api.coinset.org".into(),
    coinset_fallback_enabled: true,
    cert_path: "/path/to/wallet_node.crt".into(),
    key_path: "/path/to/wallet_node.key".into(),
    peer_connect_timeout: Duration::from_secs(8),
    peer_request_timeout: Duration::from_secs(30),
    coinset_request_timeout: Duration::from_secs(30),
}).await?;
```

TLS certificates auto-generate if the files don't exist. Set `TRUSTED_FULLNODE` env var to an IP address to prioritize a specific peer. Localhost (`127.0.0.1`) is always tried before DNS discovery.

## Types

### Coin

```rust
pub struct Coin {
    pub parent_coin_info: String, // 0x-prefixed hex, 32 bytes
    pub puzzle_hash: String,      // 0x-prefixed hex, 32 bytes
    pub amount: u64,              // mojos
}
```

### CoinRecord

```rust
pub struct CoinRecord {
    pub coin: Coin,
    pub confirmed_block_index: u32,
    pub spent_block_index: u32,    // 0 if unspent
    pub spent: bool,
    pub coinbase: bool,
    pub timestamp: u64,            // unix seconds
}
```

### CoinSpend

```rust
pub struct CoinSpend {
    pub coin: Coin,
    pub puzzle_reveal: String, // 0x-prefixed hex, serialized CLVM
    pub solution: String,      // 0x-prefixed hex, serialized CLVM
}
```

### SpendBundle

```rust
pub struct SpendBundle {
    pub coin_spends: Vec<CoinSpend>,
    pub aggregated_signature: String, // 0x-prefixed hex, 96 bytes BLS sig
}
```

### BlockRecord

```rust
pub struct BlockRecord {
    pub header_hash: String,
    pub height: u32,
    pub weight: u64,
    pub prev_hash: String,
    pub total_iters: u64,
    pub signage_point_index: u8,
    pub farmer_puzzle_hash: String,
    pub pool_puzzle_hash: String,
    pub timestamp: Option<u64>,
    pub fees: Option<u64>,
    pub extra: serde_json::Value, // additional fields from API
}
```

### Other types

- `FullBlock` -- `serde_json::Value` (deeply nested block structure)
- `FeeEstimate` -- estimates, target_times, current_fee_rate, mempool_size, etc.
- `NetworkInfo` -- network_name, network_prefix, genesis_challenge
- `BlockchainState` -- peak, sync state, difficulty, mempool stats
- `TxStatus` -- status string, success bool
- `AdditionsAndRemovals` -- additions: `Vec<CoinRecord>`, removals: `Vec<CoinRecord>`
- `Condition` -- opcode: `serde_json::Value` (hex string), vars: `Vec<String>`
- `CoinSpendWithConditions` -- coin_spend + parsed conditions
- `MempoolItem` -- `serde_json::Value`

## API Reference

### Coin queries

#### get_coin_record_by_name

Look up a single coin by its ID.

```rust
let record = client.get_coin_record_by_name("0xabcd...").await?;
// record.coin.amount, record.spent, record.confirmed_block_index
```

#### get_coin_records_by_puzzle_hash

Find all coins matching a puzzle hash.

```rust
let records = client.get_coin_records_by_puzzle_hash(
    "0xabcd...",       // puzzle_hash
    Some(1000000),     // start_height (optional)
    Some(2000000),     // end_height (optional)
    false,             // include_spent_coins
).await?;
```

#### get_coin_records_by_puzzle_hashes

Same as above but batched for multiple puzzle hashes.

```rust
let hashes = vec!["0xaaa...".into(), "0xbbb...".into()];
let records = client.get_coin_records_by_puzzle_hashes(
    &hashes, None, None, true,
).await?;
```

#### get_coin_records_by_names

Look up multiple coins by their IDs.

```rust
let names = vec!["0xaaa...".into(), "0xbbb...".into()];
let records = client.get_coin_records_by_names(
    &names, None, None, true,
).await?;
```

#### get_coin_records_by_parent_ids

Find child coins of given parent coin IDs.

```rust
let parents = vec!["0xaaa...".into()];
let records = client.get_coin_records_by_parent_ids(
    &parents, None, None, true,
).await?;
```

#### get_coin_records_by_hint / get_coin_records_by_hints

Find coins by hint (used in CAT and NFT protocols).

```rust
let records = client.get_coin_records_by_hint(
    "0xabcd...", None, None, false,
).await?;
```

#### get_memos_by_coin_name

Get memos associated with a coin. Returns `serde_json::Value`.

```rust
let memos = client.get_memos_by_coin_name("0xabcd...").await?;
```

### Puzzle and solution

#### get_puzzle_and_solution

Get the puzzle reveal and solution for a spent coin. Height is optional -- if omitted, the spent height is auto-resolved from the peer.

```rust
let spend = client.get_puzzle_and_solution(
    "0xabcd...", // coin_id
    Some(500000), // height (optional)
).await?;
// spend.puzzle_reveal, spend.solution
```

#### get_puzzle_and_solution_with_conditions

Same as above but also runs the puzzle against the solution to extract parsed CLVM conditions.

```rust
let result = client.get_puzzle_and_solution_with_conditions(
    "0xabcd...", Some(500000),
).await?;
for cond in &result.conditions {
    // cond.opcode = "0x33" (CREATE_COIN), cond.vars = ["0x..puzzle_hash", "0x..amount"]
}
```

### Transaction broadcast

#### push_tx

Broadcast a spend bundle to the network.

```rust
let status = client.push_tx(&SpendBundle {
    coin_spends: vec![CoinSpend {
        coin: Coin {
            parent_coin_info: "0x...".into(),
            puzzle_hash: "0x...".into(),
            amount: 1000000000000,
        },
        puzzle_reveal: "0xff...".into(),
        solution: "0xff...".into(),
    }],
    aggregated_signature: "0xc0...".into(),
}).await?;
// status.status = "SUCCESS" | "PENDING" | "FAILED"
// status.success = true/false
```

### Block queries

#### get_block / get_block_by_height

Get a full block. Returns `serde_json::Value`.

```rust
let block = client.get_block("0x...header_hash").await?;
let block = client.get_block_by_height(1000000).await?;
// block["reward_chain_block"]["height"].as_u64()
```

#### get_block_record / get_block_record_by_height

Get a block record (lighter than full block).

```rust
let record = client.get_block_record_by_height(1000000).await?;
// record.height, record.weight, record.timestamp, record.farmer_puzzle_hash
```

#### get_block_records

Get a range of block records. Range is `[start, end)`.

```rust
let records = client.get_block_records(1000000, 1000010).await?;
```

#### get_blocks

Get a range of full blocks. Range is `[start, end)`.

```rust
let blocks = client.get_blocks(
    1000000,  // start
    1000005,  // end
    false,    // exclude_header_hash
    false,    // exclude_reorged
).await?;
```

#### get_additions_and_removals

Get all coins created (additions) and spent (removals) in a block.

```rust
let ar = client.get_additions_and_removals("0x...header_hash").await?;
// ar.additions: Vec<CoinRecord> -- coins created
// ar.removals: Vec<CoinRecord> -- coins spent
```

#### get_block_spends

Get all coin spends in a block with puzzle_reveal and solution.

```rust
let spends = client.get_block_spends("0x...header_hash").await?;
for spend in &spends {
    // spend.coin, spend.puzzle_reveal, spend.solution
}
```

#### get_block_spends_with_conditions

Same as above but also extracts parsed CLVM conditions per spend.

```rust
let spends = client.get_block_spends_with_conditions("0x...header_hash").await?;
for s in &spends {
    // s.coin_spend.puzzle_reveal, s.conditions
}
```

#### get_block_count_metrics

```rust
let metrics = client.get_block_count_metrics().await?;
// metrics.compact_blocks, metrics.uncompact_blocks, metrics.hint_count
```

#### get_unfinished_block_headers

```rust
let headers = client.get_unfinished_block_headers().await?;
```

### Fee estimation

#### get_fee_estimate

Estimate fees for target confirmation times (in seconds).

```rust
let estimate = client.get_fee_estimate(
    None,                       // spend_bundle (optional)
    Some(&[60, 120, 300]),      // target_times in seconds
    None,                       // spend_count (optional)
).await?;
// estimate.estimates = [0.0, 0.0, 0.0] -- fee rate per target time
// estimate.current_fee_rate, estimate.mempool_size
```

### Network and state

#### get_network_info

```rust
let info = client.get_network_info().await?;
// info.network_name = "mainnet"
// info.network_prefix = "xch"
// info.genesis_challenge = "0xccd5bb71..."
```

#### get_aggsig_additional_data

```rust
let data = client.get_aggsig_additional_data().await?;
// "0xccd5bb71..." (same as genesis_challenge on mainnet)
```

#### get_blockchain_state

```rust
let state = client.get_blockchain_state().await?;
// state.peak.unwrap().height
// state.sync.unwrap().synced
// state.difficulty, state.mempool_size
```

#### get_network_space

```rust
let space = client.get_network_space(
    "0x...newer_header_hash",
    "0x...older_header_hash",
).await?;
```

### Mempool

#### get_all_mempool_items

```rust
let items = client.get_all_mempool_items().await?;
// HashMap<String, serde_json::Value> keyed by transaction ID
```

#### get_all_mempool_tx_ids

```rust
let tx_ids = client.get_all_mempool_tx_ids().await?;
```

#### get_mempool_item_by_tx_id

```rust
let item = client.get_mempool_item_by_tx_id("0x...").await?;
```

#### get_mempool_items_by_coin_name

```rust
let items = client.get_mempool_items_by_coin_name("0x...", Some(true)).await?;
```

### Convenience

#### wait_for_confirmation

Poll until a coin is confirmed on-chain.

```rust
use std::time::Duration;

let record = client.wait_for_confirmation(
    "0xabcd...",                   // coin_id
    Duration::from_secs(5),        // poll interval
    Duration::from_secs(300),      // timeout
).await?;
// Returns CoinRecord once confirmed_block_index > 0
// Errors with timeout if not confirmed within the deadline
```

## Routing behavior

Every request tries decentralized peers first (two attempts on different peers), then falls back to coinset.org. Peer connections are maintained in a pool of up to `max_peers` (default 5). Failed peers are ejected and replaced in the background.

| Source | Endpoints |
|---|---|
| Peer first, coinset fallback | All coin queries, puzzle_and_solution, fee_estimate, push_tx, block_record_by_height, block_records, additions_and_removals, block_spends, blocks |
| Constants (no network call) | get_network_info, get_aggsig_additional_data |
| Coinset only | get_block_count_metrics, get_block_record (by hash), get_unfinished_block_headers, get_memos_by_coin_name, get_network_space, all mempool endpoints |

## Error handling

```rust
use chia_query::ChiaQueryError;

match client.get_coin_record_by_name("0x...").await {
    Ok(record) => { /* use record */ }
    Err(ChiaQueryError::PeerRejection(msg)) => { /* peer rejected */ }
    Err(ChiaQueryError::PeerConnection(msg)) => { /* connection/timeout */ }
    Err(ChiaQueryError::CoinsetApiError(msg)) => { /* coinset returned error */ }
    Err(ChiaQueryError::CoinsetHttp(e)) => { /* HTTP transport error */ }
    Err(ChiaQueryError::AllSourcesFailed { .. }) => { /* both peer and coinset failed */ }
    Err(ChiaQueryError::InvalidRequest(msg)) => { /* bad input */ }
    Err(ChiaQueryError::TlsError(msg)) => { /* certificate issue */ }
    Err(ChiaQueryError::PeerDiscoveryFailed) => { /* no peers found */ }
    Err(ChiaQueryError::UnsupportedWithoutCoinset(ep)) => { /* coinset disabled, endpoint needs it */ }
}
```

## License

MIT
