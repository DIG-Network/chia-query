# chia-query -- Project Specification

A Rust crate for querying the Chia blockchain with automatic load balancing between decentralized peer connections and the coinset.org HTTP API.

## Overview

`chia-query` provides a unified API for querying the Chia blockchain. It maintains a pool of decentralized full node peer connections as the primary data source and falls back to the coinset.org HTTP API when peer requests fail. The public API mirrors the coinset.org REST API surface, making it a drop-in replacement that prefers decentralized access.

## Architecture

```
                         chia-query Public API
                      (mirrors coinset.org endpoints)
                                 |
                          QueryRouter (load balancer)
                           /              \
                     PeerBackend       CoinsetBackend
                   (preferred)         (fallback)
                        |                   |
                   PeerPool             HTTP Client
                  (5 connections)     (api.coinset.org)
                   /  |  |  \  \
                 Peer Peer Peer Peer Peer
              (via chia-wallet-sdk)
```

### Request Flow

1. Caller invokes a coinset.org-style method (e.g., `get_coin_record_by_name`)
2. `QueryRouter` translates the request and dispatches it to `PeerBackend`
3. `PeerBackend` selects a peer from the pool and executes the request via the Chia peer protocol
4. On success, the response is translated back to the coinset.org response format and returned
5. On failure, the failed peer is ejected from the pool, a replacement peer is connected, and the request is retried once on a different peer
6. If peer retry also fails (or no healthy peers available), the request falls back to `CoinsetBackend`
7. `CoinsetBackend` makes an HTTP POST to `api.coinset.org` and returns the response

## Public API

The crate exposes a single `ChiaQuery` struct. All methods are async and return `Result<T, ChiaQueryError>`. The API surface matches the coinset.org REST API -- every endpoint becomes a method with the same name, request parameters as struct fields, and response types matching the JSON shapes.

### Construction

```rust
use chia_query::{ChiaQuery, ChiaQueryConfig, NetworkType};

// Default config: mainnet, 5 peers, coinset.org fallback enabled
let client = ChiaQuery::new(ChiaQueryConfig {
    network: NetworkType::Mainnet,
    max_peers: 5,
    coinset_base_url: "https://api.coinset.org".to_string(),
    coinset_fallback_enabled: true,
    cert_path: "~/.chia/mainnet/config/ssl/wallet/wallet_node.crt".into(),
    key_path: "~/.chia/mainnet/config/ssl/wallet/wallet_node.key".into(),
    peer_connect_timeout: Duration::from_secs(8),
    peer_request_timeout: Duration::from_secs(30),
    coinset_request_timeout: Duration::from_secs(30),
}).await?;
```

### Endpoint Methods

All methods are `async` and return `Result<Response, ChiaQueryError>`.

#### Blocks

| Method | Request Fields | Response |
|--------|---------------|----------|
| `get_additions_and_removals(header_hash)` | `header_hash: Bytes32` | `AdditionsAndRemovals { additions, removals }` |
| `get_block(header_hash)` | `header_hash: Bytes32` | `FullBlock` |
| `get_block_count_metrics()` | none | `BlockCountMetrics` |
| `get_block_record(header_hash)` | `header_hash: Bytes32` | `BlockRecord` |
| `get_block_record_by_height(height)` | `height: u32` | `BlockRecord` |
| `get_block_records(start, end)` | `start: u32, end: u32` | `Vec<BlockRecord>` |
| `get_block_spends(header_hash)` | `header_hash: Bytes32` | `Vec<CoinSpend>` |
| `get_block_spends_with_conditions(header_hash)` | `header_hash: Bytes32` | `Vec<CoinSpendWithConditions>` |
| `get_blocks(start, end, exclude_header_hash, exclude_reorged)` | `start: u32, end: u32, exclude_header_hash: bool, exclude_reorged: bool` | `Vec<FullBlock>` |
| `get_unfinished_block_headers()` | none | `Vec<UnfinishedBlockHeader>` |

#### Coins

| Method | Request Fields | Response |
|--------|---------------|----------|
| `get_coin_record_by_name(name)` | `name: Bytes32` | `CoinRecord` |
| `get_coin_records_by_hint(hint, start_height, end_height, include_spent_coins)` | `hint: Bytes32, start_height: Option<u32>, end_height: Option<u32>, include_spent_coins: bool` | `Vec<CoinRecord>` |
| `get_coin_records_by_hints(hints, start_height, end_height, include_spent_coins)` | `hints: Vec<Bytes32>, ...` | `Vec<CoinRecord>` |
| `get_coin_records_by_names(names, start_height, end_height, include_spent_coins)` | `names: Vec<Bytes32>, ...` | `Vec<CoinRecord>` |
| `get_coin_records_by_parent_ids(parent_ids, start_height, end_height, include_spent_coins)` | `parent_ids: Vec<Bytes32>, ...` | `Vec<CoinRecord>` |
| `get_coin_records_by_puzzle_hash(puzzle_hash, start_height, end_height, include_spent_coins)` | `puzzle_hash: Bytes32, ...` | `Vec<CoinRecord>` |
| `get_coin_records_by_puzzle_hashes(puzzle_hashes, start_height, end_height, include_spent_coins)` | `puzzle_hashes: Vec<Bytes32>, ...` | `Vec<CoinRecord>` |
| `get_memos_by_coin_name(name)` | `name: Bytes32` | `Vec<Bytes>` |
| `get_puzzle_and_solution(coin_id, height)` | `coin_id: Bytes32, height: Option<u32>` | `CoinSpend` |
| `get_puzzle_and_solution_with_conditions(coin_id, height)` | `coin_id: Bytes32, height: Option<u32>` | `CoinSpendWithConditions` |
| `push_tx(spend_bundle)` | `spend_bundle: SpendBundle` | `TxStatus` |

#### Fees

| Method | Request Fields | Response |
|--------|---------------|----------|
| `get_fee_estimate(spend_bundle, target_times, spend_count)` | `spend_bundle: Option<SpendBundle>, target_times: Option<Vec<u64>>, spend_count: Option<u64>` | `FeeEstimate` |

#### Full Node

| Method | Request Fields | Response |
|--------|---------------|----------|
| `get_aggsig_additional_data()` | none | `Bytes32` |
| `get_network_info()` | none | `NetworkInfo` |
| `get_blockchain_state()` | none | `BlockchainState` |
| `get_network_space(newer_block_header_hash, older_block_header_hash)` | `newer: Bytes32, older: Bytes32` | `u128` |

#### Mempool

| Method | Request Fields | Response |
|--------|---------------|----------|
| `get_all_mempool_items()` | none | `HashMap<String, MempoolItem>` |
| `get_all_mempool_tx_ids()` | none | `Vec<String>` |
| `get_mempool_item_by_tx_id(tx_id)` | `tx_id: String` | `MempoolItem` |
| `get_mempool_items_by_coin_name(coin_name, include_spent_coins)` | `coin_name: Bytes32, include_spent_coins: Option<bool>` | `Vec<MempoolItem>` |

## Internal Components

### QueryRouter

The central dispatcher. For each request:

1. Check if `PeerBackend` has healthy peers available
2. If yes, dispatch to `PeerBackend`
3. If `PeerBackend` returns an error, eject the failed peer, retry with a different peer
4. If the retry also fails (or no peers available), dispatch to `CoinsetBackend`
5. If coinset fallback is disabled and peers fail, return the peer error

### PeerBackend

Translates coinset.org-style requests into Chia peer protocol messages using `chia-wallet-sdk`.

**Protocol mapping** (coinset.org endpoint -> peer protocol message):

| coinset.org Endpoint | Peer Protocol Method |
|---------------------|---------------------|
| `get_coin_record_by_name` | `request_coin_state` |
| `get_coin_records_by_puzzle_hash` | `request_puzzle_state` |
| `get_coin_records_by_puzzle_hashes` | `request_puzzle_state` (batched) |
| `get_coin_records_by_hint` | `request_puzzle_state` (with hints) |
| `get_coin_records_by_parent_ids` | `request_coin_state` (batched) |
| `get_coin_records_by_names` | `request_coin_state` (batched) |
| `get_puzzle_and_solution` | `request_puzzle_and_solution` |
| `get_block_record_by_height` | `request_block_header` |
| `get_fee_estimate` | `request_fee_estimates` |
| `push_tx` | `send_transaction` |

Some endpoints have no direct peer protocol equivalent (e.g., `get_all_mempool_items`, `get_block_count_metrics`, `get_block_spends_with_conditions`). These **always route to CoinsetBackend** since the peer protocol does not support these queries.

### CoinsetBackend

A thin HTTP client wrapper around `api.coinset.org`. Each method:

1. Serializes the request to JSON
2. POSTs to `https://api.coinset.org/{endpoint}`
3. Deserializes the JSON response
4. Maps `{ "success": false, "error": "..." }` to `ChiaQueryError::CoinsetError`

Uses `reqwest` with connection pooling enabled.

### PeerPool

Maintains a pool of exactly 5 (configurable) active peer connections.

#### State

```rust
struct PeerPool {
    peers: Vec<PeerEntry>,
    max_peers: usize,
    tls: Connector,
    network: NetworkType,
}

struct PeerEntry {
    peer: Peer,
    address: SocketAddr,
    connected_at: Instant,
}
```

#### Lifecycle

1. **Initialization**: On `ChiaQuery::new()`, discover peers via DNS introducers and connect to `max_peers` peers concurrently
2. **Peer Selection**: Round-robin across healthy peers for request distribution
3. **Ejection**: When a peer request fails or a connection drops, immediately remove the peer from the pool
4. **Replacement**: After ejection, spawn a background task to connect a new random peer to maintain pool size
5. **Shutdown**: On `ChiaQuery::drop()`, close all peer connections

#### DNS Introducers

Mainnet:
- `dns-introducer.chia.net`
- `chia.ctrlaltdel.ch`
- `seeder.dexie.space`
- `chia.hoffmang.com`

Default port: `8444`

Testnet11:
- `dns-introducer-testnet11.chia.net`

Default port: `58444`

#### Connection Logic (from DataLayer-Driver)

1. Resolve all introducer DNS names to socket addresses
2. Shuffle the resolved addresses for randomness
3. Attempt connections in batches of 10, with 8-second timeout per attempt
4. Use `chia-wallet-sdk`'s `Peer::new()` with a TLS connector loaded from the Chia SSL cert/key files
5. Return the first successful connection

#### TLS

Uses the standard Chia wallet TLS certificates:
- Mainnet: `~/.chia/mainnet/config/ssl/wallet/wallet_node.crt` and `wallet_node.key`
- Testnet11: `~/.chia/testnet11/config/ssl/wallet/wallet_node.key` and `wallet_node.key`

Loaded via `chia-wallet-sdk`'s `load_ssl_cert()` and `create_native_tls_connector()`.

## Error Handling

```rust
enum ChiaQueryError {
    /// Peer protocol returned a rejection (e.g., RejectCoinState)
    PeerRejection(String),

    /// Peer connection or communication error
    PeerConnection(String),

    /// All peers failed and coinset.org also failed (or is disabled)
    AllSourcesFailed {
        peer_error: Box<ChiaQueryError>,
        coinset_error: Option<Box<ChiaQueryError>>,
    },

    /// coinset.org returned { "success": false, "error": "..." }
    CoinsetApiError(String),

    /// HTTP transport error talking to coinset.org
    CoinsetHttp(reqwest::Error),

    /// Request cannot be served by peers (no protocol equivalent),
    /// and coinset.org fallback is disabled
    UnsupportedWithoutCoinset(String),

    /// Invalid request parameters
    InvalidRequest(String),

    /// TLS certificate loading failed
    TlsError(String),

    /// DNS resolution failed for all introducers
    PeerDiscoveryFailed,
}
```

## Crate Dependencies

```toml
[dependencies]
chia = "0.26"                        # Chia protocol types (Bytes32, Coin, CoinSpend, etc.)
chia-wallet-sdk = { version = "0.30", features = ["native-tls"] }  # Peer connections, TLS
tokio = { version = "1", features = ["full"] }  # Async runtime
reqwest = { version = "0.12", features = ["json"] }  # HTTP client for coinset.org
serde = { version = "1", features = ["derive"] }  # Serialization
serde_json = "1"                     # JSON parsing
thiserror = "2"                      # Error derive
rand = "0.8"                         # Shuffling peer addresses
futures-util = "0.3"                 # FuturesUnordered for concurrent connections

[target.'cfg(target_os = "linux")'.dependencies]
openssl = { version = "0.10", features = ["vendored"] }
openssl-sys = { version = "0.9", features = ["vendored"] }
```

## Module Structure

```
chia-query/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # Public API: ChiaQuery, ChiaQueryConfig, re-exports
│   ├── types/
│   │   ├── mod.rs              # Type re-exports
│   │   ├── request.rs          # Request parameter structs (mirror coinset.org JSON)
│   │   ├── response.rs         # Response structs (CoinRecord, BlockRecord, etc.)
│   │   └── error.rs            # ChiaQueryError enum
│   ├── router.rs               # QueryRouter: peer-first dispatch with coinset fallback
│   ├── peer/
│   │   ├── mod.rs              # PeerBackend: translates API calls to peer protocol
│   │   ├── pool.rs             # PeerPool: maintains 5 connections, ejection, replacement
│   │   ├── connect.rs          # DNS discovery, TLS setup, connect_random logic
│   │   └── translate.rs        # Peer protocol response -> coinset.org response format
│   └── coinset/
│       ├── mod.rs              # CoinsetBackend: HTTP wrapper
│       └── client.rs           # reqwest client, request/response serialization
```

## Configuration

```rust
struct ChiaQueryConfig {
    /// Mainnet or Testnet11
    network: NetworkType,

    /// Maximum number of peers to maintain in the pool (default: 5)
    max_peers: usize,

    /// Base URL for coinset.org API (default: "https://api.coinset.org")
    coinset_base_url: String,

    /// Whether to fall back to coinset.org when peers fail (default: true)
    coinset_fallback_enabled: bool,

    /// Path to Chia TLS certificate file
    cert_path: PathBuf,

    /// Path to Chia TLS key file
    key_path: PathBuf,

    /// Timeout for peer connection attempts (default: 8s)
    peer_connect_timeout: Duration,

    /// Timeout for individual peer requests (default: 30s)
    peer_request_timeout: Duration,

    /// Timeout for coinset.org HTTP requests (default: 30s)
    coinset_request_timeout: Duration,
}
```

## Behavioral Rules

1. **Peers first**: Every request that has a peer protocol equivalent goes to `PeerBackend` first
2. **Single retry on peer failure**: If a peer request fails, eject that peer, try one more peer. If that also fails, fall back to coinset.org
3. **Immediate ejection**: Any peer that fails a request or whose connection drops is removed from the pool immediately
4. **Background replacement**: After ejecting a peer, spawn an async task to connect a new random peer -- do not block the current request
5. **Pool size invariant**: The pool always targets `max_peers` connections. If below target, background tasks work to replenish
6. **Coinset-only endpoints**: Endpoints with no peer protocol equivalent (mempool queries, block count metrics, block spends with conditions, unfinished block headers) always go directly to `CoinsetBackend`. If coinset fallback is disabled, these return `ChiaQueryError::UnsupportedWithoutCoinset`
7. **Thread safety**: `ChiaQuery` is `Send + Sync` -- all internal state is behind `Arc<Mutex<_>>` or `Arc<RwLock<_>>` as appropriate

## Usage Example

```rust
use chia_query::{ChiaQuery, ChiaQueryConfig, NetworkType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;

    // Query a coin record -- tries peers first, falls back to coinset.org
    let coin_id = "0xabcdef...".parse()?;
    let record = client.get_coin_record_by_name(coin_id).await?;
    println!("Coin spent: {}", record.spent);

    // Query puzzle and solution
    let spend = client.get_puzzle_and_solution(coin_id, Some(1234567)).await?;
    println!("Puzzle: {:?}", spend.puzzle_reveal);

    // Get fee estimate -- works via peers or coinset.org
    let fees = client.get_fee_estimate(None, Some(vec![60, 120, 300]), None).await?;
    println!("Fee estimates: {:?}", fees.estimates);

    // Get mempool items -- always goes to coinset.org (no peer equivalent)
    let mempool = client.get_all_mempool_tx_ids().await?;
    println!("Mempool size: {}", mempool.len());

    Ok(())
}
```
