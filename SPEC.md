# chia-query -- Project Specification

A Rust crate for querying the Chia blockchain with automatic load balancing between decentralized peer connections and the coinset.org HTTP API

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

// Default config: mainnet, 8 peers, coinset.org fallback enabled
let client = ChiaQuery::new(ChiaQueryConfig {
    network: NetworkType::Mainnet,
    max_peers: 8,
    coinset_base_url: "https://api.coinset.org".to_string(),
    coinset_fallback_enabled: true,
    tls_identity: TlsIdentity::Generated,
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

#### Client state

| Method | Request Fields | Response |
|--------|---------------|----------|
| `peer_count()` | none | `usize` |
| `independent_peer_count()` | none | `usize` |
| `peer_peak_height()` | none | `Option<u32>` |

`peer_count()` MUST report the number of peer connections the pool HOLDS at the moment of the
call. It MUST NOT report `max_peers`: a client that is still filling reports the smaller number,
and reaches the target only when the target is met. A consumer surfacing this to a user is
stating a fact about that machine, so a configured intention must never substitute for the
measurement.

`independent_peer_count()` MUST report only the held peers reached through DNS discovery — see
[Origin: preferred is not independent](#origin-preferred-is-not-independent). It MUST be used in
place of `peer_count()` by any consumer that treats a peer as a corroborating opinion.

A peer leaves the count via `eject_peer`, which runs when a request to that peer fails. The count
is therefore of peers held and believed usable, on the same liveness standard `has_peers()` has
always answered by.

`peer_peak_height()` MUST report the peak this client's OWN peers have announced via
`NewPeakWallet`, and MUST make no network call. It is distinct from `peak_height_opt()`, which
answers "what is the chain's peak" and consults coinset FIRST — a third party's view of the chain
even on a client holding peers. An unobserved peak MUST be `None`, never `0`: every block is
trivially above zero, so a leaked `0` silently satisfies any confirmation-depth comparison.

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

A thin, transport-generic client (`CoinsetClient<T: HttpTransport>`) over
`api.coinset.org`. Each method:

1. Serializes the request to JSON
2. POSTs to `{base_url}/{endpoint}` via the injected [transport](#http-transport)
3. Deserializes the JSON response
4. Maps a `success: false` envelope to `ChiaQueryError::CoinsetApiError` (see
   [Error envelopes](#error-envelopes))

#### HTTP transport

The HTTP layer is abstracted behind the `HttpTransport` trait
(`post_json(url, body) -> Value`) so the coinset tier compiles for both native
and wasm:

- **Native** (`native` feature, default): `ReqwestTransport` — a shared
  `reqwest` client with a configurable timeout and connection pooling.
- **wasm** (`coinset` feature, no `native`): `FetchTransport` — an **injected**
  JavaScript `fetch` function (the host passes `window.fetch` / Node's global
  `fetch`). No native HTTP stack reaches the wasm build.

#### Error envelopes

A failing coinset response carries
`{ error, structuredError, traceback, success: false }`. The client resolves a
human-readable message with this precedence:

1. `structuredError` — the newer, stable summary. An object `{ code, data,
   message }` (its `message` is used) or, on some endpoints, a bare string.
2. `error` — the legacy message string.
3. `"unknown error"` — when neither is present.

`traceback` is **opaque** and MUST NOT surface into user-facing output — it is
server-internal detail (stack frames) and is deliberately ignored.

### PeerPool

Maintains a pool of 8 (configurable) active peer connections. Eight is DERIVED, not chosen: see [Pool sizing](#pool-sizing).

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
    origin: PeerOrigin,
    admitted_at: Instant,
}
```

#### Lifecycle

1. **Initialization**: On `ChiaQuery::new()`, discover peers via DNS introducers and connect to `max_peers` peers concurrently
2. **Peer Selection**: Round-robin across healthy peers for request distribution
3. **Ejection**: When a peer request fails or a connection drops, immediately remove the peer from the pool
4. **Replacement**: After ejection, spawn a background task to connect a new random peer to maintain pool size
5. **Shutdown**: On `ChiaQuery::drop()`, close all peer connections

#### Distinct admission

A `SocketAddr` MUST be held by the pool **at most once**. A connection whose address is already
held MUST be rejected, and no receiver handler may be spawned for a rejected connection.

The address check and the insertion MUST occur in the **same critical section**, holding the
write lock. Checking before acquiring the write lock is a time-of-check/time-of-use gap:
initialization races `max_peers` dials concurrently, so two fills of one address can each observe
it absent and each insert. `max_peers` MUST be enforced in that same critical section.

An **ejected** address becomes admissible again; distinctness is a property of the held set, never
a ban.

`priority_addresses` (the `TRUSTED_FULLNODE` address, then `127.0.0.1`) is recomputed on every
dial, so a caller filling a pool MUST pass the addresses it already holds as `exclude`. Without
that, the preference for a local node becomes a monopoly: any process able to bind the port — which
on an unprivileged local account requires no privilege at all — can occupy every slot while the
pool reports itself full.

#### Origin: preferred is not independent

Each held peer records `PeerOrigin`:

| Origin | Reached from |
|---|---|
| `Priority` | `TRUSTED_FULLNODE`, or `127.0.0.1` |
| `Discovered` | a DNS introducer's address set |

`peer_count()` counts connections HELD. `independent_peer_count()` counts only `Discovered`
peers. A caller deciding whether enough separate sources AGREE about the chain MUST use
`independent_peer_count()`: priority ordering expresses speed and operator control, never
authority, and a co-resident node is a source a local attacker can supply. A caller reporting how
many peers a machine holds MUST use `peer_count()`.

#### Pool sizing

`max_peers` defaults to **8**. Every term is a slot that is occupied and is NOT an independent
corroborating voice:

| Term | Why it is not a voice |
|---|---|
| `PRIORITY_SLOTS` = 2 | the dialler tries `TRUSTED_FULLNODE` AND the loopback ahead of discovery, at two distinct socket addresses, so a host running both fills two slots with peers that are not independent voices |
| 1 followed session | a subscriber follows one session's frames; the peer it reads from cannot corroborate itself |
| `QUORUM_SAMPLE` = 4 | the sample an agreement ratio is expressed against |
| 1 of slack | cycling removes a peer before its replacement connects, so one slot is transiently vacant |

`default_max_peers()` is therefore `PRIORITY_SLOTS + 1 + QUORUM_SAMPLE + 1` = 8. A default
of 5 leaves three usable corroborators in the ordinary case, and a default of 7 leaves three
mid-rotation on a host running both priority addresses — both below `QUORUM_SAMPLE`.

#### Corroboration arming: refuse, never degrade

`corroboration_readiness(asked)` reports `Armed` only when at least `CORROBORATION_FLOOR` (2)
`Discovered` peers at addresses OTHER than `asked` are held. Below that it MUST report
`Insufficient`.

It MUST be computed by the same predicate that selects corroborators, never as
`independent_peer_count() - 1`. A blind subtraction charges the answering peer against the
independent set even when it was never in it — a `Priority` peer, an operator's own node or one on
this machine — so a pool holding a full floor of independent voices reports one short and the
answer is downgraded to `Uncorroborated*`. On a host running a full node that is the ordinary case,
not an edge one.

Both corroborating reads consult it BEFORE asking anyone, and both grade the answers against the
same floor afterwards:

| stage | rule |
|---|---|
| before asking | `Insufficient` MUST yield `UncorroboratedFound` / `UncorroboratedAbsent`; no corroboration is attempted |
| after asking | fewer than `CORROBORATION_FLOOR` AGREEING answers MUST yield `UncorroboratedFound` / `UncorroboratedAbsent` |
| any answer | one contradicting answer MUST fail the read with `SourcesDisagree`, whatever the agreement count |

Both stages are required and neither substitutes for the other: the first refuses a read the pool
cannot support, the second catches a pool that held enough voices and then could not get answers
from them.

Reporting an answer as corroborated on fewer voices is FORBIDDEN. Corroborating against whoever
happens to be present converts a four-voice quorum into a one-voice one that still reports itself
corroborated, and no consumer downstream can distinguish the two. An `Uncorroborated*` answer is
not a failure — it is the undecided fact handed up for another tier to settle.

#### Cycling: peers are rotated on AGE

`cycle_expired_peers()` removes the OLDEST `Discovered` entry whose `admitted_at` is at least
`PEER_LIFETIME` (300s) old, one per call, and `maintain()` then refills. This is NC-12's cycling
half: a fixed set is a set an attacker only has to capture once, and a captured peer does not fail,
so failure-driven ejection cannot substitute for it. `Priority` entries are NOT rotated — re-dialling
the same operator address spends a handshake to change nothing.

#### Frame fan-out

A pooled session's inbound frames are fanned out to subscribers over BOUNDED per-subscriber
channels (`subscribe_frames(capacity)`). Every delivered frame is a `SourcedFrame`: a `PoolFrame`
paired with the `FrameSource` that produced it — the peer `SocketAddr` this process DIALLED,
together with a `SessionId`.

- `SessionId` is drawn from a monotonic counter at the moment the pool allocates a session. It is
  never reused, never derived from anything a peer sends, and a reconnect to the same address
  receives a NEW one, so a peer cannot inherit the identity of a session it did not open.
- A session MUST be announced by `PoolFrame::Reset` before its first frame and closed by
  `PoolFrame::SessionEnded` after its last. `Reset` carries no generation of its own; the session
  it announces is named by the `FrameSource` on the frame.
- **A subscriber receives the frames of EVERY held session, not only the one it follows.** `Reset`
  in particular is published to all subscribers, so a subscriber that tracks one peer MUST filter
  on `FrameSource` — matching both address and `SessionId` — before acting on a frame. A subscriber
  that clears its state on any `Reset` will discard it when an unrelated peer reconnects.
- A subscriber whose channel is FULL MUST have its subscription TERMINATED. Dropping a frame and
  continuing is FORBIDDEN: a missed `CoinStateUpdate` is a spend the consumer never learns about, so
  a replica goes on reporting itself synced while reading spent money as present.

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
2. Reduce to DISTINCT addresses. Deduplication MUST NOT rely on `Vec::dedup` after a shuffle:
   `dedup` collapses only adjacent duplicates, so a shuffle immediately before it makes it near
   vacuous and a repeated introducer result can occupy two dial slots
3. Shuffle the distinct addresses for randomness
4. Order the shuffled set IPv6-first (CLAUDE.md §5.2). The ordering is STABLE, so the shuffle
   survives within each class. Locality MUST NOT reorder the discovered set: a local address
   reached through DISCOVERY is refused outright by invariant 5f, and a co-resident node is reached
   only by the priority path, where it is recorded as `Priority` and is not an independent voice
5. Attempt connections in batches of 10, with 8-second timeout per attempt
6. Use `chia-wallet-sdk`'s `Peer::new()` with the TLS connector built from the configured identity
7. Return the first successful connection

If NO peer connects, construction fails with `PeerDiscoveryFailed` ONLY when
`coinset_fallback_enabled` is `false`. With the fallback enabled the client MUST construct with an
empty pool and serve from the coinset tier, refilling the pool in the background: the coinset tier
is plain HTTP requiring neither a credential nor a peer, so a peer-tier failure MUST NOT deny it.

#### TLS

The Chia peer protocol does not treat the client certificate as a credential -- a full node accepts
any well-formed certificate. The identity is therefore selected by `TlsIdentity`:

- `TlsIdentity::Generated` (default) -- a fresh self-signed certificate generated in memory at
  construction via `chia-ssl`'s `ChiaCertificate::generate()`. Nothing is read from or written to
  disk, so a client MUST NOT require a Chia installation. The certificate lives for the process
  lifetime; the peer-visible identity therefore changes on restart, which is immaterial because
  peers are tracked by address.
- `TlsIdentity::Files { cert_path, key_path }` -- a pair on disk, e.g. a real Chia node's
  `wallet_node.crt`/`.key`, loaded via `chia-wallet-sdk`'s `load_ssl_cert()`. This variant MAY
  write: when either path cannot be read, `load_ssl_cert` generates a certificate and persists it
  AND its private key to the two given paths. An implementation MUST NOT assume this variant is
  read-only. When both files exist and are readable no write occurs and no writability is
  required; whenever either path cannot be read, both paths MUST be writable or the call fails.
  Because the fallback triggers when *either* file cannot be read, an unreadable key alongside a
  readable certificate causes the certificate file to be OVERWRITTEN, not merely supplemented.

Both are converted with `create_native_tls_connector()`.

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

    /// HTTP transport error talking to coinset.org (message-only, so the
    /// variant is transport-agnostic and compiles on wasm without reqwest)
    CoinsetHttp(String),

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

The wasm-safe core (`serde`, `serde_json`, `thiserror`, `hex`, `sha2`, `log`)
is always compiled. Everything else is optional and grouped by feature:

```toml
[features]
default = ["native"]
native  = ["dep:chia-protocol", "dep:chia-consensus", "dep:chia-bls",
           "dep:chia-traits", "dep:chia-puzzle-types", "dep:clvm-traits",
           "dep:clvm-utils", "dep:clvmr", "dep:chia-wallet-sdk", "dep:chia-ssl",
           "dep:dig-chainsource-interface", "dep:tokio",
           "dep:tokio-tungstenite", "dep:reqwest", "dep:rand", "dep:futures-util"]
coinset = ["dep:wasm-bindgen", "dep:wasm-bindgen-futures", "dep:js-sys",
           "dep:serde-wasm-bindgen", "dep:getrandom"]
```

- **native** pulls the peer + routing stack: the chia component crates
  (`chia-protocol`, `chia-consensus`, `chia-bls`, `chia-traits`,
  `chia-puzzle-types`, `clvm-traits`, `clvm-utils`), `clvmr`,
  `chia-wallet-sdk` (native-tls), `chia-ssl`, `dig-chainsource-interface`,
  `tokio`, `tokio-tungstenite`, `reqwest`, `rand`, `futures-util`, and
  (linux-only) vendored `openssl`.
  The component crates are taken directly rather than through the `chia`
  umbrella: the umbrella publishes no 0.36 release (it jumps 0.32 to 0.42),
  and 0.36 is the family `chia-wallet-sdk` 0.34 and
  `dig-chainsource-interface` 0.3 are both built against.
  `chia-ssl` is the one exception to that family: it is versioned
  independently and `chia-sdk-client` 0.34 requires `0.42`, so chia-query's
  `chia-ssl` pin tracks `chia-sdk-client`'s rather than the 0.36 family. The
  two MUST agree, or `ChiaCertificate` becomes a different type from the one
  `create_native_tls_connector` accepts.
  `chia-ssl` 0.42 transitively brings a pre-1.0 crypto stack -- `rsa`
  `0.10.0-rc.18`, `pkcs1` `0.8.0-rc.4`, `crypto-bigint` 0.7, `crypto-primes`.
  Those reach only `ChiaCertificate::generate()`, whose sole use of `rsa` is
  key generation and PKCS#8 encoding; no signing, decryption, or
  key-derivation path routes through `rsa` (the certificate's own RSA-SHA256
  signature is produced by `rcgen`), and the certificate they produce protects
  nothing -- the Chia CA private key is a public constant compiled into
  `chia-ssl`. Note that the `rsa` requirement FLOATS: it also matches later
  `0.10.0-rc.N`, so a fresh downstream resolve MAY select a release candidate
  other than the one audited here.
- **coinset** pulls the wasm bindings: `wasm-bindgen`, `wasm-bindgen-futures`,
  `js-sys`, `serde-wasm-bindgen`, and `getrandom` (js backend).

## Module Structure

```
chia-query/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # Public API: ChiaQuery (native), re-exports
│   ├── drift.rs                # coinset API drift detection (shape + diff) — wasm-safe
│   ├── wasm_api.rs             # @dignetwork/chia-query-wasm bindings (wasm coinset build)
│   ├── bin/
│   │   └── coinset_drift.rs    # Drift-monitor binary (native): probe + check/update
│   ├── types/
│   │   ├── mod.rs              # Type re-exports
│   │   ├── response.rs         # Response structs (CoinRecord, BlockRecord, etc.)
│   │   └── error.rs            # ChiaQueryError enum
│   ├── router.rs               # (native) QueryRouter: peer-first dispatch + coinset fallback
│   ├── peer/                   # (native) PeerBackend, pool, connect, ordering, frames, plurality, translate
│   └── coinset/
│       ├── mod.rs              # CoinsetClient<T>: transport-generic REST wrapper
│       └── transport.rs        # HttpTransport trait: ReqwestTransport / FetchTransport
├── tests/
│   ├── parity.rs               # (native, #[ignore]) live peer-vs-coinset comparison
│   └── fixtures/
│       └── coinset-api-snapshot.json   # committed drift baseline
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

    /// Source of the peer TLS client identity (default: `TlsIdentity::Generated`)
    tls_identity: TlsIdentity,

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
5. **Pool size invariant**: The pool always targets `max_peers` connections. The `PeerBackend::read` path maintains the pool on every request, refilling it if below target
5b. **Cycling invariant**: A `Discovered` peer held for `PEER_LIFETIME` or longer MUST be rotated out on AGE, independently of whether any request to it has failed
5c. **Corroboration invariant**: An answer MUST NOT be reported as corroborated — `Found` or `CorroboratedAbsent` — unless at least `CORROBORATION_FLOOR` independent peers other than the answering one AGREED with it. A pool holding fewer than `CORROBORATION_FLOOR` such peers MUST NOT attempt corroboration at all
5d. **Frame invariant**: A subscriber that overflows its bounded channel MUST be terminated, never served a stream with a gap in it
5e. **Frame attribution invariant**: Every frame delivered to a subscriber MUST name the session that produced it, by peer address and session id. A session MUST be announced by `Reset` before its first frame and closed by `SessionEnded` after its last, and a session that ends MUST have its peer ejected rather than left in the pool
5f. **Local-discovery invariant**: An address reached through DNS discovery MUST be refused if it is loopback, private, link-local or unspecified in either family. Only the priority path may reach a host-local node, and what it reaches is recorded as `Priority`
5a. **Distinct-address invariant**: At most one connection per `SocketAddr` is held, decided under the write lock — see [Distinct admission](#distinct-admission). `max_peers` connections therefore mean `max_peers` distinct addresses, which is what makes the count meaningful as a measure of redundancy
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

## Build Targets & Feature Flags

chia-query builds for two targets from one source tree, selected by feature:

| Feature | Target | Surface |
|---------|--------|---------|
| `native` (default) | native | Full client: peer WebSockets + CLVM routing + `reqwest`, racing peers against the coinset fallback (`ChiaQuery`). |
| `coinset` | `wasm32-unknown-unknown` | The coinset.org REST tier only (`coinset::CoinsetClient`), HTTP over an injected `fetch`. |

Build the wasm coinset-only tier with:

```
cargo build --target wasm32-unknown-unknown --no-default-features --features coinset
```

All non-coinset code (the peer backend, `router.rs` CLVM execution,
`chia-wallet-sdk` + `native-tls`, `tokio` networking, `reqwest`, the linux
vendored OpenSSL, `Coin::from_protocol`) is gated behind the `native` feature,
so the wasm build pulls in none of it — no `blst`, no native HTTP, no
`getrandom` leak. A CI job (`wasm-coinset`) asserts this stays clean on every PR.

### `@dignetwork/chia-query-wasm`

`wasm-pack` builds the coinset tier into the npm package
`@dignetwork/chia-query-wasm`, published for BOTH the browser (bundler target)
and Node (nodejs target) so a consumer — notably dig-urn-resolver's chain-verify
— runs in either environment. The bindings (`wasm_api::CoinsetClient`) take an
injected `fetch`:

```js
import { CoinsetClient } from '@dignetwork/chia-query-wasm';
const client = new CoinsetClient('https://api.coinset.org', fetch);
const state = await client.request('get_blockchain_state', {});
```

**Trade-off (single-endpoint trust).** The wasm coinset-only build drops the
peer tier, so a wasm consumer reads from ONE coinset endpoint with no
peer cross-check. This is acceptable because on-chain verification stays
client-side (singleton-lineage / anti-rollback checks layered on top of the raw
reads), the coinset URL is configurable (a consumer may point at its own trusted
node), and the transport abstraction leaves room for a future multi-endpoint
cross-check without an API change.

## coinset.org API Drift Monitor

coinset.org publishes no OpenAPI schema, so a scheduled sentinel watches it for
breaking changes (the automated backstop to the chia-query freshness preflight).

- **Mechanism.** `src/drift.rs` reduces a JSON response to its *type-shape* —
  every key preserved, every scalar replaced by a type tag (`string`,
  `integer`, `number`, `boolean`, `null`), arrays collapsed to a single-element
  shape — discarding all concrete values (which change every block). `diff_shapes`
  reports each added/removed key and each type/structure change between two shapes.
- **Snapshot.** `tests/fixtures/coinset-api-snapshot.json` is the committed
  baseline shape of a representative probe set: `get_blockchain_state`,
  `get_network_info`, `get_block_record_by_height`, and an error-envelope probe
  (`get_coin_record_by_name` on an absent coin — guards the
  `error`/`structuredError`/`traceback` shape).
- **Volatile-peak exclusion.** The `blockchain_state.peak` subtree is dropped
  from the `get_blockchain_state` shape before snapshotting/diffing: the peak is
  the chain tip, and its `fees`/`timestamp`/`prev_transaction_block_hash`/
  `reward_claims_incorporated` fields flip between `null` (non-transaction block)
  and integer/string/array (transaction block) every block, which would produce
  unavoidable false-positive drift. The BlockRecord shape is watched stably by
  the `get_block_record_by_height` probe (height 1 is a permanent transaction
  block with every field populated). The typed `BlockRecord` decoder tolerates
  both peak variants (transaction-block ints/array + non-transaction-block
  nulls), asserted by `types::response` unit tests.
- **Binary.** `coinset_drift check` (native, `required-features = ["native"]`)
  probes live coinset, diffs against the snapshot, and exits non-zero on drift;
  `coinset_drift update` regenerates the snapshot.
- **Workflow.** `.github/workflows/coinset-drift.yml` runs `check` on a daily
  cron + manual dispatch. On drift it files/updates a single deduped
  `coinset-drift` issue (one open issue, updated not duplicated) AND fails loud.
  It is NOT a required merge gate — it watches an external API, so a coinset
  outage never blocks PRs.

## ChainSource Provider Registry (native)

chia-query is the canonical `dig-chainsource-interface` registry: it implements the ONE
`ChainSource` trait over its async router and composes providers under an operator trust model whose
custody view FAILS CLOSED. This surface is native-only (`feature = "native"`, module
`provider_registry`); it never reaches the wasm coinset-only build. It depends on
`dig-chainsource-interface = "0.3"` (crates.io), whose `chia-protocol 0.36.1` unifies with
chia-query's own `chia-protocol 0.36` and with `chia-wallet-sdk 0.34` at a single
`chia-protocol 0.36.x`, so every `Bytes32`/`Coin`/`CoinSpend` crossing the trait boundary is one type.

### The blocking facade (`ChiaQueryProvider`) -- SPEC 7 runtime requirement

`ChiaQueryProvider` presents the synchronous, object-safe `ChainSource` trait over the asynchronous
`QueryRouter`. Each sync method bridges to async via `run_blocking`, which resolves the ambient tokio
context:

- **inside a multi-thread runtime** -- `task::block_in_place(|| handle.block_on(fut))` (does not
  starve the async worker);
- **inside a current-thread runtime** -- blocking would panic, so it returns
  `ChainSourceError::Transport` carrying a message that names the multi-thread requirement (NEVER an
  opaque tokio panic);
- **outside any runtime** -- `handle.block_on(fut)` directly.

A `catch_unwind` backstop converts any residual panic into `ChainSourceError::Transport`, so misuse
never unwinds across the trait boundary. **The facade MUST run on a multi-thread tokio runtime.** A
consumer that is itself async MUST wrap each call in `tokio::task::spawn_blocking` so the blocking
read never runs on an async worker thread.

### The lightweight coinset source (`CoinsetProvider::from_url`)

`CoinsetChainSource` is a second `ChainSource` implementation that serves point-reads DIRECTLY over
the coinset.org HTTP tier, with NO Chia-peer handshake, NO TLS certificate, and NO sync -- for
consumers that need cheap chain point-reads without paying for the full `ChiaQueryProvider` router.
It owns its own multi-thread tokio runtime and bridges each sync read to the async `CoinsetClient`
via the same `run_blocking` facade (the SPEC 7 runtime rules above apply identically).

Construction (native, `feature = "native"`):

- `CoinsetProvider::from_url(coinset_url)` -- builds a registry-ready provider against an explicit
  base URL.
- `CoinsetProvider::from_env()` / `CoinsetChainSource::from_env()` -- reads `$DIG_COINSET_URL`
  (`COINSET_URL_ENV`) when set and non-empty, else `DEFAULT_COINSET_URL` (`https://api.coinset.org`).
  This is the CHAIN-read tier's canonical `$DIG_COINSET_URL` / `--coinset-url` override -- distinct
  from the content-read node ladder.
- `CoinsetChainSource::with_client(client)` -- from an already-built `CoinsetClient` (used with a
  mock transport in tests).

It registers as `ProviderKind::PublicOracle` with `trustless: false` at try-order priority `30`
(coinset is a single public oracle that can lie; the registry's operator-assigned trust + quorum
gates custody, never this source alone). It implements the L00 `ChainSource` trait, so a consumer
hands it to a `ProviderRegistry` by dependency injection like any other source.

Method support -- point-reads served, lineage refused (fail-closed):

| Interface method | Coinset endpoint | Notes |
|---|---|---|
| `coin_record(id)` | `get_coin_record_by_name_opt` | corroborated absence -> `Ok(None)`; failure or uncorroborated absence -> `Err` |
| `coin_records_by_puzzle_hash(ph, spent)` | `get_coin_records_by_puzzle_hash` | list bounded by `MAX_COIN_RECORDS` (100_000) -> oversized is `TooManyRecords` |
| `coin_records_by_parent(id)` | `get_coin_records_by_parent_ids([id], .., true)` | list bounded by `MAX_COIN_RECORDS` |
| `coin_spend(id)` | `get_puzzle_and_solution_opt` | unspent/unknown -> `Ok(None)`; failure -> `Err` |
| `parent_spend(id)` | trait default (`coin_record` + `coin_spend`) | composed point-reads |
| `peak_height()` | `get_blockchain_state` (`.peak.height`) | no peak -> `Ok(None)`; failure -> `Err` |
| `block_timestamp(h)` | `get_block_record_by_height_opt` | no block / no timestamp -> `Ok(None)` |
| `resolve_singleton_lineage(launcher)` | -- | `Unsupported` -- a genuine launcher->tip walk needs the CLVM singleton-shape machinery, not a lightweight point-read; use `ChiaQueryProvider` or walk `parent_spend`. Fails closed as `Err`, NEVER a false `Ok(None)`. |

The same fail-closed `Ok(None)`-vs-`Err` contract and `ChiaQueryError -> ChainSourceError` mapping
below apply; a misbehaving/hostile coinset endpoint answering an unbounded list read fails closed
(`TooManyRecords`) rather than causing unbounded work.

### Fail-closed method mapping (SPEC 3, the money-critical crux)

Every read distinguishes `Ok(None)`/empty-`Vec` (a source that reliably answered "no such thing" --
safe to act on) from `Err` (could not answer -- the consumer MUST fail closed). A
transport/timeout/parse error is NEVER reported as absence; absence is NEVER reported as an error.

| Interface method | Router method | Absence handling |
|---|---|---|
| `coin_record(id)` | `get_coin_record_by_name_opt` | corroborated absence -> `Ok(None)`; failure or uncorroborated absence -> `Err` |
| `coin_records_by_puzzle_hash(ph, spent)` | `get_coin_records_by_puzzle_hash` (start=end=None) | successful empty query -> `Ok(vec![])`; failure -> `Err` |
| `coin_records_by_parent(id)` | `get_coin_records_by_parent_ids([id], None, None, true)` | successful empty query -> `Ok(vec![])`; failure -> `Err` |
| `coin_spend(id)` | `get_coin_spend_opt` | corroborated unspent/unknown -> `Ok(None)`; failure or uncorroborated absence -> `Err` |
| `parent_spend(id)` | trait default (`coin_record` + `coin_spend`) | gap mid-walk -> `Ok(None)`; failure -> `Err` |
| `resolve_singleton_lineage(launcher)` | forward walk over `get_coin_spend_opt` | never launched / fully melted -> `Ok(None)`; failure -> `Err` |
| `peak_height()` | `peak_height_opt` (from `get_blockchain_state`) | no peak -> `Ok(None)`; failure -> `Err` |
| `block_timestamp(h)` | `block_timestamp_opt` (from `get_block_record_by_height_opt`) | no block / no timestamp -> `Ok(None)`; failure -> `Err` |

**Absence-aware read paths (the PREFERRED design).** The pre-existing single-record router methods
collapse provable absence into `Err` (coinset returns `success:true` + a null field, which then fails
to deserialize). New `_opt` read paths were added at every layer to preserve correct custody
semantics -- a legitimately-unspent coin's `coin_spend` is `Ok(None)`, not `Err`, which the singleton
walk requires to find the tip:

- **coinset** -- `post_extract_opt` / `optional_field`: a `success:true` envelope with a `null` (or
  absent) data field is provable absence -> `Ok(None)`; a present-but-unparseable field is `Err`
  (never `Ok(None)`); `success:false` (via `post`) is `Err`.
- **peer** -- `try_get_coin_record_by_name_opt` / `try_get_coin_spend_opt`: a rejected/timed-out
  request is `Err`. A successful `RespondCoinState` with an EMPTY coin-state list (or an unspent
  coin, for spends) is ONE peer's word, NOT provable absence, and the read reports which of the two
  it has via `OptAnswer`:
  - `Found(v)` -- the peer returned the thing AND an independent peer agreed on the chain claim
    inside it (see below).
  - `UncorroboratedFound(v)` -- the peer returned the thing and no independent peer could be
    brought to make the same claim.
  - `CorroboratedAbsent` -- a SECOND peer, at a different address and with
    `PeerOrigin::Discovered`, was asked the same question and also answered empty.
  - `UncorroboratedAbsent` -- only one peer answered empty and no independent peer was available to
    corroborate it.
  A corroborator that CONTRADICTS the first peer (absent, then present) MUST fail the read with
  `SourcesDisagree`; the implementation MUST NOT choose between the two answers. A corroborator that
  fails to answer MUST leave the absence `UncorroboratedAbsent` -- silence is not agreement.
  Corroboration of an ABSENCE is sought from exactly ONE further peer, never from the whole pool: a
  single slow peer MUST NOT be able to stall a read.

  **A POSITIVE answer is corroborated too, and differently.** The coin-id binding
  `SHA256(parent_coin_info || puzzle_hash || amount)` authenticates the coin's IDENTITY and nothing
  else; `created_height`, `spent_height` and `spent` are copied verbatim from the peer's
  `CoinState`, and those are the fields a consumer reads as evidence that money is settled. So the
  positive answer's CHAIN CLAIM -- the projection defined by the `ChainClaim` trait: identity plus
  the height-bearing fields, excluding tier-local fields (`timestamp`, `coinbase`) the peer
  protocol cannot supply -- is put to EVERY independent peer, CONCURRENTLY, and every answer is
  collected before anything is decided:
  - at least one independent peer making the SAME claim -> `Found(v)`;
  - any independent peer making a DIFFERENT claim, or reporting the thing absent ->
    `SourcesDisagree`, whichever peer answered first and however many agreed. A contradiction MUST
    outrank agreement; vote-counting would let an attacker holding two pool slots manufacture a
    fact;
  - nobody available or nobody able to answer -> `UncorroboratedFound(v)`. A corroborator that
    fails is ejected and MUST NOT be read as agreement.
  Querying the corroborators concurrently is normative, not an optimisation: asking them in
  sequence and stopping at the first would let whichever peer is fastest settle a claim about the
  chain.
  The `wait_for_confirmation` "not-found-yet" polling heuristic (which treats
  `PeerRejection`/`CoinsetApiError` as not-found) is NEVER reused for these reads.
- **router** -- `peer_then_coinset_opt`: `Ok(None)` means CORROBORATED absence -- two independent
  sources were asked and both reported the thing missing. A `Found` answer is returned immediately;
  a `CorroboratedAbsent` answer becomes `Ok(None)`; an `UncorroboratedAbsent` answer is put to the
  coinset tier, which either corroborates it (`Ok(None)`), contradicts it (`SourcesDisagree`), or
  cannot answer (`UncorroboratedAbsence`). Absence that only one source will vouch for MUST NOT be
  reported as `Ok(None)`. Symmetrically, an `UncorroboratedFound` answer is put to the coinset
  tier, which either makes the same chain claim (`Ok(Some(v))`), makes a different one or reports
  the thing absent (`SourcesDisagree`), or cannot answer (`UncorroboratedPresence`). A presence
  that only one source will vouch for MUST NOT be reported as `Ok(Some(v))`: a false absence keeps
  a caller polling, while a false presence makes it stop and record a height. Only a peer FAILURE falls through to the retry, so absence is never masked
  by a fallback and a failure is never collapsed into `Ok(None)`.
  **Stated limit:** when NO peer answers at all, the coinset tier is the only source there is and
  its absence is returned as `Ok(None)` on its own -- unchanged behaviour for a coinset-only client.
  What this contract removes is absence resting on an anonymous, unauthenticated peer.

### `ChiaQueryError` -> `ChainSourceError` mapping

| `ChiaQueryError` | `ChainSourceError` |
|---|---|
| `PeerConnection(msg)` naming a timeout | `Timeout` |
| `PeerConnection` / `PeerRejection` / `PeerDiscoveryFailed` / `TlsError` / `CoinsetHttp` / `CoinsetApiError` / `AllSourcesFailed` | `Transport` |
| `UncorroboratedAbsence` / `UncorroboratedPresence` / `SourcesDisagree` | `Transport` (neither is an answer about the chain; the consumer MUST fail closed and retry, never read "unknown" as "not there") |
| `InvalidRequest` (bad input, e.g. malformed hex) | `Malformed` |
| `UnsupportedWithoutCoinset` | `Unsupported` |

**Type conversion is fail-closed.** chia-query's String-hex fields parse to `chia_protocol` types
(`Bytes32`, `Coin`, `Program`, `CoinSpend`); a decode/length failure is `Malformed`, NEVER a silent
zero/default. A `0` height/timestamp maps to `None` (matching "not known by this source"); `spent`
gates `spent_height`.

### Singleton lineage -- a genuine forward walk

`resolve_singleton_lineage` performs a REAL forward walk launcher -> tip using the shared
`dig_chainsource_interface::SingletonLineage` type (no dig-did dependency -- that crate is same-level).
From the launcher spend it derives the eve coin, then follows each singleton recreation hop-by-hop to
the current unspent tip, collecting every member coin id. It is NEVER an echo of a caller-supplied
coin, so `SingletonLineage::contains` MEMBERSHIP is meaningful. Each hop is authenticated by
`singleton_child_from_spend`: the spent coin's puzzle reveal MUST hash to its committed puzzle hash
(`clvm_utils::tree_hash`), and the child is the odd-amount `CREATE_COIN` output of the parent spend
(computed with `chia_protocol::Coin::coin_id`). The parent puzzle MUST additionally have
singleton-family SHAPE -- it is either the one-time SINGLETON LAUNCHER (matched by its full puzzle
hash) or a SINGLETON TOP-LAYER v1.1 puzzle (matched by its uncurried mod hash); any other puzzle that
merely happens to emit an odd `CREATE_COIN` is NOT a singleton recreation and fails closed as
`Malformed`. Because the singleton top layer morphs its recreation output into the singleton wrapper
by construction, asserting the parent is genuine singleton shape is what guarantees the selected child
is singleton-shaped. **Identity binding (beyond shape).** Shape alone proves a hop is *a* singleton,
not *this* one -- so every TOP-LAYER hop additionally binds its curried `singleton_struct.launcher_id`
(recovered by parsing the top layer's `SingletonArgs`) to the walk's `launcher_id`: a mismatch means
the hop belongs to a DIFFERENT singleton and fails closed as `Malformed`, so a shape-valid hop from a
foreign singleton cannot be spliced into this lineage. The launcher hop itself needs no such check --
its coin id IS the `launcher_id`, already bound by the coin-id binding below. The `CREATE_COIN` amount is decoded fail-closed: an amount atom wider than 8
bytes (a value that cannot fit `u64`) is rejected as `Malformed` rather than silently wrapped, so an
overflowing amount can never be misread as a small odd value. Additionally, each hop binds the fetched
spend to the requested coin id (`spend.coin.coin_id() == current`, the launcher spend bound to
`launcher_id`), making the walk cryptographically self-authenticating from the launcher; a mismatch
fails closed as `Malformed`. A revisited coin (cycle) fails closed as `Malformed`. The walk is bounded against
resource-exhaustion DoS by TWO layers, because each generation is a strictly-sequential network
round-trip (one fetch per hop on the coinset path, two on the peer path) with only per-request
timeouts. (1) **Primary — an overall wall-clock deadline** (`WALK_DEADLINE`, 45s) wraps the entire
walk future, so a hostile source serving an ever-advancing chain of DISTINCT (non-repeating)
recreations — which passes the reveal + coin-id binding every hop and never trips the cycle guard —
cannot keep the walk alive indefinitely; exceeding the deadline fails closed as `Timeout` (distinct
from `Malformed`), bounding total network time, CPU, and memory-growth-rate together. (2)
**Belt-and-suspenders — a hop cap** (`MAX_LINEAGE_GENERATIONS`, 100,000, checked before each fetch)
fails closed as `Malformed`; a real singleton (a few thousand hops at most over its lifetime) stays
far below it, and over any real network the deadline stops an adversarial walk long before the cap is
reached. The coin-id binding is load-bearing over EVERY source: the peer transport returns
the genuine spent coin (fetched from its coin state alongside the puzzle/solution) so a peer-sourced
lineage authenticates and resolves — it is never built from a name-only placeholder coin, which would
hash to the wrong id and fail the binding closed.
`Ok(None)` = the launcher never existed or the singleton was fully melted. **Per-hop CLVM
verification is NECESSARY but NOT SUFFICIENT** -- it does not defeat total fabrication; the registry
trust model layered above provides custody soundness.

### Trust model -- the two views (SPEC 5)

Providers are the four kind wrappers `CoinsetProvider` (PublicOracle), `LocalNodeProvider`
(LocalNode; compose over the SPEC 5.3 `dig.local` -> `localhost` ladder), `DigPeersProvider`
(DigPeers), `CustomProvider` (operator override) -- each wraps any `ChainSource` and labels it with a
`ProviderKind`. The operator assigns each a `TrustLevel` at registration (default per kind:
`LocalNode` -> `Trusted`; every other kind -> `Untrusted`). A provider's self-declared
`ProviderInfo.trustless` flag is ADVISORY ONLY -- it never grants custody trust.

- **`registry.trusted()` -- the CUSTODY view.** A read is satisfied ONLY by (a) an operator-`Trusted`
  source (the gold standard, always preferred when present; when every trusted source errors it fails
  closed and NEVER degrades to a public source), OR (b) a qualifying quorum. **Fail-closed default:**
  a pure-public quorum (no operator-trusted member) does NOT satisfy custody unless the operator sets
  `allow_public_quorum_custody(true)`, which enables a quorum requiring **2 independent groups** to
  agree (logged loudly as reduced assurance). Otherwise custody fails closed with `NoProvider`.
- **`registry.any()` -- the DISCOVERY view.** A single-provider, low-trust read for NON-custody use
  (returns the first provider that responds WITHIN THE UNTRUSTED-INPUT BOUND below). Its result MUST NOT
  be used to route funds. **Bounded untrusted input (discovery).** Before a discovery answer is returned,
  its record count is bounded by the SAME cap as the quorum path (`QuorumComparable::validate_bound`,
  `MAX_QUORUM_RECORDS` = 100,000): an answer exceeding the cap is DROPPED and the read continues to the
  next provider (exactly as a non-responding provider would), so a hostile source cannot force unbounded
  CPU/memory on the non-custody path. When every provider floods, the read fails closed with `TooManyRecords`.

**Independence groups.** Each provider registers with an `independence_group` id; a quorum counts
DISTINCT groups, so two providers in the same group (e.g. the same coinset.org listed twice) count as
one and cannot alone satisfy a 2-group threshold. Quorum agreement compares converted answers for
equality, ORDER-INSENSITIVELY: the list reads (`coin_records_by_puzzle_hash`, `coin_records_by_parent`)
return a SET of matching coins with no promised ordering, so two honest sources returning the same
records in a different order AGREE (compared by canonical coin-id order) rather than spuriously
disagreeing; a genuinely different record SET still fails closed (the security property is preserved,
only the false negative on ordering is removed). The canonical order computes each record's coin id
ONCE into the sort key (O(n) hashes, not the O(n log n) a per-comparison `coin_id()` would cost).
**Transport response-body cap.** The native coinset HTTP transport bounds every response at the RECEIVE
layer: a body whose `Content-Length` exceeds `MAX_RESPONSE_BYTES` (256 MiB) is rejected before download,
and a body with absent/under-declared length is streamed with a running-size check that aborts once the
accumulated bytes exceed the cap -- so an oversized or chunked hostile body is rejected BEFORE
deserialization. This bounds the receive/parse peak memory (complementary to the `MAX_COIN_RECORDS` /
`MAX_QUORUM_RECORDS` count caps, which bound downstream work). An over-cap body fails closed as a
transport error.
**Bounded untrusted input.** Before an untrusted quorum answer is canonicalized or compared, its
record count is bounded: a list answer exceeding `MAX_QUORUM_RECORDS` (100,000 -- far above any
legitimate puzzle-hash/parent fan-out) is DROPPED before any canonicalization, capping the CPU/memory
a hostile source can force during a quorum. The oversized member simply loses its vote (exactly as a
non-responding member would) -- it does NOT abort the whole read, so a single hostile provider cannot
deny an otherwise-valid honest quorum. When no honest quorum can form (e.g. every source floods),
custody still fails closed with `NoProvider`. For `resolve_singleton_lineage`, cross-source
agreement compares the full lineage; consumers still apply the `SingletonLineage::contains` MEMBERSHIP
authority test to the result, never tip/puzzle-hash equality. Disagreement, too few groups, or
all-errors fails closed.
