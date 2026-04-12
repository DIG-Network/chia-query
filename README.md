# chia-query

Query the Chia blockchain through decentralized peer connections with automatic fallback to the [coinset.org](https://api.coinset.org) HTTP API.

## Features

- **Peer-first architecture** -- queries decentralized full nodes before falling back to coinset.org
- **Peer pool** -- maintains up to 5 concurrent peer connections with automatic ejection and replacement
- **Full coinset.org API parity** -- every endpoint from coinset.org is available as a typed Rust method
- **CLVM block parsing** -- extracts additions, removals, and coin spends directly from block generators
- **Zero-config TLS** -- auto-generates certificates for peer connections

## Quick start

```rust
use chia_query::{ChiaQuery, ChiaQueryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;

    // Queries try peers first, fall back to coinset.org
    let record = client.get_coin_record_by_name("0xabc...").await?;
    println!("confirmed at height {}", record.confirmed_block_index);

    // Wait for a coin to appear on-chain
    use std::time::Duration;
    let confirmed = client.wait_for_confirmation(
        "0xdef...",
        Duration::from_secs(5),
        Duration::from_secs(300),
    ).await?;

    Ok(())
}
```

## License

MIT
# chia-query
