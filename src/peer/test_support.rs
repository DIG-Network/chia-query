//! Shared test scaffolding for the peer tier.
//!
//! Lives here rather than inside one module's `mod tests` because both the pool's admission tests
//! and the backend's corroboration tests need a REAL [`Peer`], and a peer cannot be mocked:
//! `Peer::from_websocket` reads the socket's own `peer_addr`, so there is no way to construct one
//! without a live connection.

use std::net::SocketAddr;

use chia_wallet_sdk::client::Peer;

/// A real [`Peer`] over a genuine loopback websocket.
///
/// Each call binds a FRESH ephemeral port, so two peers built by two calls carry two distinct
/// [`Peer::socket_addr`] values — which is what lets a test address them apart and give them
/// different things to say. The returned peer is cloneable (`Peer` is an `Arc` inside), so the
/// *same* connection can also be offered under several addresses when that is the shape under test.
pub(crate) async fn loopback_peer() -> Peer {
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::MaybeTlsStream;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback listener");
    let addr = listener.local_addr().expect("read the listener address");

    // Hold the server side open for the life of the test; dropping it would close the connection
    // under the peer being tested.
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                let _keep_open = ws;
                std::future::pending::<()>().await;
            }
        }
    });

    let stream = TcpStream::connect(addr).await.expect("dial the listener");
    let (ws, _) =
        tokio_tungstenite::client_async(format!("ws://{addr}/ws"), MaybeTlsStream::Plain(stream))
            .await
            .expect("complete the websocket handshake");

    let (peer, _receiver) =
        Peer::from_websocket(ws, Default::default()).expect("build a peer from the websocket");
    peer
}

/// A documentation-range address (RFC 5737), distinct per `last_octet`.
pub(crate) fn address(last_octet: u8) -> SocketAddr {
    SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, last_octet)),
        8444,
    )
}

/// A documentation-range IPv6 address (RFC 3849), distinct per `last_group`.
///
/// The peer tier is IPv6-first (CLAUDE.md §5.2) and the fleet that motivated the lag eviction held
/// `[2806:2f0:...]:8444`, so any policy that enumerates or evicts peers has to be shown working on
/// a v6 entry. A policy proven only against [`address`] is proven only against half the network.
pub(crate) fn address_v6(last_group: u16) -> SocketAddr {
    SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::new(
            0x2001, 0x0db8, 0, 0, 0, 0, 0, last_group,
        )),
        8444,
    )
}
