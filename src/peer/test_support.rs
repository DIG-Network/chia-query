//! Shared test scaffolding for the peer tier.
//!
//! Lives here rather than inside one module's `mod tests` because both the pool's admission tests
//! and the backend's corroboration tests need a REAL [`Peer`], and a peer cannot be mocked:
//! `Peer::from_websocket` reads the socket's own `peer_addr`, so there is no way to construct one
//! without a live connection.

use std::net::SocketAddr;

use chia_protocol::{RespondCoinState, RespondPuzzleState};
use chia_wallet_sdk::client::Peer;

/// A loopback address no other peer built by these helpers will use.
///
/// **Each fixture peer gets its own IP, not merely its own PORT, and that is load-bearing.**
/// Independence is counted per HOST (`PeerPool::is_corroborator`, chia-query#27), so a fixture whose
/// "independent" peers all sit on `127.0.0.1` is modelling one machine answering several times —
/// exactly the thing that count exists to refuse. Binding distinct `127.x.y.z` addresses keeps the
/// fixture faithful to production, where every held peer is a different host.
///
/// The whole of `127.0.0.0/8` routes to loopback on Linux, macOS and Windows alike, so this is
/// portable; it is verified by `two_fixture_peers_are_on_different_hosts` below.
fn distinct_loopback_addr() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);

    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!(
        "127.{}.{}.{}:0",
        (n >> 14) & 0xff,
        (n >> 7) & 0x7f,
        1 + (n & 0x7f)
    )
}

/// A real [`Peer`] over a genuine loopback websocket.
///
/// Each call binds a FRESH address — a distinct loopback IP as well as an ephemeral port — so two
/// peers built by two calls are two distinct HOSTS as far as the pool's independence counting is
/// concerned. The returned peer is cloneable (`Peer` is an `Arc` inside), so the *same* connection
/// can also be offered under several addresses when that is the shape under test.
pub(crate) async fn loopback_peer() -> Peer {
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::MaybeTlsStream;

    let listener = TcpListener::bind(distinct_loopback_addr())
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

/// A real [`Peer`] whose server side ANSWERS `RequestPuzzleState` with `response`.
///
/// [`loopback_peer`] holds a socket open and never replies, which is all a membership test needs
/// and is useless to a test of what this crate does with a peer's ANSWER. The reads that consume a
/// wire-supplied as-of height cannot be exercised at all without one, so this speaks just enough of
/// the wallet protocol to be one: it decodes each inbound `Message`, and replies to a puzzle-state
/// request under the SAME request id, which is how the SDK's `Peer` matches a response to its call.
///
/// It answers only that one request type. Anything else is left unanswered, so a test that
/// accidentally exercises a different read fails on its own timeout rather than on a fabricated
/// reply it never meant to script.
pub(crate) async fn puzzle_state_peer(response: RespondPuzzleState) -> Peer {
    use chia_protocol::ProtocolMessageTypes;
    use chia_traits::Streamable;

    let body = response
        .to_bytes()
        .expect("a RespondPuzzleState is streamable");
    scripted_peer(move |request| {
        (request.msg_type == ProtocolMessageTypes::RequestPuzzleState)
            .then(|| (ProtocolMessageTypes::RespondPuzzleState, body.clone()))
    })
    .await
    .0
}

/// A real [`Peer`] whose server side ANSWERS `RequestCoinState` with `response`.
///
/// The coin-read counterpart of [`puzzle_state_peer`]: [`loopback_peer`] never replies, which
/// proves nothing about what this crate does with a coin-state ANSWER. This is what lets a test
/// prove the scalar coin-record read corroborates a peer's `confirmed_block_index`/
/// `spent_block_index` exactly as its `_opt` sibling does, rather than trusting the lone peer that
/// happened to answer first (dig_ecosystem#3034).
pub(crate) async fn coin_state_peer(response: RespondCoinState) -> Peer {
    use chia_protocol::ProtocolMessageTypes;
    use chia_traits::Streamable;

    let body = response
        .to_bytes()
        .expect("a RespondCoinState is streamable");
    scripted_peer(move |request| {
        (request.msg_type == ProtocolMessageTypes::RequestCoinState)
            .then(|| (ProtocolMessageTypes::RespondCoinState, body.clone()))
    })
    .await
    .0
}

/// A real [`Peer`] that ACKS every `send_transaction` with `status` and `error`, counting them.
///
/// The counter is the assertion that matters for chia-query#50: the bound is "never more than two
/// transmissions", and a bound nothing counts is a comment. Reading a peer's answer proves what it
/// SAID; only the count proves how many times it was ASKED.
pub(crate) async fn transaction_peer(
    status: u8,
    error: Option<String>,
) -> (Peer, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use chia_protocol::{Bytes32, ProtocolMessageTypes, TransactionAck};
    use chia_traits::Streamable;

    let body = TransactionAck::new(Bytes32::new([0x5A; 32]), status, error)
        .to_bytes()
        .expect("a TransactionAck is streamable");
    scripted_peer(move |request| {
        (request.msg_type == ProtocolMessageTypes::SendTransaction)
            .then(|| (ProtocolMessageTypes::TransactionAck, body.clone()))
    })
    .await
}

/// A real [`Peer`] whose server side answers with whatever `reply` returns for each request.
///
/// [`loopback_peer`] holds a socket open and never replies, which is all a membership test needs
/// and is useless to a test of what this crate does with a peer's ANSWER. So this speaks just
/// enough of the wallet protocol to be one: it decodes each inbound `Message` and replies under the
/// SAME request id, which is how the SDK's `Peer` matches a response to its call.
///
/// `reply` returning `None` leaves the request unanswered, so a test that accidentally exercises a
/// read it did not script fails on its own timeout rather than on a fabricated reply. The returned
/// counter counts every request the script ANSWERED.
pub(crate) async fn scripted_peer<F>(
    reply: F,
) -> (Peer, std::sync::Arc<std::sync::atomic::AtomicUsize>)
where
    F: Fn(&chia_protocol::Message) -> Option<(chia_protocol::ProtocolMessageTypes, Vec<u8>)>
        + Send
        + 'static,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use chia_protocol::Message;
    use chia_traits::Streamable;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::MaybeTlsStream;

    let listener = TcpListener::bind(distinct_loopback_addr())
        .await
        .expect("bind a loopback listener");
    let addr = listener.local_addr().expect("read the listener address");
    let answered = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&answered);

    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        while let Some(Ok(frame)) = ws.next().await {
            let tokio_tungstenite::tungstenite::Message::Binary(bytes) = frame else {
                continue;
            };
            let Ok(request) = Message::from_bytes(&bytes) else {
                continue;
            };
            let Some((msg_type, data)) = reply(&request) else {
                continue;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let response = Message {
                msg_type,
                id: request.id,
                data: data.into(),
            };
            if ws
                .send(tokio_tungstenite::tungstenite::Message::Binary(
                    response.to_bytes().expect("a Message is streamable"),
                ))
                .await
                .is_err()
            {
                return;
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
    (peer, answered)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// **Two fixture peers are two HOSTS, not one host on two ports.**
    ///
    /// Every corroboration fixture in this crate rests on this: independence is counted per IP, so
    /// peers sharing `127.0.0.1` would model one machine agreeing with itself and every such test
    /// would be measuring the defect rather than the fix. It also pins that binding across
    /// `127.0.0.0/8` actually works on the platforms CI runs, which is the assumption underneath.
    #[tokio::test]
    async fn two_fixture_peers_are_on_different_hosts() {
        let first = loopback_peer().await;
        let second = loopback_peer().await;

        assert_ne!(
            first.socket_addr().ip(),
            second.socket_addr().ip(),
            "fixture peers must differ by IP, not merely by port, or every independence test in \
             this crate is vacuous"
        );
    }
}
