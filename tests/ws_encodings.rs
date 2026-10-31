//! PERF-9 (spec/13): msgpack and JSON on one wire.
//!
//! - The offered-set matrix: three offers by four asks, each cell an
//!   upgrade — the `101` names the pick, a call answers in it; a refusal is
//!   the `101` (naming the first entry of the ask), then close 4415 whose
//!   reason lists what the service offers.
//! - Under msgpack every frame is one binary message holding a map with its
//!   field names and no `jsonrpc`: a call's answer, its items (encoded once
//!   by plexus-core's pipe), its `done`; equal as JSON to the same call's
//!   frames on a JSON socket.
//! - A text message on a msgpack socket closes it 1003; a binary message
//!   that is not msgpack is answered `-32700` and the socket goes on.
//! - stdio: the first line, the request `plexus.encoding`, names the ask and
//!   is answered with the pick; then
//!   4-byte length-prefixed frames, whole, from one writer — two streams
//!   live at once never tear a frame.
//!
//! The frames are read and written here with rmp-serde, not with
//! plexus-core's writer: the wire is checked against another msgpack.

mod ws_support;

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonrpsee::{PendingSubscriptionSink, RpcModule};
use plexus_core::activations::echo::EchoActivation;
use plexus_core::DynamicHub;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{Encoding, WebSocketConfig};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::Message;
use ws_support::Ws;

fn hub_module() -> RpcModule<()> {
    DynamicHub::arc_into_rpc_module(Arc::new(DynamicHub::new("t").register(EchoActivation))).unwrap()
}

async fn serve(offers: &[Encoding]) -> u16 {
    let options = UpgradeOptions::default().encodings(offers.iter().copied());
    let (handle, addr) = serve_websocket_with(hub_module(), WebSocketConfig::new(0), None, options).await.expect("serve");
    std::mem::forget(handle);
    addr.port()
}

/// An upgrade naming `ask` (`None`: no `Sec-WebSocket-Protocol`): the socket
/// and the subprotocol the `101` named.
async fn dial(port: u16, ask: Option<&str>) -> (Ws, Option<String>) {
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    if let Some(a) = ask {
        req.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, a.parse().unwrap());
    }
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (ws, resp) = tokio_tungstenite::client_async(req, tcp).await.expect("the 101");
    let named = resp.headers().get(SEC_WEBSOCKET_PROTOCOL).map(|v| v.to_str().unwrap().to_string());
    (ws, named)
}

fn mp(v: &Value) -> Vec<u8> {
    rmp_serde::to_vec_named(v).unwrap()
}

fn unmp(b: &[u8]) -> Value {
    rmp_serde::from_slice(b).expect("one msgpack value")
}

/// The next frame: `Ok(json)` with whether it came as binary msgpack (else
/// text JSON); `Err((code, reason))` for a close.
async fn next(ws: &mut Ws) -> Result<(Value, bool), (u16, String)> {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await.expect("a frame in 10 s") {
            Some(Ok(Message::Text(t))) => return Ok((serde_json::from_str(t.as_str()).expect("JSON text"), false)),
            Some(Ok(Message::Binary(b))) => return Ok((unmp(&b), true)),
            Some(Ok(Message::Close(f))) => return Err(f.map(|f| (u16::from(f.code), f.reason.to_string())).unwrap_or((0, String::new()))),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err((0, e.to_string())),
            None => return Err((0, "ended".into())),
        }
    }
}

/// One call on a socket in `msgpack` or not: every frame back (the answer,
/// the items, `done`), each with whether it was binary.
async fn call(ws: &mut Ws, msgpack: bool, id: u64, method: &str, params: Value) -> Vec<(Value, bool)> {
    let mut req = json!({"id": id, "method": "t.call", "params": {"method": method, "params": params}});
    if msgpack {
        ws.send(Message::Binary(mp(&req).into())).await.unwrap();
    } else {
        req["jsonrpc"] = json!("2.0");
        ws.send(Message::Text(req.to_string().into())).await.unwrap();
    }
    let mut out = Vec::new();
    loop {
        let (f, bin) = next(ws).await.expect("frames, the socket open");
        let done = f["params"]["result"]["type"] == "done" || f.get("error").is_some();
        out.push((f, bin));
        if done {
            return out;
        }
    }
}

/// spec/13's matrix, every cell an upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_offered_set_matrix() {
    use Encoding::{Json, Msgpack};
    let asks: [Option<&str>; 4] = [None, Some("plexus.json"), Some("plexus.msgpack"), Some("plexus.msgpack, plexus.json")];
    // Each row: what the service offers, then per ask the encoding picked
    // (`None`: refused).
    let rows: [(&[Encoding], &str, [Option<Encoding>; 4]); 3] = [
        (&[Json], "plexus.json", [Some(Json), Some(Json), None, Some(Json)]),
        (&[Msgpack], "plexus.msgpack", [None, None, Some(Msgpack), Some(Msgpack)]),
        (&[Msgpack, Json], "plexus.msgpack, plexus.json", [Some(Json), Some(Json), Some(Msgpack), Some(Msgpack)]),
    ];
    let mut table = Vec::new();
    for (offers, tokens, want) in rows {
        let port = serve(offers).await;
        for (ask, want) in asks.iter().zip(want) {
            let (mut ws, named) = dial(port, *ask).await;
            let first_asked = ask.map(|a| a.split(',').next().unwrap().trim().to_string());
            let cell = match want {
                Some(e) => {
                    let token = ask.map(|_| e.token().to_string());
                    assert_eq!(named, token, "offers {tokens} × asks {ask:?}: the 101 names the pick");
                    let frames = call(&mut ws, e == Msgpack, 1, "echo.once", json!({"message": "m"})).await;
                    assert!(frames.iter().all(|(_, bin)| *bin == (e == Msgpack)), "offers {tokens} × asks {ask:?}: every frame in {e}: {frames:?}");
                    assert_eq!(frames.len(), 3, "the answer, one item, done: {frames:?}");
                    assert_eq!(frames[1].0["params"]["result"]["content"], json!({"event": "echo", "message": "m", "count": 1}));
                    e.name().to_string()
                }
                None => {
                    assert_eq!(named, first_asked, "offers {tokens} × asks {ask:?}: a refusal's 101 names the first entry of the ask");
                    let close = next(&mut ws).await.expect_err("a refusal closes at once");
                    assert_eq!(close, (4415, format!("offered: {tokens}")), "offers {tokens} × asks {ask:?}");
                    "refused".to_string()
                }
            };
            table.push(format!("{tokens:28} × {:30} → {cell}", ask.unwrap_or("(nothing)")));
        }
    }
    println!("{}", table.join("\n"));
    assert_eq!(table.len(), 12);
}

/// Under msgpack the frames are maps with their field names and no
/// `jsonrpc`, one binary message each, and equal as JSON to the same call's
/// frames under JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn msgpack_frames_are_the_json_frames_as_maps() {
    let port = serve(&[Encoding::Msgpack, Encoding::Json]).await;
    let (mut m, _) = dial(port, Some("plexus.msgpack, plexus.json")).await;
    let (mut j, _) = dial(port, None).await;
    for (method, params) in [("echo.once", json!({"message": "a \"quoted\" ✓ line\nbreak"})), ("echo.echo", json!({"message": "x", "count": 2}))] {
        let mframes = call(&mut m, true, 7, method, params.clone()).await;
        let jframes = call(&mut j, false, 7, method, params).await;
        assert_eq!(mframes.len(), jframes.len(), "{method}: as many frames");
        for ((mf, mbin), (jf, jbin)) in mframes.iter().zip(&jframes) {
            assert!(*mbin && !*jbin, "{method}: binary under msgpack, text under JSON");
            assert!(mf.get("jsonrpc").is_none(), "{method}: no `jsonrpc` in a msgpack frame: {mf}");
            assert_eq!(jf["jsonrpc"], "2.0");
            let (mut mf, mut jf) = (mf.clone(), jf.clone());
            jf.as_object_mut().unwrap().remove("jsonrpc");
            for f in [&mut mf, &mut jf] {
                // The subscription ids are each socket's own, and a timestamp is the clock's.
                if let Some(p) = f.get_mut("params").and_then(Value::as_object_mut) {
                    p.insert("subscription".into(), json!(0));
                    if let Some(m) = p.get_mut("result").and_then(|r| r.get_mut("metadata")).and_then(Value::as_object_mut) {
                        m.insert("timestamp".into(), json!(0));
                    }
                }
                if f.get("result").map_or(false, Value::is_number) {
                    f["result"] = json!(0);
                }
            }
            assert_eq!(mf, jf, "{method}: equal as JSON");
        }
        // The answer, then the items named by it.
        let sid = &mframes[0].0["result"];
        assert!(sid.is_u64(), "the subscription id an integer: {sid}");
        let item = &mframes[1].0;
        assert_eq!(item["method"], "t.call");
        assert_eq!(item["params"]["result"]["type"], "data");
        assert_eq!(item["params"]["result"]["content_type"], method);
    }
}

/// spec/13: a text message on a msgpack socket is a protocol error that
/// closes it 1003; a binary message that is not one msgpack value is
/// answered `-32700` and the socket goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_text_message_on_a_msgpack_socket_closes_it() {
    let port = serve(&[Encoding::Msgpack, Encoding::Json]).await;
    let (mut ws, named) = dial(port, Some("plexus.msgpack")).await;
    assert_eq!(named.as_deref(), Some("plexus.msgpack"));
    ws.send(Message::Binary(vec![0xc1, 0xff].into())).await.unwrap();
    let (answer, bin) = next(&mut ws).await.expect("an answer, the socket kept");
    assert!(bin);
    assert_eq!(answer, json!({"id": null, "error": {"code": -32700, "message": "Parse error"}}));
    // Still served.
    let frames = call(&mut ws, true, 2, "echo.once", json!({"message": "still"})).await;
    assert_eq!(frames.len(), 3);
    ws.send(Message::Text(r#"{"jsonrpc":"2.0","id":3,"method":"t.call","params":{"method":"echo.once","params":{"message":"no"}}}"#.into())).await.unwrap();
    let (code, reason) = next(&mut ws).await.expect_err("closed");
    assert_eq!(code, 1003, "{reason}");
    assert!(reason.contains("plexus.msgpack"), "{reason}");
    // A JSON socket takes text as before.
    let (mut j, named) = dial(port, None).await;
    assert_eq!(named, None, "asking nothing names nothing");
    assert_eq!(call(&mut j, false, 1, "echo.once", json!({"message": "json"})).await.len(), 3);
}

// ---------------------------------------------------------------------------
// stdio
// ---------------------------------------------------------------------------

/// A module with a stream that writes `n` items at once (no plexus-core in
/// the way: the framing alone is under test).
fn stream_module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_subscription("count", "counted", "count_unsub", |params, pending: PendingSubscriptionSink, _, _| async move {
        let (tag, n): (String, u32) = params.parse()?;
        let sink = pending.accept().await?;
        for i in 0..n {
            sink.send(serde_json::value::to_raw_value(&json!({"tag": tag, "i": i, "pad": "p".repeat(300)}))?).await?;
            if i % 7 == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    })
    .unwrap();
    m
}

async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(r: &mut R) -> String {
    use tokio::io::AsyncBufReadExt;
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), r.read_line(&mut line)).await.expect("a line in 5 s").unwrap();
    line
}

/// The first line names the ask and is answered with the pick; then
/// length-prefixed msgpack frames both ways. Two streams live at once: every
/// frame whole, each stream's items in order and complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_first_line_then_frames_from_one_writer() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
    let (client, service) = tokio::io::duplex(1 << 16);
    let (service_r, service_w) = tokio::io::split(service);
    tokio::spawn(plexus_transport::stdio::serve_stdio_io(stream_module(), plexus_transport::StdioConfig::default(), service_r, service_w));
    let (client_r, mut client_w) = tokio::io::split(client);
    let mut client_r = BufReader::new(client_r);

    client_w.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"plexus.encoding\",\"params\":{\"encodings\":[\"msgpack\",\"json\"]}}\n").await.unwrap();
    let answer: Value = serde_json::from_str(&read_line(&mut client_r).await).unwrap();
    assert_eq!(answer, json!({"jsonrpc": "2.0", "id": 0, "result": {"encoding": "msgpack"}}), "the service's first line: its pick");

    let frame = |v: Value| {
        let b = mp(&v);
        let mut out = (b.len() as u32).to_be_bytes().to_vec();
        out.extend(b);
        out
    };
    let mut both = frame(json!({"id": 1, "method": "count", "params": ["a", 400]}));
    both.extend(frame(json!({"id": 2, "method": "count", "params": ["b", 400]})));
    client_w.write_all(&both).await.unwrap();

    let mut subs = std::collections::HashMap::<u64, String>::new();
    let mut next_i = std::collections::HashMap::<String, u64>::new();
    let mut frames = 0;
    while next_i.get("a").copied().unwrap_or(0) < 400 || next_i.get("b").copied().unwrap_or(0) < 400 {
        let mut len = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(10), client_r.read_exact(&mut len)).await.expect("a frame in 10 s").unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        client_r.read_exact(&mut body).await.unwrap();
        let f = unmp(&body); // a torn frame is not one msgpack value: this panics
        frames += 1;
        assert!(f.get("jsonrpc").is_none(), "{f}");
        if let Some(sid) = f.get("result").and_then(Value::as_u64) {
            subs.insert(sid, if f["id"] == 1 { "a".into() } else { "b".into() });
            continue;
        }
        let item = &f["params"]["result"];
        let tag = item["tag"].as_str().unwrap().to_string();
        assert_eq!(subs.get(&f["params"]["subscription"].as_u64().unwrap()), Some(&tag), "an item of a subscription already answered");
        let want = next_i.entry(tag.clone()).or_default();
        assert_eq!(item["i"].as_u64(), Some(*want), "{tag}: in order");
        *want += 1;
    }
    assert_eq!(frames, 802, "two answers and 800 items, every one whole");
}

/// A first line that is not the negotiation is a JSON frame: today's
/// framing, unchanged.
#[tokio::test]
async fn stdio_without_the_first_line_is_json_lines() {
    use tokio::io::{AsyncWriteExt, BufReader};
    let (client, service) = tokio::io::duplex(1 << 16);
    let (service_r, service_w) = tokio::io::split(service);
    tokio::spawn(plexus_transport::stdio::serve_stdio_io(stream_module(), plexus_transport::StdioConfig::default(), service_r, service_w));
    let (client_r, mut client_w) = tokio::io::split(client);
    let mut client_r = BufReader::new(client_r);
    client_w.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"count\",\"params\":[\"a\",2]}\n").await.unwrap();
    let answer: Value = serde_json::from_str(&read_line(&mut client_r).await).unwrap();
    assert_eq!(answer["id"], 1);
    for i in 0..2 {
        let n: Value = serde_json::from_str(&read_line(&mut client_r).await).unwrap();
        assert_eq!(n["params"]["result"]["i"], i);
    }
}

/// A service that offers JSON alone refuses a client that names msgpack
/// alone: its first line names what it offers, and it ends.
#[tokio::test]
async fn stdio_refusal_names_the_offer() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
    let (client, service) = tokio::io::duplex(1 << 16);
    let (service_r, service_w) = tokio::io::split(service);
    let config = plexus_transport::StdioConfig { encodings: plexus_transport::Encodings::only(Encoding::Json), ..Default::default() };
    let served = tokio::spawn(plexus_transport::stdio::serve_stdio_io(stream_module(), config, service_r, service_w));
    let (client_r, mut client_w) = tokio::io::split(client);
    let mut client_r = BufReader::new(client_r);
    client_w.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"plexus.encoding\",\"params\":{\"encodings\":[\"msgpack\"]}}\n").await.unwrap();
    let line: Value = serde_json::from_str(&read_line(&mut client_r).await).unwrap();
    assert_eq!(line, json!({"jsonrpc": "2.0", "id": 5, "result": {"encoding": null, "offered": ["json"]}}));
    tokio::time::timeout(Duration::from_secs(5), served).await.expect("the session ends").unwrap().unwrap();
    let mut rest = Vec::new();
    client_r.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "nothing after the refusal");
}
