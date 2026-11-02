//! BIDIR-6: the input half of a call on the wire, against an activation with
//! `Input<Token>`. The ticket's frames, verbatim, then each row of its cases:
//! credit and ack, the handler ending first, a bad item, cancel, the gate.
//!
//! A frame this test writes is a string; a frame it reads is compared whole.
//! `tests/duplex_fixture` is the activation.

mod ws_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use duplex_fixture::plexus_client::{ClientError, ClientEvent, WsTransport};
use duplex_fixture::{IngestActivation, IngestClient, IngestEvent, Probe, Token};
use futures::{SinkExt, StreamExt};
use plexus_core::plexus::{AuthContext, DynamicHub, Gate, Membership, Memberships, Resolve, RoleTable, ScopeRef, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{ConnectionQuotas, WebSocketConfig};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

const WAIT: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(150);

fn service(probe: &Probe) -> DynamicHub {
    DynamicHub::new("menger").register(IngestActivation { probe: probe.clone() })
}

async fn served(probe: &Probe) -> u16 {
    serve(service(probe).into_rpc_module().unwrap(), UpgradeOptions::default()).await
}

async fn send(ws: &mut Ws, frame: String) {
    ws.send(Message::text(frame)).await.unwrap();
}

/// The next frame's text, exactly as it came.
async fn next_raw(ws: &mut Ws) -> String {
    loop {
        match tokio::time::timeout(WAIT, ws.next()).await.expect("a frame within the time") {
            Some(Ok(Message::Text(t))) => return t.as_str().to_string(),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            other => panic!("not a frame: {other:?}"),
        }
    }
}

/// The `result` of the next notification.
async fn next_item(ws: &mut Ws) -> Value {
    let frame: Value = serde_json::from_str(&next_raw(ws).await).unwrap();
    frame["params"]["result"].clone()
}

async fn nothing_for_a_while(ws: &mut Ws) {
    match tokio::time::timeout(QUIET, ws.next()).await {
        Err(_) => {}
        Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
        Ok(other) => panic!("expected nothing, got {other:?}"),
    }
}

/// Open `method`, answered with the subscription and the first credit.
async fn open(ws: &mut Ws, id: u64, method: &str, params: Value) -> (u64, u64) {
    send(ws, json!({"jsonrpc":"2.0","id":id,"method":"menger.call","params":{"method":method,"params":params}}).to_string()).await;
    let answer: Value = serde_json::from_str(&next_raw(ws).await).unwrap();
    assert_eq!(answer["id"], id, "{answer}");
    let sub = answer["result"].as_u64().unwrap_or_else(|| panic!("a subscription id: {answer}"));
    let credit = next_item(ws).await;
    assert_eq!(credit["type"], "credit", "{credit}");
    (sub, credit["n"].as_u64().unwrap())
}

fn input(sub: u64, seq: u64, items: &[&str]) -> String {
    let items: Vec<Value> = items.iter().map(|t| json!({ "t": t })).collect();
    json!({"jsonrpc":"2.0","method":"menger.input","params":{"subscription":sub,"seq":seq,"items":items}}).to_string()
}

fn end(sub: u64, seq: u64) -> String {
    json!({"jsonrpc":"2.0","method":"menger.end","params":{"subscription":sub,"seq":seq}}).to_string()
}

async fn until_dropped(probe: &Probe, n: usize) {
    tokio::time::timeout(WAIT, async {
        while probe.dropped() < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the handler dropped");
}

// ------------------------------------------------------- the ticket's frames

/// Acceptance 1: the frames of the ticket, verbatim. Only the ids are the
/// server's to choose, and `data` and `done` carry their metadata as today.
#[tokio::test]
async fn the_tickets_frames_verbatim() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;

    send(&mut ws, r#"{"jsonrpc":"2.0","id":7,"method":"menger.call","params":{"method":"ingest.tokens","params":{"run":"r1"}}}"#.into()).await;
    let answer: Value = serde_json::from_str(&next_raw(&mut ws).await).unwrap();
    let sub = answer["result"].as_u64().unwrap();
    assert_eq!(answer, json!({"jsonrpc":"2.0","id":7,"result":sub}));
    assert_eq!(next_raw(&mut ws).await, format!(r#"{{"jsonrpc":"2.0","method":"result","params":{{"subscription":{sub},"result":{{"type":"credit","n":64}}}}}}"#));

    send(&mut ws, format!(r#"{{"jsonrpc":"2.0","method":"menger.input","params":{{"subscription":{sub},"seq":0,"items":[{{"t":"The"}},{{"t":" cat"}}]}}}}"#)).await;
    send(&mut ws, format!(r#"{{"jsonrpc":"2.0","method":"menger.input","params":{{"subscription":{sub},"seq":1,"items":[{{"t":" sat"}}]}}}}"#)).await;
    send(&mut ws, format!(r#"{{"jsonrpc":"2.0","method":"menger.end","params":{{"subscription":{sub},"seq":2}}}}"#)).await;

    assert_eq!(next_raw(&mut ws).await, format!(r#"{{"jsonrpc":"2.0","method":"result","params":{{"subscription":{sub},"result":{{"type":"ack","seq":1}}}}}}"#));
    let data: Value = serde_json::from_str(&next_raw(&mut ws).await).unwrap();
    assert_eq!((data["method"].as_str(), data["params"]["subscription"].as_u64()), (Some("result"), Some(sub)));
    assert_eq!(data["params"]["result"]["type"], "data");
    assert_eq!(data["params"]["result"]["content"], json!({"type":"ingested","count":3}));
    let done = next_item(&mut ws).await;
    assert_eq!(done["type"], "done");
    assert_eq!(probe.seen(), ["The", " cat", " sat"]);
}

/// Acceptance 1: a method without `Input` is what it was, frame by frame and
/// byte by byte (the metadata's hash and clock masked).
#[tokio::test]
async fn a_method_without_input_is_byte_identical() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    send(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.plain","params":{"run":"r1"}}}"#.into()).await;
    let answer: Value = serde_json::from_str(&next_raw(&mut ws).await).unwrap();
    let sub = answer["result"].as_u64().unwrap();
    let mask = |frame: String| -> String {
        let hash = frame.split("\"plexus_hash\":\"").nth(1).unwrap().split('"').next().unwrap().to_string();
        let clock = frame.split("\"timestamp\":").nth(1).unwrap().split(['}', ',']).next().unwrap().to_string();
        frame.replace(&hash, "H").replace(&format!("\"timestamp\":{clock}"), "\"timestamp\":T").replace(&sub.to_string(), "S")
    };
    assert_eq!(
        mask(next_raw(&mut ws).await),
        r#"{"jsonrpc":"2.0","method":"result","params":{"subscription":S,"result":{"type":"data","metadata":{"provenance":["ingest"],"plexus_hash":"H","timestamp":T},"content_type":"ingest.plain","content":{"type":"ok"}}}}"#
    );
    assert_eq!(
        mask(next_raw(&mut ws).await),
        r#"{"jsonrpc":"2.0","method":"result","params":{"subscription":S,"result":{"type":"done","metadata":{"provenance":["ingest"],"plexus_hash":"H","timestamp":T}}}}"#
    );
    nothing_for_a_while(&mut ws).await;
}

/// Acceptance 2: `input` is on the schema a client reads.
#[tokio::test]
async fn the_schema_says_input() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    send(&mut ws, json!({"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.schema","params":{"method":"tokens"}}}).to_string()).await;
    next_raw(&mut ws).await;
    let schema = next_item(&mut ws).await;
    let method = &schema["content"];
    assert_eq!(method["input"]["properties"]["t"]["type"], "string", "{schema}");
    assert!(method["params"]["properties"].get("tokens").is_none());
}

// ------------------------------------------------------------ credit and ack

/// A client that obeys its credit is never refused, whatever passes through,
/// and `ack` advances with `seq`: half a window at a time.
#[tokio::test]
async fn a_client_under_credit_is_never_refused() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, mut credit) = open(&mut ws, 1, "ingest.gated", json!({})).await;
    assert_eq!(credit, 4, "the method's window");
    let total = 1000u64;
    let (mut sent, mut last_ack) = (0u64, None);
    while sent < total {
        while credit > 0 && sent < total {
            send(&mut ws, input(sub, sent, &["x"])).await;
            sent += 1;
            credit -= 1;
        }
        if sent == total {
            break;
        }
        let item = next_item(&mut ws).await;
        match item["type"].as_str() {
            Some("ack") => {
                let seq = item["seq"].as_u64().unwrap();
                assert!(last_ack.is_none_or(|l| seq > l), "ack moves forward: {seq} after {last_ack:?}");
                last_ack = Some(seq);
                let more = next_item(&mut ws).await;
                assert_eq!(more["type"], "credit", "{more}");
                let n = more["n"].as_u64().unwrap();
                assert!((2..=4).contains(&n), "half a window of 4, or all the handler has consumed: {n}");
                credit += n;
            }
            other => panic!("{other:?}: {item}"),
        }
    }
    send(&mut ws, end(sub, total)).await;
    loop {
        let item = next_item(&mut ws).await;
        match item["type"].as_str() {
            Some("ack" | "credit") => {}
            Some("data") => {
                assert_eq!(item["content"]["count"], total);
                break;
            }
            other => panic!("{other:?}: {item}"),
        }
    }
    assert_eq!(next_item(&mut ws).await["type"], "done");
    let seen = probe.seen();
    assert_eq!(seen.len(), total as usize);
}

/// The owner's first decision: credit is what the handler consumes. An item
/// counts when the handler polls for the next one, so nothing moves until
/// what it read has been forwarded.
#[tokio::test]
async fn credit_moves_when_the_handler_has_forwarded() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, credit) = open(&mut ws, 1, "ingest.gated", json!({})).await;
    assert_eq!(credit, 4);
    send(&mut ws, input(sub, 0, &["a"])).await;
    send(&mut ws, input(sub, 1, &["b"])).await;
    nothing_for_a_while(&mut ws).await;
    // Read, and not yet forwarded: the handler has not polled again.
    probe.permits.add_permits(1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(probe.seen(), ["a"]);
    nothing_for_a_while(&mut ws).await;
    probe.permits.add_permits(1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(probe.seen(), ["a", "b"]);
    nothing_for_a_while(&mut ws).await;
    // The third poll is the one that says `b` is through: half of 4.
    probe.permits.add_permits(1);
    assert_eq!(next_item(&mut ws).await, json!({"type":"ack","seq":1}));
    assert_eq!(next_item(&mut ws).await, json!({"type":"credit","n":2}));
}

/// Acceptance 3: past the window, one typed error, and the call is over.
#[tokio::test]
async fn past_credit_is_one_typed_error_and_ends_the_call() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, credit) = open(&mut ws, 1, "ingest.stuck", json!({})).await;
    assert_eq!(credit, 4);
    assert_eq!(next_item(&mut ws).await["content"]["type"], "ok");
    for seq in 0..5 {
        send(&mut ws, input(sub, seq, &["x"])).await;
    }
    // The handler is not reading: the pipe refuses it all the same.
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("past_credit")), "{closed}");
    let error = next_item(&mut ws).await;
    assert_eq!((error["type"].as_str(), error["error_kind"].as_str()), (Some("error"), Some("past_credit")), "{error}");
    assert_eq!((error["seq"].as_u64(), error["credit"].as_u64()), (Some(4), Some(4)));
    assert_eq!(error["code"], "past_credit");
    until_dropped(&probe, 1).await;
    send(&mut ws, input(sub, 5, &["x"])).await;
    nothing_for_a_while(&mut ws).await;
}

/// A frame of several items spends that many.
#[tokio::test]
async fn credit_counts_items_not_frames() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.stuck", json!({})).await;
    next_item(&mut ws).await;
    send(&mut ws, input(sub, 0, &["a", "b", "c"])).await;
    send(&mut ws, input(sub, 1, &["d", "e"])).await;
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64()), (Some("past_credit"), Some(1)), "{error}");
}

/// A resend at or below what was taken is dropped; a frame past the next one
/// is a broken client.
#[tokio::test]
async fn a_resend_is_safe_and_a_gap_is_not() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, input(sub, 0, &["a"])).await;
    send(&mut ws, input(sub, 0, &["a again"])).await;
    send(&mut ws, input(sub, 1, &["b"])).await;
    send(&mut ws, end(sub, 2)).await;
    assert_eq!(next_item(&mut ws).await, json!({"type":"ack","seq":1}));
    assert_eq!(next_item(&mut ws).await["content"]["count"], 2);
    assert_eq!(probe.seen(), ["a", "b"]);
    assert_eq!(next_item(&mut ws).await["type"], "done");

    let (sub, _) = open(&mut ws, 2, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, input(sub, 1, &["b"])).await;
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64(), error["expected"].as_u64()), (Some("out_of_sequence"), Some(1), Some(0)), "{error}");
}

/// `end` carries the frames sent: one that disagrees has lost count.
#[tokio::test]
async fn an_end_with_the_wrong_count_is_refused() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, input(sub, 0, &["a"])).await;
    send(&mut ws, end(sub, 5)).await;
    let mut item = next_item(&mut ws).await;
    if item["type"] == "input_closed" {
        item = next_item(&mut ws).await;
    }
    assert_eq!((item["error_kind"].as_str(), item["expected"].as_u64()), (Some("out_of_sequence"), Some(1)), "{item}");
}

// ---------------------------------------------------- limits: opt-in and held

/// The owner's third decision: nothing limits what has passed through a call
/// unless its method opts in; this one did.
#[tokio::test]
async fn a_method_that_opted_in_gets_too_much_input() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.capped", json!({})).await;
    for seq in 0..4 {
        send(&mut ws, input(sub, seq, &["x"])).await;
    }
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["limit"].as_str(), error["max"].as_u64()), (Some("too_much_input"), Some("items"), Some(3)), "{error}");
}

/// What a call holds is bounded by credit, and an obedient client is never
/// ended for it (owner, 2026-11-02): past the method's cap of bytes held, the
/// server grants nothing more until the handler drains below it.
#[tokio::test]
async fn an_obedient_client_sending_large_items_is_slowed_never_ended() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, mut credit) = open(&mut ws, 1, "ingest.heavy", json!({})).await;
    assert_eq!(credit, 8);
    // Each item is 408 bytes as JSON: eight of them, all within credit, hold
    // 3,264 bytes against a cap of 1,000. Nothing is refused.
    let big = "y".repeat(400);
    for seq in 0..8 {
        send(&mut ws, input(sub, seq, &[&big])).await;
    }
    credit -= 8;
    nothing_for_a_while(&mut ws).await;
    // Four consumed is half the window, but four held is 1,632 bytes: no credit.
    probe.permits.add_permits(5);
    tokio::time::sleep(QUIET).await;
    assert_eq!(probe.seen().len(), 5);
    nothing_for_a_while(&mut ws).await;
    // Six consumed, two held (816 bytes): below the cap, the credit comes.
    probe.permits.add_permits(2);
    assert_eq!(next_item(&mut ws).await, json!({"type":"ack","seq":5}));
    assert_eq!(next_item(&mut ws).await, json!({"type":"credit","n":6}));
    credit += 6;
    // The rest at the client's own pace, under whatever credit it is given.
    probe.permits.add_permits(1_000_000);
    let total = 64u64;
    let mut sent = 8u64;
    while sent < total {
        while credit > 0 && sent < total {
            send(&mut ws, input(sub, sent, &[&big])).await;
            sent += 1;
            credit -= 1;
        }
        if sent == total {
            break;
        }
        let item = next_item(&mut ws).await;
        match item["type"].as_str() {
            Some("ack") => {}
            Some("credit") => credit += item["n"].as_u64().unwrap(),
            other => panic!("an obedient client is never ended: {other:?}: {item}"),
        }
    }
    send(&mut ws, end(sub, total)).await;
    loop {
        let item = next_item(&mut ws).await;
        match item["type"].as_str() {
            Some("ack" | "credit") => {}
            Some("data") => {
                assert_eq!(item["content"]["count"], total);
                break;
            }
            other => panic!("{other:?}: {item}"),
        }
    }
    assert_eq!(next_item(&mut ws).await["type"], "done");
}

// ------------------------------------------------------- how a call can end

/// The handler ends first: `input_closed` before `done`, a later frame reaches
/// nothing, and the socket goes on.
#[tokio::test]
async fn the_handler_ending_first_says_input_closed_before_done() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.early", json!({"take": 2})).await;
    send(&mut ws, input(sub, 0, &["a", "b"])).await;
    let data = next_item(&mut ws).await;
    assert_eq!(data["content"]["count"], 2, "{data}");
    // No `ack`: the handler took the items and never polled past them.
    assert_eq!(next_item(&mut ws).await, json!({"type":"input_closed","reason":"method_ended"}));
    assert_eq!(next_item(&mut ws).await["type"], "done");
    send(&mut ws, input(sub, 1, &["c"])).await;
    nothing_for_a_while(&mut ws).await;
    assert_eq!(request(&mut ws, 9, "menger.input", json!({"subscription":sub,"seq":2,"items":[]})).await["error"]["data"]["error_kind"], "not_this_call");
}

/// A handler that drops its input closes the half and goes on with the call.
#[tokio::test]
async fn a_handler_that_lets_go_closes_the_half_and_the_call_runs_on() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.letgo", json!({})).await;
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("method_ended")), "{closed}");
    send(&mut ws, input(sub, 0, &["a"])).await;
    probe.permits.add_permits(1);
    assert_eq!(next_item(&mut ws).await["content"]["type"], "ok");
    assert_eq!(next_item(&mut ws).await["type"], "done");
}

/// Acceptance 4: an item that is not a `T` ends the call, naming its frame.
#[tokio::test]
async fn an_item_that_does_not_decode_names_its_frame() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, input(sub, 0, &["a"])).await;
    send(&mut ws, json!({"jsonrpc":"2.0","method":"menger.input","params":{"subscription":sub,"seq":1,"items":[{"t":5}]}}).to_string()).await;
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("invalid_input")), "{closed}");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64()), (Some("invalid_input"), Some(1)), "{error}");
    assert!(error["message"].as_str().unwrap().contains("frame 1"));
    until_dropped(&probe, 1).await;
    nothing_for_a_while(&mut ws).await;
}

/// Acceptance 4: the unsubscribe ends both halves, at once, mid-input.
#[tokio::test]
async fn cancel_ends_both_halves() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, input(sub, 0, &["a"])).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(probe.dropped(), 0);
    send(&mut ws, json!({"jsonrpc":"2.0","id":2,"method":"menger.call_unsub","params":[sub]}).to_string()).await;
    let answer: Value = serde_json::from_str(&next_raw(&mut ws).await).unwrap();
    assert_eq!(answer["result"], true, "{answer}");
    until_dropped(&probe, 1).await;
    send(&mut ws, input(sub, 1, &["b"])).await;
    nothing_for_a_while(&mut ws).await;
    assert_eq!(probe.seen(), ["a"], "the input half ended with the call");
}

/// A lost socket is a cancel.
#[tokio::test]
async fn a_lost_socket_ends_the_call() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    drop(ws);
    until_dropped(&probe, 1).await;
}

/// A request with an id is answered; a frame of another connection reaches
/// nothing, and a request says so.
#[tokio::test]
async fn the_request_form_is_answered_and_a_stranger_is_told() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    let answer = request(&mut ws, 2, "menger.input", json!({"subscription":sub,"seq":0,"items":[{"t":"a"}]})).await;
    assert_eq!(answer["result"], true, "{answer}");
    let answer = request(&mut ws, 3, "menger.input", json!({"subscription":sub,"seq":0,"items":[{"t":"a"}]})).await;
    assert_eq!(answer["result"], false, "a resend: {answer}");

    let mut stranger = connect(port).await;
    let answer = request(&mut stranger, 1, "menger.input", json!({"subscription":sub,"seq":1,"items":[{"t":"mine"}]})).await;
    assert_eq!(answer["error"]["data"]["error_kind"], "not_this_call", "{answer}");
    send(&mut stranger, input(sub, 1, &["mine"])).await;
    send(&mut stranger, end(sub, 2)).await;

    let answer = request(&mut ws, 4, "menger.end", json!({"subscription":sub,"seq":1})).await;
    assert_eq!(answer["result"], true, "{answer}");
    until_dropped(&probe, 1).await;
    assert_eq!(probe.seen(), ["a"]);
}

/// A duplex call is one call under `too_many_calls`, refused before any credit.
#[tokio::test]
async fn too_many_calls_comes_before_any_credit() {
    let probe = Probe::default();
    let mut config = WebSocketConfig::new(0);
    config.quotas = ConnectionQuotas { max_calls_per_connection: 1, ..ConnectionQuotas::default() };
    let port = serve_with(service(&probe).into_rpc_module().unwrap(), config, UpgradeOptions::default()).await;
    let mut ws = connect(port).await;
    open(&mut ws, 1, "ingest.tokens", json!({"run":"r1"})).await;
    send(&mut ws, json!({"jsonrpc":"2.0","id":2,"method":"menger.call","params":{"method":"ingest.tokens","params":{"run":"r2"}}}).to_string()).await;
    next_raw(&mut ws).await;
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["max"].as_u64()), (Some("too_many_calls"), Some(1)), "{error}");
}

/// In memory, with no socket's table, the method says so instead of waiting.
#[tokio::test]
async fn without_a_socket_the_call_fails_clearly() {
    let probe = Probe::default();
    let module = service(&probe).into_rpc_module().unwrap();
    let call = r#"{"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.tokens","params":{"run":"r1"}}}"#;
    let (answer, mut items) = tokio::time::timeout(WAIT, module.raw_json_request(call, 16)).await.expect("no hang").unwrap();
    let answer: Value = serde_json::from_str(answer.get()).unwrap();
    assert!(answer["result"].is_number(), "{answer}");
    let item = tokio::time::timeout(WAIT, items.recv()).await.expect("no hang").unwrap();
    let item: Value = serde_json::from_str(item.get()).unwrap();
    let item = &item["params"]["result"];
    assert_eq!(item["type"], "error", "{item}");
    assert!(item["message"].as_str().unwrap().contains("ingest.tokens"), "{item}");
    let refused = service(&probe).route("ingest.tokens", json!({"run":"r1"}), None).await;
    assert!(refused.err().unwrap().to_string().contains("WebSocket"));
}

// ------------------------------------------------------------------ the gate

const ADA: &str = "key:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Default)]
struct Members(Mutex<Vec<Membership>>);

impl Members {
    fn add(&self, who: &str, role: &str, scope: &str) {
        self.0.lock().unwrap().push(Membership { principal: who.parse().unwrap(), role: plexus_auth_core::RoleName::new(role), scope: ScopeRef::parse(scope) });
    }
}

impl Memberships for Members {
    fn held(&self, who: &plexus_auth_core::identity::Principal) -> Vec<Membership> {
        self.0.lock().unwrap().iter().filter(|m| &m.principal == who).cloned().collect()
    }
}

struct Everything;

impl Resolve for Everything {
    fn chain(&self, target: &str) -> Option<Vec<ScopeRef>> {
        Some(vec![ScopeRef::named(target), ScopeRef::Everywhere])
    }
}

/// Every token signs in as Ada.
struct Ada;

#[async_trait]
impl SessionValidator for Ada {
    async fn validate(&self, _token: &str) -> Option<AuthContext> {
        Some(AuthContext::for_principal(&ADA.parse().unwrap(), "s", vec![], Value::Null))
    }
}

async fn gated(probe: &Probe, members: &Arc<Members>) -> (u16, Gate) {
    let roles = RoleTable::new().define("writer", ["ingest.tokens", "ingest.chunks"]).unwrap();
    let gate = Gate::new(members.clone(), Arc::new(roles), Arc::new(Everything));
    let served = service(probe).with_gate(Arc::new(gate.clone()));
    let validator: Arc<dyn SessionValidator> = Arc::new(Ada);
    let (handle, addr) = serve_websocket_with(served.into_rpc_module().unwrap(), WebSocketConfig::new(0), Some(validator), UpgradeOptions::default()).await.unwrap();
    std::mem::forget(handle);
    (addr.port(), gate)
}

async fn signed_in(port: u16) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    req.headers_mut().insert("authorization", "Bearer ada".parse().unwrap());
    tokio_tungstenite::client_async(req, tcp).await.unwrap().0
}

/// The gate decides at open, before any credit.
#[tokio::test]
async fn the_gate_refuses_at_open_before_any_credit() {
    let probe = Probe::default();
    let members = Arc::new(Members::default());
    let (port, _gate) = gated(&probe, &members).await;
    let mut ws = signed_in(port).await;
    send(&mut ws, json!({"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.tokens","params":{"run":"RUN"}}}).to_string()).await;
    next_raw(&mut ws).await;
    let error = next_item(&mut ws).await;
    assert_eq!((error["type"].as_str(), error["error_kind"].as_str()), (Some("error"), Some("forbidden")), "{error}");
    assert_eq!(error["grant"], "ingest.tokens");
    nothing_for_a_while(&mut ws).await;
}

/// Acceptance 5: a grant revoked mid-stream ends the call `forbidden`: the
/// client told `input_closed` once, later items dropped.
#[tokio::test]
async fn a_grant_revoked_mid_stream_ends_the_call_forbidden() {
    let probe = Probe::default();
    let members = Arc::new(Members::default());
    members.add(ADA, "writer", "RUN");
    let (port, gate) = gated(&probe, &members).await;
    let mut ws = signed_in(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.tokens", json!({"run":"RUN"})).await;
    send(&mut ws, input(sub, 0, &["a"])).await;
    tokio::time::timeout(WAIT, async {
        while probe.seen().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    members.0.lock().unwrap().clear();
    gate.moved(plexus_core::plexus::duplex_gate::GrantsMove::Revoke);
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("forbidden")), "{closed}");
    let error = next_item(&mut ws).await;
    assert_eq!((error["type"].as_str(), error["error_kind"].as_str()), (Some("error"), Some("forbidden")), "{error}");
    until_dropped(&probe, 1).await;
    send(&mut ws, input(sub, 1, &["b"])).await;
    nothing_for_a_while(&mut ws).await;
    assert_eq!(probe.seen(), ["a"]);
}

// ------------------------------------------------------- the emitted client

/// Acceptance 6's Rust half: `client.tokens(run, stream)` on a live socket,
/// far more items than one window, the credit spent underneath.
#[tokio::test]
async fn the_emitted_client_streams_its_input() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let client = IngestClient::new(transport);
    let tokens = futures::stream::iter((0..1000).map(|i| Token { t: format!("t{i}") }));
    let events = client.tokens_all("r1".into(), tokens).await.unwrap();
    assert_eq!(events, [IngestEvent::Ingested { count: 1000 }]);
    let seen = probe.seen();
    assert_eq!(seen.len(), 1000);
    assert!(seen.iter().enumerate().all(|(i, t)| t == &format!("t{i}")), "in order");
}

/// An empty input is an `end` and nothing else.
#[tokio::test]
async fn the_emitted_client_ends_an_empty_input() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let events = IngestClient::new(transport).tokens_all("r1".into(), futures::stream::empty()).await.unwrap();
    assert_eq!(events, [IngestEvent::Ingested { count: 0 }]);
}

/// A slow input: the call waits for it, and the output stays live.
#[tokio::test]
async fn the_emitted_client_feeds_as_items_come() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let client = IngestClient::new(transport);
    let tokens = async_stream::stream! {
        for i in 0..5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            yield Token { t: format!("t{i}") };
        }
    };
    let mut events = client.tokens("r1".into(), Box::pin(tokens));
    let first = tokio::time::timeout(WAIT, events.next()).await.unwrap().unwrap();
    assert!(matches!(first, Ok(ClientEvent::Data(IngestEvent::Ingested { count: 5 }))), "{first:?}");
    assert!(events.next().await.is_none());
}

/// A refusal reaches the caller as what it is, and the stream ends.
#[tokio::test]
async fn the_emitted_client_hears_a_refusal() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let client = IngestClient::new(transport);
    let tokens = futures::stream::iter((0..10).map(|i| Token { t: format!("t{i}") }));
    let err = client.capped_all(tokens).await.unwrap_err();
    assert!(matches!(&err, ClientError::Remote { code: Some(c), .. } if c == "too_much_input"), "{err:?}");
}

/// Dropping the client's stream cancels the call.
#[tokio::test]
async fn dropping_the_clients_stream_cancels_the_call() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let client = IngestClient::new(transport);
    let tokens = futures::stream::pending::<Token>();
    let mut events = client.tokens("r1".into(), tokens);
    // A poll, so the call is established.
    assert!(tokio::time::timeout(QUIET, events.next()).await.is_err());
    drop(events);
    until_dropped(&probe, 1).await;
}

// ------------------------------------------- BIDIR-17: binary input frames

/// `[0x01][subscription: u64 BE][seq: u64 BE][payload]`, one item a frame.
fn binary(sub: u64, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(17 + payload.len());
    frame.push(0x01);
    frame.extend_from_slice(&sub.to_be_bytes());
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

async fn send_binary(ws: &mut Ws, frame: Vec<u8>) {
    ws.send(Message::binary(frame)).await.unwrap();
}

fn chunks(probe: &Probe) -> Vec<Vec<u8>> {
    probe.chunks.lock().unwrap().iter().map(|c| c.to_vec()).collect()
}

/// Until `done`, the call's data item.
async fn data_then_done(ws: &mut Ws) -> Value {
    loop {
        let item = next_item(ws).await;
        match item["type"].as_str() {
            Some("ack" | "credit") => {}
            Some("data") => {
                assert_eq!(next_item(ws).await["type"], "done");
                return item["content"].clone();
            }
            other => panic!("{other:?}: {item}"),
        }
    }
}

/// Acceptance 1: binary frames reach the handler as the bytes sent, in
/// order: any byte, a payload that begins `{`, an empty one. `end` is JSON.
#[tokio::test]
async fn binary_frames_reach_the_handler_exactly_in_order() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, credit) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    assert_eq!(credit, 4);
    let payloads: Vec<Vec<u8>> = vec![vec![0, 1, 2, 255], b"{\"not\":\"json to anyone\"}".to_vec(), vec![], (0..=255u8).collect()];
    for (seq, p) in payloads.iter().enumerate() {
        send_binary(&mut ws, binary(sub, seq as u64, p)).await;
    }
    send(&mut ws, end(sub, payloads.len() as u64)).await;
    assert_eq!(data_then_done(&mut ws).await, json!({"type":"ingested","count":4}));
    assert_eq!(chunks(&probe), payloads);
}

/// Acceptance 3: the same call as base64 JSON items, and the two kinds of
/// frame in one sequence.
#[tokio::test]
async fn the_same_call_works_as_base64_json() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    send(&mut ws, json!({"jsonrpc":"2.0","method":"menger.input","params":{"subscription":sub,"seq":0,"items":["AAEC/w==", ""]}}).to_string()).await;
    send_binary(&mut ws, binary(sub, 1, b"raw")).await;
    send(&mut ws, end(sub, 2)).await;
    assert_eq!(data_then_done(&mut ws).await, json!({"type":"ingested","count":3}));
    assert_eq!(chunks(&probe), vec![vec![0, 1, 2, 255], vec![], b"raw".to_vec()]);
}

/// A JSON item of a binary input that is not base64 is `invalid_input`; so is
/// a binary frame for an input of JSON items.
#[tokio::test]
async fn an_item_of_the_wrong_kind_is_invalid_input() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    send(&mut ws, json!({"jsonrpc":"2.0","method":"menger.input","params":{"subscription":sub,"seq":0,"items":["not base64!"]}}).to_string()).await;
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64()), (Some("invalid_input"), Some(0)), "{error}");

    let (sub, _) = open(&mut ws, 2, "ingest.tokens", json!({"run":"r"})).await;
    send_binary(&mut ws, binary(sub, 0, br#"{"t":"a"}"#)).await;
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64()), (Some("invalid_input"), Some(0)), "{error}");
}

/// Acceptance 2: a malformed binary frame (a short header, a tag that is not
/// `0x01`) is one typed error, and the socket goes on.
#[tokio::test]
async fn a_malformed_binary_frame_is_one_typed_error() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    for frame in [vec![0x01, 0, 0, 0], vec![0x7f, 1, 2, 3], vec![]] {
        send_binary(&mut ws, frame.clone()).await;
        let answer: Value = serde_json::from_str(&next_raw(&mut ws).await).unwrap();
        assert_eq!(answer["id"], Value::Null, "{answer}");
        assert_eq!(answer["error"]["data"]["error_kind"], "malformed_frame", "{frame:?}: {answer}");
    }
    let (sub, _) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    send_binary(&mut ws, binary(sub, 0, b"ok")).await;
    send(&mut ws, end(sub, 1)).await;
    assert_eq!(data_then_done(&mut ws).await["count"], 1);
}

/// Acceptance 2: a frame naming no call of this socket is dropped (and
/// counted, as a stranger's notification is), with a call open or none.
#[tokio::test]
async fn a_strangers_binary_frame_is_dropped() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    send_binary(&mut ws, binary(424242, 0, b"no one's")).await;
    nothing_for_a_while(&mut ws).await;
    let (sub, _) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    send_binary(&mut ws, binary(sub + 1, 0, b"no one's")).await;
    nothing_for_a_while(&mut ws).await;
    send_binary(&mut ws, binary(sub, 0, b"mine")).await;
    send(&mut ws, end(sub, 1)).await;
    assert_eq!(data_then_done(&mut ws).await["count"], 1);
    assert_eq!(chunks(&probe), vec![b"mine".to_vec()]);
}

/// Acceptance 2: past credit in binary frames is `past_credit`, as in JSON.
#[tokio::test]
async fn past_credit_in_binary_frames_is_past_credit() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    let (sub, credit) = open(&mut ws, 1, "ingest.chunks", json!({"run":"r"})).await;
    for seq in 0..=credit {
        send_binary(&mut ws, binary(sub, seq, b"x")).await;
    }
    assert_eq!(next_item(&mut ws).await["type"], "input_closed");
    let error = next_item(&mut ws).await;
    assert_eq!((error["error_kind"].as_str(), error["seq"].as_u64()), (Some("past_credit"), Some(credit)), "{error}");
}

/// Acceptance 2: a grant revoked mid-stream ends a binary call `forbidden`.
#[tokio::test]
async fn a_revoked_grant_ends_a_binary_call() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let members = Arc::new(Members::default());
    members.add(ADA, "writer", "RUN");
    let (port, gate) = gated(&probe, &members).await;
    let mut ws = signed_in(port).await;
    let (sub, _) = open(&mut ws, 1, "ingest.chunks", json!({"run":"RUN"})).await;
    send_binary(&mut ws, binary(sub, 0, b"a")).await;
    tokio::time::timeout(WAIT, async {
        while probe.chunks.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    members.0.lock().unwrap().clear();
    gate.moved(plexus_core::plexus::duplex_gate::GrantsMove::Revoke);
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("forbidden")), "{closed}");
    let error = next_item(&mut ws).await;
    assert_eq!(error["error_kind"], "forbidden", "{error}");
    send_binary(&mut ws, binary(sub, 1, b"b")).await;
    nothing_for_a_while(&mut ws).await;
    assert_eq!(chunks(&probe), vec![b"a".to_vec()]);
}

/// The schema marks the input binary: base64 as JSON, raw in a frame.
#[tokio::test]
async fn the_schema_marks_the_input_binary() {
    let probe = Probe::default();
    let port = served(&probe).await;
    let mut ws = connect(port).await;
    send(&mut ws, json!({"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.schema","params":{"method":"chunks"}}}).to_string()).await;
    next_raw(&mut ws).await;
    let input = next_item(&mut ws).await["content"]["input"].clone();
    assert_eq!((input["type"].as_str(), input["contentEncoding"].as_str(), input["x-plexus-binary"].as_bool()), (Some("string"), Some("base64"), Some(true)), "{input}");
}

/// Counts the binary input frames a socket reads.
struct Binaries(Arc<std::sync::atomic::AtomicUsize>);

impl plexus_transport::ConnectionHook for Binaries {
    fn on_frame(&self, frame: &[u8]) -> bool {
        if frame.first() == Some(&0x01) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        false
    }
}

/// The emitted client sends `Input<Bytes>` as binary frames, one item each.
#[tokio::test]
async fn the_emitted_client_sends_bytes_as_binary_frames() {
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let counted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook = counted.clone();
    let options = UpgradeOptions {
        hooks: Some(plexus_transport::Hooks::new(move |_, _| Some(Arc::new(Binaries(hook.clone())) as Arc<dyn plexus_transport::ConnectionHook>))),
        ..UpgradeOptions::default()
    };
    let port = serve(service(&probe).into_rpc_module().unwrap(), options).await;
    let transport = WsTransport::connect(&format!("ws://127.0.0.1:{port}/"), "menger").await.unwrap();
    let client = IngestClient::new(transport);
    let sent: Vec<bytes::Bytes> = (0..100u32).map(|i| bytes::Bytes::from(i.to_be_bytes().repeat(i as usize % 7))).collect();
    let events = client.chunks_all("r".into(), futures::stream::iter(sent.clone())).await.unwrap();
    assert_eq!(events, [IngestEvent::Ingested { count: 100 }]);
    assert_eq!(chunks(&probe), sent.iter().map(|b| b.to_vec()).collect::<Vec<_>>());
    assert_eq!(counted.load(std::sync::atomic::Ordering::SeqCst), 100, "one binary frame an item");
}
