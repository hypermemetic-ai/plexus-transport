//! TRANSPORT-17: spec/11 on a real pair of pipes. "A writer MUST write each
//! message and its `\n` as one unit: two streams' notifications interleaved
//! inside a line break the framing for good."
//!
//! This file is its own child: run with `--serve` it is `serve_stdio` on its own
//! stdin and stdout, so nothing here stands in for them (`harness = false` keeps
//! libtest's lines off that stdout). Run without it, it is the client.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use jsonrpsee::RpcModule;
use plexus_transport::{stdio::serve_stdio, StdioConfig};
use serde_json::{json, Value};

/// `ping`, an answer and no stream; `stream [tag, n, size]`, `n` notifications
/// of `size` bytes and an end; `n` of `u32::MAX` runs until it is let go;
/// `flood [size]`, one item as fast as it will go, until it is let go.
fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |params, _, _| params.one::<u64>().unwrap_or(0)).unwrap();
    m.register_subscription("stream", "note", "unstream", |params, pending, _, _| async move {
        let (tag, n, size): (String, u32, usize) = params.parse()?;
        let sink = pending.accept().await?;
        let pad = "x".repeat(size);
        for i in 0..n {
            sink.send(serde_json::value::to_raw_value(&json!({"tag": tag, "i": i, "pad": pad}))?).await?;
        }
        jsonrpsee::core::SubscriptionResult::Ok(())
    })
    .unwrap();
    m.register_subscription("flood", "note", "unflood", |params, pending, _, _| async move {
        let size: usize = params.one()?;
        let sink = pending.accept().await?;
        let item = serde_json::value::to_raw_value(&"x".repeat(size))?;
        while sink.send(item.clone()).await.is_ok() {}
        jsonrpsee::core::SubscriptionResult::Ok(())
    })
    .unwrap();
    m
}

fn serve() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    runtime.block_on(serve_stdio(module(), StdioConfig::default())).unwrap();
}

/// A child, and this side of its pipes.
struct Link {
    child: Child,
    stdin: Option<ChildStdin>,
    out: BufReader<ChildStdout>,
    next: u64,
}

impl Drop for Link {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Link {
    fn open() -> Link {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--serve")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let (stdin, out) = (child.stdin.take(), BufReader::new(child.stdout.take().unwrap()));
        Link { child, stdin, out, next: 0 }
    }

    fn send(&mut self, method: &str, params: Value) -> u64 {
        self.next += 1;
        let line = format!("{}\n", json!({"jsonrpc": "2.0", "id": self.next, "method": method, "params": params}));
        self.stdin.as_mut().unwrap().write_all(line.as_bytes()).unwrap();
        self.next
    }

    /// The next line, which must be one JSON value; `None` at the end of its stdout.
    fn line(&mut self) -> Option<Value> {
        let mut raw = Vec::new();
        if self.out.read_until(b'\n', &mut raw).unwrap() == 0 {
            return None;
        }
        assert_eq!(raw.pop(), Some(b'\n'), "a line cut short");
        match serde_json::from_slice(&raw) {
            Ok(v) => Some(v),
            Err(e) => panic!("not one message on a line ({e}), two streams interleaved: {}", String::from_utf8_lossy(&raw[..raw.len().min(160)])),
        }
    }

    /// Reads until every stream in `want` (tag → items) has ended, and holds what
    /// every line must: it parses, each stream's items are in order and nothing
    /// is missing, and a stream's answer comes before its first item.
    fn streams(&mut self, want: &HashMap<&str, u32>, ids: &HashMap<u64, &str>) {
        let total: u32 = want.values().sum();
        let (mut answered, mut seen, mut got) = (HashMap::new(), HashMap::<String, u32>::new(), 0);
        while got < total {
            let v = self.line().expect("every item arrives");
            if let Some(id) = v["id"].as_u64() {
                let sub = v["result"].clone();
                assert!(!sub.is_null(), "{v}");
                answered.insert(sub.to_string(), ids[&id]);
                continue;
            }
            let (sub, item) = (&v["params"]["subscription"], &v["params"]["result"]);
            let tag = item["tag"].as_str().expect("an item");
            assert_eq!(answered.get(&sub.to_string()), Some(&tag), "an item before its stream's answer: {sub}");
            let n = seen.entry(tag.to_string()).or_default();
            assert_eq!(item["i"].as_u64(), Some(*n as u64), "stream {tag} out of order");
            *n += 1;
            got += 1;
        }
        assert_eq!(answered.len(), want.len());
    }
}

/// Two streams live at once: every line is one message. On a writer per stream
/// this fails within the first few hundred lines.
fn two_live_streams_never_share_a_line() {
    let mut l = Link::open();
    let a = l.send("stream", json!(["a", 4000, 2000]));
    let b = l.send("stream", json!(["b", 4000, 2000]));
    l.streams(&HashMap::from([("a", 4000), ("b", 4000)]), &HashMap::from([(a, "a"), (b, "b")]));
}

/// Lines past a pipe's 64 KiB go out in many writes, and stay whole.
fn lines_past_a_pipe_stay_whole() {
    let mut l = Link::open();
    let a = l.send("stream", json!(["a", 40, 300_000]));
    let b = l.send("stream", json!(["b", 40, 300_000]));
    l.streams(&HashMap::from([("a", 40), ("b", 40)]), &HashMap::from([(a, "a"), (b, "b")]));
}

/// Answers keep their order among the items of a live stream.
fn answers_keep_their_order_beside_a_stream() {
    let mut l = Link::open();
    let s = l.send("stream", json!(["a", 2000, 1000]));
    let pings: Vec<u64> = (0..200).map(|i| l.send("ping", json!([i]))).collect();
    let (mut items, mut next, mut sub) = (0, 0, None);
    while items < 2000 || next < pings.len() {
        let v = l.line().expect("every line arrives");
        match v["id"].as_u64() {
            Some(id) if id == s => sub = Some(v["result"].clone()),
            Some(id) => {
                assert_eq!(id, pings[next], "answers out of order");
                assert_eq!(v["result"].as_u64(), Some(next as u64), "{v}");
                next += 1;
            }
            None => {
                assert_eq!(Some(&v["params"]["subscription"]), sub.as_ref(), "an item before its answer");
                items += 1;
            }
        }
    }
}

/// End of input: every answer already made still goes out.
fn the_last_answers_survive_the_end_of_input() {
    let mut l = Link::open();
    for i in 0..500 {
        l.send("ping", json!([i]));
    }
    l.stdin = None;
    for i in 0..500 {
        let v = l.line().unwrap_or_else(|| panic!("answer {i} was lost at the end of input"));
        assert_eq!(v["result"].as_u64(), Some(i), "{v}");
    }
    assert!(l.line().is_none());
    assert!(l.child.wait().unwrap().success());
}

/// spec/11: "EOF on stdin ends every call; the child exits."
fn the_end_of_input_ends_a_stream_that_never_would() {
    let mut l = Link::open();
    l.send("stream", json!(["a", u32::MAX, 100]));
    while l.line().expect("items").get("params").is_none() {}
    l.stdin = None;
    let (out, child) = (&mut l.out, &mut l.child);
    std::thread::scope(|scope| {
        scope.spawn(move || std::io::copy(out, &mut std::io::sink()));
        let t0 = Instant::now();
        while child.try_wait().unwrap().is_none() {
            if t0.elapsed() > Duration::from_secs(2) {
                child.kill().unwrap();
                panic!("the child did not exit at the end of its input, with a stream live");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
}

/// A reader that stops holds the stream back: a flood nobody reads leaves the
/// child no larger than its queues.
fn a_reader_that_stops_holds_the_stream_back() {
    let mut l = Link::open();
    l.send("flood", json!([4000]));
    std::thread::sleep(Duration::from_millis(1500));
    let status = std::fs::read_to_string(format!("/proc/{}/status", l.child.id())).unwrap();
    let rss_kb: u64 = status.lines().find_map(|s| s.strip_prefix("VmRSS:")).and_then(|s| s.split_whitespace().next()).and_then(|s| s.parse().ok()).unwrap();
    eprintln!("{rss_kb} kB resident with nobody reading");
    assert!(rss_kb < 64_000, "{rss_kb} kB resident with nobody reading: the queue is not holding the stream back");
    assert_eq!(l.line().expect("the answer")["id"], 1);
    for _ in 0..1000 {
        assert_eq!(l.line().expect("an item")["params"]["result"].as_str().map(str::len), Some(4000));
    }
}

fn main() {
    if std::env::args().any(|a| a == "--serve") {
        return serve();
    }
    let tests: [(&str, fn()); 6] = [
        ("two_live_streams_never_share_a_line", two_live_streams_never_share_a_line),
        ("lines_past_a_pipe_stay_whole", lines_past_a_pipe_stay_whole),
        ("answers_keep_their_order_beside_a_stream", answers_keep_their_order_beside_a_stream),
        ("the_last_answers_survive_the_end_of_input", the_last_answers_survive_the_end_of_input),
        ("the_end_of_input_ends_a_stream_that_never_would", the_end_of_input_ends_a_stream_that_never_would),
        ("a_reader_that_stops_holds_the_stream_back", a_reader_that_stops_holds_the_stream_back),
    ];
    let mut failed = 0;
    for (name, test) in tests {
        match std::panic::catch_unwind(test) {
            Ok(()) => println!("test {name} ... ok"),
            Err(_) => {
                println!("test {name} ... FAILED");
                failed += 1;
            }
        }
    }
    println!("test result: {}. {} passed; {failed} failed", if failed == 0 { "ok" } else { "FAILED" }, tests.len() - failed);
    std::process::exit((failed > 0) as i32);
}
