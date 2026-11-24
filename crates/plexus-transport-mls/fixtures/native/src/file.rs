//! The vector file: `vectors/v1.json`.
//!
//! ```json
//! { "profile": "plexus-transport-mls/wire", "version": 1, "test_key": {...},
//!   "vectors": [ { "name": "...", "record": "intent", "about": "...",
//!                  "input": ["a1...", {"repeat": "00", "times": 65464}, "..."],
//!                  "expect": {"result": "ok", "intent": "...", "digest": "...", "view": "..."} },
//!                { ..., "expect": {"result": "error", "code": "cbor.duplicate_key"} } ] }
//! ```
//!
//! An input is the concatenation of its pieces: a string is hex, an object is
//! its `repeat` hex `times` times over. `intent` and `digest` are `null` for a
//! record that carries no intent.

use serde_json::{json, Value};

use plexus_transport_mls::wire::conformance::Record;

use crate::vectors::{hex, signing_pub, unhex, Expect, Segment, Vector, TEST_SCALAR};

/// The file's text for `vectors`.
pub fn write(vectors: &[Vector]) -> String {
    let vectors: Vec<Value> = vectors
        .iter()
        .map(|v| {
            let input: Vec<Value> = v
                .input
                .iter()
                .map(|s| match s {
                    Segment::Bytes(bytes) => json!(hex(bytes)),
                    Segment::Repeat(bytes, times) => json!({"repeat": hex(bytes), "times": times}),
                })
                .collect();
            let expect = match &v.expect {
                Expect::Ok {
                    intent,
                    digest,
                    view,
                } => json!({"result": "ok", "intent": intent, "digest": digest, "view": view}),
                Expect::Error(code) => json!({"result": "error", "code": code}),
            };
            json!({
                "name": v.name,
                "record": v.record.as_str(),
                "about": v.about,
                "input": input,
                "expect": expect,
            })
        })
        .collect();
    let file = json!({
        "profile": "plexus-transport-mls/wire",
        "version": 1,
        "test_key": {
            "about": "P-256 key of every signature in this file. Published test material: it protects nothing.",
            "private_scalar": hex(&TEST_SCALAR),
            "public_sec1": hex(&signing_pub()),
        },
        "vectors": vectors,
    });
    let mut text = serde_json::to_string_pretty(&file).expect("JSON");
    text.push('\n');
    text
}

/// The vectors of a file's text.
pub fn read(text: &str) -> Result<Vec<Vector>, String> {
    let file: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    if file["version"] != json!(1) {
        return Err("not a version 1 vector file".into());
    }
    let str_of = |v: &Value, what: &str| -> Result<String, String> {
        v.as_str()
            .map(String::from)
            .ok_or_else(|| format!("{what} is not a string"))
    };
    let opt_str = |v: &Value| v.as_str().map(String::from);
    file["vectors"]
        .as_array()
        .ok_or("no vectors")?
        .iter()
        .map(|v| {
            let name = str_of(&v["name"], "name")?;
            let record = Record::from_name(&str_of(&v["record"], "record")?)
                .ok_or_else(|| format!("{name}: unknown record"))?;
            let input = v["input"]
                .as_array()
                .ok_or_else(|| format!("{name}: no input"))?
                .iter()
                .map(|s| {
                    let bad = || format!("{name}: bad input piece");
                    match s {
                        Value::String(h) => unhex(h).map(Segment::Bytes).ok_or_else(bad),
                        piece => Ok(Segment::Repeat(
                            piece["repeat"].as_str().and_then(unhex).ok_or_else(bad)?,
                            piece["times"].as_u64().ok_or_else(bad)? as usize,
                        )),
                    }
                })
                .collect::<Result<_, String>>()?;
            let expect = match v["expect"]["result"].as_str() {
                Some("ok") => Expect::Ok {
                    intent: opt_str(&v["expect"]["intent"]),
                    digest: opt_str(&v["expect"]["digest"]),
                    view: str_of(&v["expect"]["view"], "view")?,
                },
                Some("error") => Expect::Error(str_of(&v["expect"]["code"], "code")?),
                _ => return Err(format!("{name}: unknown result")),
            };
            Ok(Vector {
                name,
                record,
                about: str_of(&v["about"], "about")?,
                input,
                expect,
            })
        })
        .collect()
}
