//! PLX-105 — the MCP gateway's `content[0].text` rule, asserted.
//!
//! # Why this file exists
//!
//! PLX-116 measured that **no test anywhere in `plexus-transport` or
//! `plexus-substrate` asserted `content[0].text`**, and stopped rather than
//! change the shape of `claudecode_loopback`'s `permit` blind. `tests/mcp_auth.rs`
//! and `tests/rawctx.rs` stop at the `PlexusStream`; `call_tool` — where the
//! buffered items become the single text payload a tool caller reads back — had
//! no coverage at all.
//!
//! That payload is a **protocol surface owned by the caller, not by us**: a
//! Claude CLI spawned with `--permission-prompt-tool` parses `content[0].text`
//! as the permission decision. The rule that produces it is count- **and**
//! type-dependent, so a change in how many items a method emits changes the
//! top-level JSON type of that text.
//!
//! Every case below is measured against `mcp::bridge::render_tool_text`, the
//! function `call_tool` itself calls.

use plexus_transport::mcp::bridge::render_tool_text;
use serde_json::json;

/// The shape `claudecode_loopback::permit` was written for: exactly one buffered
/// item, and it is a JSON **string**. The string's *contents* are the text — no
/// quoting, no wrapper. This is the only shape the CLI's permission contract
/// accepts.
#[test]
fn one_string_item_is_unwrapped_to_its_contents() {
    let permit_payload = r#"{"behavior":"allow","updatedInput":{"file":"/tmp/x"}}"#;
    let text = render_tool_text(&[json!(permit_payload)]);
    assert_eq!(text, permit_payload);
    // And it really is what a permission-prompt-tool caller would parse.
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["behavior"], "allow");
}

/// A denial travels the same way today — as an `Ok`-valued payload, not an error.
#[test]
fn a_denial_is_an_ordinary_one_string_payload() {
    let deny = r#"{"behavior":"deny","message":"Denied by parent"}"#;
    assert_eq!(render_tool_text(&[json!(deny)]), deny);
}

/// One item that is **not** a string is pretty-printed instead of unwrapped, so
/// the top-level JSON type the caller sees is the value's own.
#[test]
fn one_non_string_item_is_pretty_printed_not_unwrapped() {
    let text = render_tool_text(&[json!({"behavior": "allow"})]);
    assert_eq!(text, "{\n  \"behavior\": \"allow\"\n}");
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(parsed.is_object());
}

/// **The rule PLX-116 recorded slightly wrong, and it matters.** More than one
/// item is *not* unconditionally an array: if every item is a string they are
/// concatenated with no separator. A method that emits two string updates
/// therefore yields a single run-together string, which for a JSON-parsing
/// caller is neither of the two payloads.
#[test]
fn many_all_string_items_are_concatenated_not_arrayed() {
    let text = render_tool_text(&[json!("{\"a\":1}"), json!("{\"b\":2}")]);
    assert_eq!(text, "{\"a\":1}{\"b\":2}");
    assert!(
        serde_json::from_str::<serde_json::Value>(&text).is_err(),
        "concatenating two JSON documents does not produce JSON"
    );
}

/// The mixed case — and this is exactly the shape the turn runtime produces for
/// a streaming method, because `plexus-core`'s projection emits the yielded item
/// as a `Data` update **and the terminal as another `Data`**, distinguished only
/// by `content_type`, which this gateway never reads.
///
/// The text stops being the payload and becomes a **JSON array containing it**.
#[test]
fn an_update_plus_a_terminal_becomes_a_json_array() {
    let update = json!(r#"{"behavior":"allow","updatedInput":{}}"#);
    let terminal = json!({"stop": {"kind": "complete"}, "value": null});
    let text = render_tool_text(&[update, terminal]);

    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        parsed.is_array(),
        "an update and a terminal are two buffered items, so the text is an array: {text}"
    );
    assert_eq!(parsed.as_array().unwrap().len(), 2);
    // The permission payload survives, but only as element 0 of a wrapper the
    // CLI has no contract for.
    assert!(parsed[0].is_string());
    assert_eq!(parsed[1]["stop"]["kind"], "complete");
}

/// A `Refused` terminal is not special-cased anywhere in this rule: it is one
/// more non-string `Data` payload. Pinned so that whoever teaches the gateway to
/// read `content_type` has to change this test deliberately.
#[test]
fn a_refused_terminal_is_just_another_buffered_object() {
    let terminal = json!({
        "stop": {"kind": "refused", "detail": {"code": "loopback.permission_denied"}},
        "value": null
    });
    let text = render_tool_text(&[terminal]);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["stop"]["kind"], "refused");
    assert!(
        !text.contains("behavior"),
        "a refusal carries no permission-prompt payload of its own"
    );
}

#[test]
fn no_items_is_the_literal_no_output() {
    assert_eq!(render_tool_text(&[]), "(no output)");
}
