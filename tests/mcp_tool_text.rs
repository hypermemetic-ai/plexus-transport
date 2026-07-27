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

//! # PLX-145
//!
//! The five branches below are **unchanged**. PLX-145 did not touch
//! `render_tool_text`; it added `tool_payload`, the item-level rule that decides
//! what reaches the buffer this function renders. The two are pinned separately
//! on purpose — the count-and-type rule the CLI depends on is one contract, the
//! turn-envelope decision is another — and the second half of this file pins the
//! new one, including what it changes for every other MCP tool.

use plexus_transport::mcp::bridge::{render_tool_text, tool_payload};
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

/// The mixed case. **PLX-145 correction to this test's premise, not its
/// assertion**: this *was* the shape the turn runtime produced for a streaming
/// method, because the projection emits the yielded item as a `Data` update and
/// the terminal as another `Data`, and the gateway read neither's
/// `content_type`. It is no longer reachable that way — `tool_payload` now
/// strips a `complete`/null terminal before the buffer — but the rule below is
/// still the rule for any two mixed-type payloads that *do* reach the buffer,
/// so it stays pinned exactly as written. See
/// `an_update_plus_a_complete_terminal_is_now_just_the_update` for the
/// end-to-end counterpart.
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
/// more non-string `Data` payload. **PLX-145 kept it that way deliberately** —
/// `tool_payload` passes a non-`complete` terminal through whole, so a refusal
/// still renders exactly like this.
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

// ===========================================================================
// PLX-145 — the item-level rule: what a `Data` item contributes to the buffer
// ===========================================================================
//
// `render_tool_text` above sees an anonymous list of payloads. `tool_payload` is
// the half that reads `content_type`, and it is where the gateway-wide decision
// lives: **strip the turn envelope, and nothing else**. Every case a live MCP
// tool can produce is asserted below, including the ones that did not change —
// PLX-145's third criterion is that nothing moves silently.

/// The turn projection's terminal for a normally-completed turn with no value:
/// it contributes **nothing**. This is the whole of the permission fix — it is
/// what takes `permit` from two buffered items back to one.
#[test]
fn a_complete_terminal_with_a_null_value_contributes_nothing() {
    let terminal = json!({"stop": {"kind": "complete"}, "value": null});
    assert_eq!(tool_payload("loopback.permit.terminal", &terminal), None);
}

/// End to end at this layer: an update plus a `complete` terminal is now a
/// one-item buffer, and the text is the payload itself — a bare object, which is
/// exactly what the CLI's `--permission-prompt-tool` contract requires.
///
/// This is the counterpart to `an_update_plus_a_terminal_becomes_a_json_array`,
/// which pinned the broken shape.
#[test]
fn an_update_plus_a_complete_terminal_is_now_just_the_update() {
    let update = json!(r#"{"behavior":"allow","updatedInput":{}}"#);
    let terminal = json!({"stop": {"kind": "complete"}, "value": null});

    let buffer: Vec<_> = [
        ("loopback.permit.update", &update),
        ("loopback.permit.terminal", &terminal),
    ]
    .into_iter()
    .filter_map(|(ct, c)| tool_payload(ct, c))
    .collect();

    assert_eq!(buffer.len(), 1, "the terminal envelope is gone");
    let text = render_tool_text(&buffer);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(parsed.is_object(), "the CLI contract is a bare object: {text}");
    assert_eq!(parsed["behavior"], "allow");
}

/// **Change #1, asserted rather than discovered later.** A unary `#[method]`
/// emits exactly one terminal carrying its return value. Its tool text was the
/// envelope; it is now the value the method declared.
#[test]
fn a_unary_methods_text_is_now_its_return_value_not_the_envelope() {
    let terminal = json!({
        "stop": {"kind": "complete"},
        "value": {"session_id": "s-1", "created": true}
    });

    // Before PLX-145 the whole envelope was buffered.
    let before = render_tool_text(std::slice::from_ref(&terminal));
    assert!(before.contains("\"stop\""), "the old text carried the envelope");

    let after = render_tool_text(
        &[tool_payload("sessions.create.terminal", &terminal)].map(Option::unwrap),
    );
    let parsed: serde_json::Value = serde_json::from_str(&after).unwrap();
    assert_eq!(parsed["session_id"], "s-1");
    assert!(parsed.get("stop").is_none(), "no envelope survives: {after}");
}

/// **Change #2.** `Result<(), E>` — a terminal with no value — becomes
/// `(no output)` where it used to be `{"stop":…,"value":null}`.
#[test]
fn a_unit_returning_method_is_now_no_output() {
    let terminal = json!({"stop": {"kind": "complete"}, "value": null});
    let buffer: Vec<_> = [tool_payload("sessions.delete.terminal", &terminal)]
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(render_tool_text(&buffer), "(no output)");
}

/// **Change #3.** A streaming method's text is the updates alone. An all-string
/// stream therefore hits `render_tool_text`'s `join("")` branch instead of
/// becoming an array — the same concatenation PLX-105 corrected the record on,
/// unchanged and still load-bearing.
#[test]
fn a_streaming_methods_text_is_the_updates_alone() {
    let a = json!("tok-a");
    let b = json!("tok-b");
    let terminal = json!({"stop": {"kind": "complete"}, "value": null});

    let buffer: Vec<_> = [
        ("chat.send.update", &a),
        ("chat.send.update", &b),
        ("chat.send.terminal", &terminal),
    ]
    .into_iter()
    .filter_map(|(ct, c)| tool_payload(ct, c))
    .collect();

    assert_eq!(render_tool_text(&buffer), "tok-atok-b");
}

/// A streaming method whose turn ALSO carries a terminal value keeps both: the
/// updates and the value, with only the wrapper removed.
#[test]
fn a_terminal_value_alongside_updates_is_kept_unwrapped() {
    let update = json!({"chunk": 1});
    let terminal = json!({"stop": {"kind": "complete"}, "value": {"total": 1}});

    let buffer: Vec<_> = [("x.y.update", &update), ("x.y.terminal", &terminal)]
        .into_iter()
        .filter_map(|(ct, c)| tool_payload(ct, c))
        .collect();

    assert_eq!(buffer, vec![json!({"chunk": 1}), json!({"total": 1})]);
}

/// **Unchanged #4 — and the reason the rule is not "render only the value".**
/// A `Failed` terminal is passed through whole, so the structured error the
/// turn model carries (RFC 002 §6.7.1) still reaches the tool caller.
#[test]
fn a_failed_terminal_is_passed_through_whole_error_and_all() {
    let terminal = json!({
        "stop": {"kind": "failed", "error": {"code": "plexus.boom", "message": "boom"}},
        "value": null
    });
    let kept = tool_payload("x.y.terminal", &terminal).expect("a failure is never dropped");
    assert_eq!(kept, terminal);
    assert_eq!(kept["stop"]["error"]["code"], "plexus.boom");
}

/// **Unchanged #5.** The same for a refusal and a limit — a considered NO is
/// exactly the case the turn model exists to keep legible.
#[test]
fn refused_limited_and_cancelled_terminals_are_passed_through_whole() {
    for kind in ["refused", "limited", "cancelled"] {
        let terminal = json!({
            "stop": {"kind": kind, "detail": {"code": "loopback.permission_denied"}},
            "value": null
        });
        assert_eq!(
            tool_payload("loopback.permit.terminal", &terminal).as_ref(),
            Some(&terminal),
            "a `{kind}` terminal must survive the gateway intact"
        );
    }
}

/// **Unchanged #6.** An update is never touched, whatever its shape.
#[test]
fn an_update_is_always_contributed_verbatim() {
    for content in [json!("a string"), json!({"o": 1}), json!([1, 2]), json!(null)] {
        assert_eq!(tool_payload("ns.m.update", &content), Some(content.clone()));
    }
}

/// **Unchanged #7.** Bidirectional methods and hand-written activations emit
/// `Data` with a legacy `<ns>.<method>` `content_type` and no terminal at all.
/// Nothing about them changes.
#[test]
fn legacy_and_bidirectional_content_types_are_untouched() {
    for ct in ["bash.execute", "health.status", "chat.stream"] {
        let content = json!({"stop": {"kind": "complete"}, "value": 1});
        assert_eq!(
            tool_payload(ct, &content),
            Some(content.clone()),
            "`{ct}` has no `.terminal` suffix, so the envelope guard must not fire"
        );
    }
}

/// The guard is structural as well as nominal: a method genuinely *named*
/// `terminal` is not mistaken for the turn projection's envelope.
#[test]
fn a_method_named_terminal_is_not_mistaken_for_an_envelope() {
    // Right suffix, wrong shape — three keys, so not the envelope.
    let payload = json!({"stop": {"kind": "complete"}, "value": 1, "rows": 2});
    assert_eq!(tool_payload("tty.terminal", &payload), Some(payload.clone()));

    // Right suffix, right key count, but `stop` is not a tagged StopReason.
    let payload = json!({"stop": "yes", "value": 1});
    assert_eq!(tool_payload("tty.terminal", &payload), Some(payload.clone()));

    // Right suffix, not an object at all.
    let payload = json!("just a string");
    assert_eq!(tool_payload("tty.terminal", &payload), Some(payload.clone()));
}
