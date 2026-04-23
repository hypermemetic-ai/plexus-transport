---
id: RED-5
title: "Permissive-resolver audit tool (S07)"
status: Pending
type: implementation
blocked_by: []
unlocks: []
severity: Low
---

## Problem

RED-S07 confirmed there is no defense against `#[from_auth(self.accept_all)]` where `accept_all` simply returns `Ok(FakeUser)` without examining its `&AuthContext` argument. The macro captures the resolver expression as a string in the wire schema (`x-plexus-source.resolver`), so downstream tools can at least *see* the resolver name — but nothing inspects body semantics.

This is a friendly-attacker surface: a developer writing a test fixture + forgetting to replace the stub, not an adversarial bypass.

## Required behavior

A thin out-of-tree audit tool (`plexus-audit` or a `cargo audit-plexus` subcommand) that:

1. Walks the generated IR / schema from every activation in a workspace
2. Extracts each method's `x-plexus-source.resolver` string
3. Flags names matching any of: `accept_all`, `stub`, `fake`, `mock`, `noop`, `bypass`, `TODO`, `unimplemented`, anything containing "test" in a non-test crate
4. Outputs a report listing flagged resolvers + their locations
5. Returns non-zero exit code if any flagged resolver appears in a non-test build context

### Pattern list (starter)

```
accept_.*
.*_stub
.*_fake
.*_mock
noop.*
bypass.*
unimplemented.*
.*TODO.*
always_.*
permissive_.*
```

Configurable allowlist at `.plexus-audit.toml` in the workspace root:

```toml
allowlist = [
  "self.accept_during_migration_2026",  # documented waiver
]
```

## What must NOT change

- Runtime behavior: this is purely static analysis
- Wire schema: unchanged
- Macro behavior: unchanged

## Acceptance criteria

1. Given a fixture activation with `#[from_auth(self.accept_all)]`, the tool emits a warning line naming the resolver and the method's fully-qualified path.
2. Given a fixture activation with `#[from_auth(self.db.validate_user)]`, the tool emits nothing.
3. Exit code is non-zero when any flagged resolver is found without an allowlist entry.
4. Allowlist file silences specific flagged resolvers.
5. The tool can run against a live uscis IR and produces a clean report.

## Risks

1. **False positives.** Names like `accept_after_validate_2024` trigger the `accept_*` pattern. Allowlist handles this but adds friction.
2. **Regex evasion.** `validate_user_accept_strict` bypasses the naming pattern. Acceptable — this is a nudge, not a wall.
3. **Requires downstream tooling.** SOC2 auditor needs to understand what the tool does. Add a doc snippet in `security-review/SKILL.md`.

## Coordination

- Low severity because the hole requires developer intent (writing a fake resolver) rather than accidental exposure
- Can land after RED-2 / RED-4 which address the higher-severity accidental exposure paths
- Tool lives in a new crate `plexus-audit` under `~/dev/controlflow/hypermemetic/`

## Completion

Implementor builds the audit crate, writes one integration test, documents usage. Flips status when first clean run against uscis passes.
