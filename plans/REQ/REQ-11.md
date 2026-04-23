---
id: REQ-11
title: "REQ-6 end-to-end verification against uscis (FormVeritasV2)"
status: Pending
type: implementation
blocked_by: [REQ-6]
unlocks: []
severity: Low
---

## Problem

REQ-6's plexus-macros change (commit `3aeb9cf` in plexus-macros) is covered by in-repo unit tests but has not been verified against a real consumer. Uscis (FormVeritasV2) was used earlier in the Apr 22-23 autonomous run to verify the earlier REQ-5 + activation-level REQ-7 work, but REQ-6 landed on plexus-macros 0.5.6 while FormVeritasV2 pins `plexus-macros = "0.4"` from crates.io. Running REQ-6's merge semantics against uscis requires patching FormVeritasV2's Cargo.toml to consume the local plexus-macros, rebuilding uscis-notifier, regenerating the IR, and inspecting the per-method schemas for the expected annotations.

## Why this is its own ticket

Doing the verification requires touching a separate repository (`~/dev/hyperforge/workspaces/sshmendez/orgs/OneBigMediaCo/FormVeritasV2/`), which sits outside the plexus-transport / plexus-macros / synapse-cc realm where this epic otherwise lives. The patch + rebuild + regenerate + verify loop is a real test run, not a macro-level assertion. Separating it keeps REQ-6's code change focused on the library and defers the cross-repo integration to a controlled step.

## Required behavior

The implementor:

1. Enables the `[patch.crates-io]` block in `FormVeritasV2/Cargo.toml` that points `plexus-core`, `plexus-macros`, and `plexus-transport` at the local `~/dev/controlflow/hypermemetic/` paths. (The block is currently commented out in FormVeritasV2.)

2. Rebuilds `uscis-notifier` under the patched dependencies. Confirms it compiles without source-code changes to FormVeritasV2 (if REQ-6 introduced a minor breaking change, FormVeritasV2 may need touch-ups — note and resolve).

3. Restarts `uscis-notifier` (port 44902) with a working Postgres + ALLOWED_ORIGINS as before.

4. Regenerates the TypeScript client: `synapse-cc build typescript uscis -o /tmp/cc-uscis-req6 --no-install --no-build --no-tests --force`.

5. Inspects `/tmp/cc-uscis-req6/ir.json`. For each method in each activation that declares `request = FormVeritasRequest`:
   - `params.properties.origin` exists with `x-plexus-source: { from: "derived" }`
   - `params.properties.transport` exists with `x-plexus-source: { from: "derived" }`
   - `params.properties.client_ip` exists with `x-plexus-source: { from: "derived" }`
   - `params.properties.auth_token` IS NOT present (FormVeritasRequest has no cookie field)
   - Methods with `#[from_auth(self.db.validate_user)]` have a param with `x-plexus-source: { from: "auth", resolver: "self.db.validate_user" }` (resolver string match — exact whitespace not required)

6. Inspects `health.check` specifically — it uses `#[plexus_macros::method(request = ())]`. Its `params.properties` should NOT contain `origin`, `transport`, or `client_ip` (the override skips the merge).

7. Decides whether to leave the `[patch.crates-io]` block enabled in FormVeritasV2 (follow-up ticket material) or re-comment it before committing.

## What must NOT change

- FormVeritasV2 source code (beyond Cargo.toml's patch block toggle) — REQ-6 is supposed to be additive.
- Uscis-notifier's runtime behavior — the macro change is schema-only at the method level; dispatch behavior is unchanged.

## Risks

1. **Breaking API changes between plexus-macros 0.4 and 0.5.** If anything that FormVeritasV2 depends on (e.g. `HubMethodsAttrs`-internal fields, proc-macro emission quirks) changed shape between 0.4 and 0.5, the build will fail. Mitigation: note each compile error, fix FormVeritasV2 source minimally, document the delta for a plexus-macros changelog entry.

2. **`plexus-core` version mismatch.** FormVeritasV2 uses plexus-core 0.4.0; local is 0.5.2. The patch block also patches plexus-core, so the full transitive graph resolves to local — but the plexus-core API may also have drifted. Same mitigation: document deltas, fix minimally.

3. **Cargo.toml patch state is visible in `git status`.** If left on, future FormVeritasV2 commits will accidentally include the patch. The implementor must be explicit about whether to revert or merge the toggle.

## Acceptance criteria

1. After enabling the patch and rebuilding uscis, the generated `ir.json` for a non-override activation method (e.g. `clients.list`) contains `params.properties.origin` with `x-plexus-source.from == "derived"`.
2. `clients.list` schema contains a param with `x-plexus-source.from == "auth"` and `resolver` string including `validate_user`.
3. `health.check` schema does NOT contain `origin`, `transport`, or `client_ip` in its params.
4. Client regeneration (`synapse-cc build typescript uscis`) succeeds; tsc-clean.
5. The `[patch.crates-io]` block's final state in FormVeritasV2/Cargo.toml is documented in the commit or follow-up note (on/off, with rationale).

## Coordination

- `blocked_by: [REQ-6]` — needs the macro change landed first
- Optional: `blocked_by: [REQ-10]` if the implementor wants to verify `required = [...]` behavior too. REQ-10 adoption in FormVeritasV2 would be its own source-code change. For v1 verification, leaving REQ-10 out is fine.

## Completion

Implementor walks through steps 1-7 above, captures evidence (a diff of the ir.json between pre-patch and post-patch builds would be ideal), and notes the final patch block state. Commits (or reverts) the FormVeritasV2 change with a clear message. Flips status to Complete.
