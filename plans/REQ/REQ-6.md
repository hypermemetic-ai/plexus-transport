---
id: REQ-6
title: "plexus-macros emits per-method merged param schemas with x-plexus-source"
status: Partial
type: implementation
blocked_by: []
unlocks: [REQ-8, REQ-9, SAFE-6]
severity: High
---

**Partial implementation Apr 23 2026 (autonomous run):** Core merge
behavior landed in plexus-macros 0.5.6. `method_enum::generate()` now
takes the activation's request type, threads per-method auth resolver
entries + override flags into the runtime schema generator, and
augments each method's `params.properties` at runtime with:

- `#[from_auth(expr)]` params tagged `x-plexus-source: { from: "auth", resolver: "<expr>" }`
- Activation-level request struct fields merged verbatim (preserving
  their existing `x-plexus-source` from PlexusRequest derive), when
  the method does not override via `request = ()`

Verified by 3 new tests in `req6_from_auth_tests.rs` + an updated test
in `activation_schema_tests.rs`. All 84 plexus-macros tests pass.

**Still deferred:**

- Field-level `required = [...]` locking on the activation attribute.
  Today methods can freely override with `request = ()`; no compile-time
  check prevents dropping a semantically-mandatory field.
- Override validation with clear compile errors (depends on `required`
  parsing landing first).
- Verification end-to-end against uscis (FormVeritasV2 currently pins
  `plexus-macros = "0.4"` from crates.io; would require enabling the
  repo's commented-out `[patch.crates-io]` block to consume the local
  0.5.6 build).

## Problem

Today `#[plexus::activation(request = MyRequest)]` extracts `MyRequest` fields for validation side-effect, but the wire schema doesn't reflect that at the method level. Clients see only the method's RPC-facing params; `#[from_auth]` and `#[activation_param]` params are stripped entirely; the activation's request fields live in a separate `psRequest` block that clients must cross-reference.

This creates three concrete problems:

1. **Per-method auth invisibility.** A tool reading the schema can't tell which methods use `#[from_auth]` or which resolver guards them. The only hint is the activation-level `psRequest`, which is wrong for mixed-auth activations.

2. **Activation-level override is crude.** `#[plexus::method(request = ())]` either drops the whole request struct or keeps it; there's no field-level control. `health.check` uses this to bypass everything, which then breaks any consumer (e.g., REQ-7's minimal JSDoc) that uses psRequest-presence as the sole signal.

3. **JSDoc / help text is imprecise.** Tonight's REQ-7 minimal emits `@server-derived` on every method in any activation with psRequest — including methods that override. The signal is too coarse.

## Goal

Each method's `MethodSchema.params` becomes the complete contract for that method:

- Every parameter appears — including `#[from_auth]`, `#[activation_param]`, and activation-injected request fields
- Each parameter carries `x-plexus-source` identifying where it comes from (rpc / cookie / header / query / derived / auth)
- `required` lists only RPC-sourced parameters; derived/auth params are never required of the client
- Methods inherit the activation's request fields by default; can override individual fields when the activation permits

Activation-level `psRequest` continues to carry the activation's request struct (REQ-5's wire addition); it remains the source of truth for *what gets injected*. REQ-6 makes the injection visible per method.

## Required behavior

### Source annotations

For each method parameter, emit `x-plexus-source` on the parameter's JSON Schema:

| Param shape | `x-plexus-source` value |
|---|---|
| Plain RPC param (today's default) | `{"from": "rpc"}` (or absent — implementor chooses) |
| `#[from_auth(expr)]` | `{"from": "auth", "resolver": "<expr-as-string>"}` |
| `#[activation_param]` whose type comes from the activation's request struct, where that struct's field has `#[from_cookie("name")]` | `{"from": "cookie", "key": "name"}` |
| `#[activation_param]` whose field is `#[from_header("name")]` | `{"from": "header", "key": "name"}` |
| `#[activation_param]` whose field is `#[from_query("name")]` | `{"from": "query", "key": "name"}` |
| `#[activation_param]` whose field is a `PlexusRequestField` newtype (e.g. `ValidOrigin`) | `{"from": "derived"}` |
| `#[from_request(fn)]` with a custom extractor | `{"from": "derived"}` |

### Activation-level field injection

When an activation declares `request = FormVeritasRequest`, every method in that activation *implicitly* has the request's fields merged into its param schema, even when the method's source code doesn't name them. The method's *source-visible* signature is unchanged (only the schema is augmented). Extraction runs at dispatch time for validation side-effects.

When a method explicitly declares a matching param (e.g., `fn list(&self, origin: ValidOrigin, ...)`), the method gets the extracted value passed through; the schema still surfaces it with the right `x-plexus-source`.

### `required` array rules

The method's schema `required` array contains ONLY params whose `x-plexus-source.from` is `"rpc"` or absent. Auth-sourced and derived params are never required of the client — they're populated by the server from connection state.

### Per-method override with field-level locking

The activation attribute gains an optional `required = [field1, field2, ...]` list naming fields that methods CANNOT override:

```rust
#[plexus::activation(
    namespace = "clients",
    request = FormVeritasRequest,
    required = [origin, transport],    // these fields are locked for methods
)]                                      // client_ip is overridable
```

A method's `#[plexus::method(request = OtherRequest)]` is validated at macro expansion:

- Drops a required field → compile error with clear message
- Supplies all required fields (possibly with different types for non-required ones) → allowed; the method's schema reflects OtherRequest's fields
- `request = ()` → compile error if ANY field is required; allowed if `required = []` or absent

When `required = []` or absent, methods may freely override (including `request = ()`). When `required` is non-empty, the activation explicitly controls what can be dropped.

### Strip-vs-emit split in the macro

The macro already strips `#[from_auth]` and `#[activation_param]` from the rustc-visible function signature (so rustc doesn't complain about unknown attributes). REQ-6 keeps that strip for the generated function, but RESTORES those params in the method enum's schema generation path — where they become the source of `x-plexus-source` annotations.

## What must NOT change

- Runtime dispatch behavior for methods without `required = [...]` locking is byte-identical to today
- `PluginSchema.psRequest` (activation-level) continues to exist on the wire (REQ-5 addition). REQ-6 adds *per-method* annotations but does not remove the activation-level schema. UNIFY-1 may eventually drop psRequest.
- The set of methods exposed on the wire is unchanged
- Existing activations without `request = ...` (no activation-level extraction) continue to produce the same method schemas as today (plain RPC params only, no `x-plexus-source` needed)
- `PlexusRequest` derive (REQ-1) and its `request_schema()` output. REQ-6 READS the existing annotations; it doesn't change them.
- Per-method `#[plexus::method(request = ())]` continues to work when the activation has no `required` fields. REQ-6 adds the `required` field-locking mechanism; it doesn't remove override.

## Risks

1. **schemars `extend` on method enum variant fields** may not compose with variant-level attributes the way single-struct `extend` does. Mitigation: REQ-6-S01 spike — write a minimal enum with one variant and a per-field `#[schemars(extend("x-plexus-source" = {...}))]`; verify the annotation lands on the right property in the generated oneOf schema.

2. **`required` array filtering** — schemars puts all non-Option fields in `required` by default. REQ-6 needs non-RPC params to NOT be in `required` despite being declared non-Option. Options: (a) post-process the generated schema per variant, filtering names; (b) wrap non-RPC params in a synthetic Option<T> in the emitted enum variant. Option (a) is less invasive. Decide during implementation.

3. **Validation of override against `required = [...]`** must happen at macro expansion, not at runtime. This is ordinary proc-macro work (compare names, emit compile errors) but adds attribute-parsing complexity.

4. **Resolver expression as a string.** `#[from_auth(self.db.validate_user)]` has an arbitrary Rust expression as the resolver. Capturing it as a string for `x-plexus-source.resolver` requires `syn`'s `ToTokens` or `Span::source_text`; the former is canonical. Test that complex expressions (generics, method chains) serialize usefully.

## Acceptance criteria

1. A method with `#[from_auth(self.db.validate_user)]` produces a schema where the auth param appears in `properties` with `x-plexus-source.from == "auth"` and `x-plexus-source.resolver` equal to the string `"self.db.validate_user"`.
2. A method with `#[activation_param] origin: ValidOrigin` (where the activation has `request = FormVeritasRequest` and `FormVeritasRequest` declares `origin: ValidOrigin`) produces a schema where the `origin` param has `x-plexus-source.from == "derived"`.
3. A method with `#[activation_param] auth_token: String` (where the request struct has `#[from_cookie("access_token")] auth_token: String`) produces a schema where `auth_token` has `x-plexus-source.from == "cookie"` and `key == "access_token"`.
4. A method with `#[plexus::method(request = ())]` override, in an activation with `required = []` or no `required` list, produces a schema with only its plain RPC params.
5. A method with `#[plexus::method(request = ())]` override, in an activation with `required = [origin]`, FAILS to compile with a clear error naming `origin`.
6. An activation-injected field (the method does NOT declare it in its signature, but the activation has `request = X`) appears in the method's schema with `x-plexus-source` from X's field annotation.
7. The `required` array in any method's params schema never contains a parameter whose `x-plexus-source.from` is anything other than `"rpc"` (or absent).
8. A method with no activation-level request (activation lacks `request = ...`) produces a schema identical to today's output, modulo an optional `x-plexus-source: "rpc"` marker on each plain param (implementor chooses emit-or-omit by default).
9. Existing plexus-macros tests continue to pass without modification.
10. A new test asserts that for at least one fixture method, all observable behaviors above hold simultaneously. The fixture lives in `plexus-macros/tests/` and is committed.

## Verification against uscis (FormVeritasV2)

After REQ-6 lands, rebuild uscis and regenerate the IR. Expected changes:

- Each method in activations declaring `request = FormVeritasRequest` gets `origin`, `transport`, `client_ip` params in its `MethodSchema.params`, each annotated `x-plexus-source: { from: "derived" }`.
- `health.check` (with `request = ()` and no `required` on the activation) keeps only its RPC params.
- Methods using `#[from_auth(self.db.validate_user)]` carry `x-plexus-source: { from: "auth", resolver: "self.db.validate_user" }` on the auth param.

This is mechanical verification, captured as a follow-up check (not an acceptance criterion) because uscis is an external consumer.

## Coordination

- `unlocks: [REQ-8, REQ-9, SAFE-6]`
- REQ-5's activation-level renderer work can remain in place (no conflict) or be augmented by REQ-8 with per-method detail
- REQ-7's tonight-landed activation-level JSDoc emission is a stepping stone; REQ-9 replaces it with per-method emission using REQ-6's annotations

## Completion

Implementor lands:
- `plexus-macros/src/codegen/method_enum.rs` edits emitting per-param `x-plexus-source`
- `plexus-macros/src/parse.rs` (or wherever activation attrs parse) gaining `required = [...]` support
- Override validation logic with clear compile errors
- Tests covering the 10 acceptance criteria
- Manual verification against uscis backend (rebuild + inspect ir.json for expected annotations)

Flips status to Complete in the same commit that lands the above.
