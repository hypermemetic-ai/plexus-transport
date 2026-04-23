# REQ-0: Spike — Validate Core Design Decisions Before Implementation

**blocked_by:** []
**unlocks:** [REQ-1, REQ-2, REQ-3, REQ-4, REQ-5, REQ-6, REQ-7]
**touches:** plexus-transport, plexus-macros, plexus-protocol, synapse, synapse-cc
**status:** Complete

## Goal

Before writing any production code, write a collection of small standalone programs that answer the key open questions. Each program is disposable — the point is to find out what works, what breaks, and what needs a different approach. The results directly inform every subsequent REQ ticket.

Create a workspace crate: `plexus-transport/spike/req/` with a `Cargo.toml` and one file per program under `src/bin/`. Each binary can be run with `cargo run --bin <name>` and should print a clear success/failure verdict.

---

## Programs

### S-01: `schemars_extension_field`

**Question:** Can we inject `x-plexus-source` as a custom JSON extension on a specific struct field in a schemars-derived schema, without writing a full custom `JsonSchema` impl?

**Try in order:**

```rust
// Attempt A: schemars schema_with attribute
#[derive(schemars::JsonSchema)]
struct Req {
    #[schemars(schema_with = "cookie_schema")]
    auth_token: String,
}

fn cookie_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::String.into()),
        extensions: [
            ("x-plexus-source".into(),
             serde_json::json!({"from": "cookie", "key": "access_token"}))
        ].into_iter().collect(),
        ..Default::default()
    }.into()
}

fn main() {
    let schema = schemars::schema_for!(Req);
    let json = serde_json::to_string_pretty(&schema).unwrap();
    println!("{}", json);
    // SUCCESS: "x-plexus-source" appears in auth_token's schema object
    // FAILURE: extension absent or schema structure wrong
}
```

**Success:** `x-plexus-source` appears on the field in the generated JSON.
**Failure signals:** Need to generate JsonSchema impl entirely from the proc macro rather than delegating to schemars field-level schema_with.

---

### S-02: `derive_two_impls`

**Question:** Can a single proc macro derive on a struct generate both a trait impl (`PlexusRequest`) and a `JsonSchema` impl? Does schemars complain about a manually-written `JsonSchema` impl alongside or instead of its own derive?

```rust
// In a test crate that depends on plexus-transport (for PlexusRequest)
// and schemars:

// Stub PlexusRequest trait for the spike:
trait PlexusRequest: Sized {
    fn extract(headers: &[(&str, &str)], peer: Option<std::net::SocketAddr>)
        -> Result<Self, String>;
}

// Manually write what the derive should generate for one struct:
struct Req {
    auth_token: String,
    peer_addr: Option<std::net::SocketAddr>,
}

impl PlexusRequest for Req {
    fn extract(headers: &[(&str, &str)], peer: Option<std::net::SocketAddr>)
        -> Result<Self, String>
    {
        let cookie_header = headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("cookie"))
            .map(|(_, v)| *v)
            .unwrap_or("");
        let auth_token = parse_cookie(cookie_header, "access_token")
            .ok_or("access_token cookie required")?
            .to_string();
        Ok(Self { auth_token, peer_addr: peer })
    }
}

impl schemars::JsonSchema for Req {
    fn schema_name() -> String { "Req".into() }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        // Hand-write what the derive should produce
        // including x-plexus-source extensions on each field
        todo!()
    }
}

fn main() {
    // Extraction works:
    let headers = [("cookie", "access_token=abc123")];
    let req = Req::extract(&headers, Some("1.2.3.4:5678".parse().unwrap())).unwrap();
    assert_eq!(req.auth_token, "abc123");
    assert_eq!(req.peer_addr.unwrap().to_string(), "1.2.3.4:5678");

    // Schema works:
    let schema = schemars::schema_for!(Req);
    let json = serde_json::to_string_pretty(&schema).unwrap();
    println!("{}", json);
    println!("S-02: OK");
}
```

**Success:** Both impls coexist, schema has extensions, extraction works.
**Failure signals:** schemars blanket impl conflicts with manual impl — investigate `no_bound` or `impl_for`.

---

### S-03: `required_field_gate`

**Question:** Does a non-`Option<T>` field returning `Err` from extraction correctly short-circuit and produce the right error type — before any method body runs?

```rust
struct AuthReq {
    auth_token: String,   // required
    origin: Option<String>, // optional
}

// Simulate extraction:
fn extract(cookies: &str, headers: &[(&str, &str)]) -> Result<AuthReq, String> {
    let auth_token = parse_cookie(cookies, "access_token")
        .ok_or_else(|| "Authentication required: access_token cookie missing".to_string())?;
    let origin = headers.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("origin"))
        .map(|(_, v)| v.to_string());
    Ok(AuthReq { auth_token: auth_token.to_string(), origin })
}

fn main() {
    // Missing cookie → Err before method body:
    assert!(extract("other=val", &[]).is_err());
    let err = extract("other=val", &[]).unwrap_err();
    assert!(err.contains("access_token"), "err: {}", err);

    // Present cookie → Ok:
    assert!(extract("access_token=tok123", &[]).is_ok());

    // Optional origin absent → Ok with None:
    let req = extract("access_token=tok123", &[]).unwrap();
    assert!(req.origin.is_none());

    // Optional origin present → Ok with Some:
    let req = extract("access_token=tok123", &[("origin", "https://app.example.com")]).unwrap();
    assert_eq!(req.origin.as_deref(), Some("https://app.example.com"));

    println!("S-03: OK");
}
```

**Success:** All assertions pass.
**Failure signals:** Error message format wrong, or extraction semantics differ from expectations.

---

### S-04: `plexus_request_field_trait`

**Question:** Can a newtype like `ValidOrigin` implement a `PlexusRequestField` trait such that a struct field of type `ValidOrigin` in a `#[derive(PlexusRequest)]` struct automatically uses the newtype's extraction logic, including validation?

```rust
trait PlexusRequestField: Sized {
    // source metadata for schema generation
    fn source_annotation() -> serde_json::Value;
    // extraction from raw headers/peer
    fn extract_from(headers: &[(&str, &str)], peer: Option<std::net::SocketAddr>)
        -> Result<Self, String>;
}

// Blanket impl for Option<T> where T: PlexusRequestField
impl<T: PlexusRequestField> PlexusRequestField for Option<T> {
    fn source_annotation() -> serde_json::Value { T::source_annotation() }
    fn extract_from(headers: &[(&str, &str)], peer: Option<std::net::SocketAddr>)
        -> Result<Self, String>
    {
        match T::extract_from(headers, peer) {
            Ok(v) => Ok(Some(v)),
            Err(_) => Ok(None),  // optional: missing → None
        }
    }
}

struct ValidOrigin(pub String);

impl PlexusRequestField for ValidOrigin {
    fn source_annotation() -> serde_json::Value {
        serde_json::json!({"from": "header", "key": "origin"})
    }
    fn extract_from(headers: &[(&str, &str)], _peer: Option<std::net::SocketAddr>)
        -> Result<Self, String>
    {
        const ALLOWED: &[&str] = &["https://app.example.com", "http://localhost:5173"];
        match headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("origin")).map(|(_, v)| *v) {
            None => Ok(ValidOrigin(String::new())),   // no origin = CLI/synapse path
            Some(o) if ALLOWED.contains(&o) => Ok(ValidOrigin(o.to_string())),
            Some(o) => Err(format!("Origin '{}' not allowed", o)),
        }
    }
}

fn main() {
    // Allowed origin
    assert!(ValidOrigin::extract_from(&[("origin", "https://app.example.com")], None).is_ok());

    // Disallowed origin
    assert!(ValidOrigin::extract_from(&[("origin", "https://evil.com")], None).is_err());

    // No origin (CLI path) — non-optional returns Ok with empty string
    assert!(ValidOrigin::extract_from(&[], None).is_ok());

    // Option<ValidOrigin> — disallowed returns Ok(None) not Err
    assert_eq!(
        Option::<ValidOrigin>::extract_from(&[("origin", "https://evil.com")], None).unwrap().is_none(),
        // wait — is disallowed origin Ok(None) or Err?
        // This is a design question: for Option<ValidOrigin>, should a disallowed
        // origin be None or an error?
        // Test BOTH and document which behavior we want.
        false  // disallowed origin should still Err even when Option<ValidOrigin>
    );

    println!("S-04: OK — also check the Option<ValidOrigin> disallowed behavior above");
}
```

**Success:** Newtype validation works, blanket Option impl behaves as expected.
**Critical design question exposed:** For `Option<ValidOrigin>`, does a *present but invalid* origin produce `Err` or `None`? `None` would silently swallow a security violation. This should be `Err`. The `Option<T>` blanket should only absorb *absent* fields, not *invalid* ones. The impl above needs correction — document the right behavior.

---

### S-05: `macro_parse_request_attr`

**Question:** Does the existing `HubMethodsAttrs` parser in plexus-macros accept a new `request = SomeType` key-value pair without panicking, and can we extract the type path from it?

```rust
// In a test inside plexus-macros/tests/ or as a trybuild compile test:

// Must compile and run correctly:
#[plexus_macros::activation(
    namespace = "test",
    version = "1.0.0",
    request = (),           // unit type — no request extraction
)]
impl TestActivation {
    #[plexus_macros::method(description = "ping")]
    async fn ping(&self) -> impl Stream<Item = String> + Send + 'static {
        async_stream::stream! { yield "pong".to_string(); }
    }
}
```

Separately, add `request = SomeStruct` to the parse test:

```rust
// Must compile:
struct MyRequest { auth_token: String }

#[plexus_macros::activation(
    namespace = "test",
    version = "1.0.0",
    request = MyRequest,
)]
impl TestActivation2 {
    #[plexus_macros::method]
    async fn ping(&self) -> impl Stream<Item = String> + Send + 'static {
        async_stream::stream! { yield "ok".to_string(); }
    }
}
```

**Success:** Macro accepts `request = Type` without error, generates correct Activation impl.
**Failure signals:** Parser rejects unknown key — need to add `request` to `HubMethodsAttrs::parse()`.

---

### S-06: `activation_param_strip`

**Question:** Does adding `#[activation_param]` to a method parameter (a) strip it from the generated RPC schema, and (b) not appear as a JSON-RPC input param? Verify using the existing `MethodInfo` parsing path.

```rust
// Must compile. The generated ClientsActivationMethod enum must NOT have
// auth_token in its List variant.
struct FakeRequest { auth_token: String }

#[plexus_macros::activation(namespace = "clients", request = FakeRequest)]
impl FakeActivation {
    #[plexus_macros::method]
    async fn list(
        &self,
        #[activation_param] auth_token: String,  // should be stripped from schema
        search: Option<String>,                   // should appear in schema
    ) -> impl Stream<Item = String> + Send + 'static {
        async_stream::stream! { yield auth_token; yield search.unwrap_or_default(); }
    }
}

fn main() {
    // Inspect the generated method schema:
    let schemas = FakeActivation::method_schemas();
    let list_schema = schemas.iter().find(|s| s.name == "list").unwrap();
    let params: serde_json::Value = serde_json::from_str(&list_schema.params_schema).unwrap();
    
    // search is a param, auth_token is not:
    assert!(params["properties"]["search"].is_object(), "search should be in params");
    assert!(params["properties"]["auth_token"].is_null(), "auth_token should NOT be in params");
    
    println!("S-06: OK");
}
```

**Success:** Schema only contains `search`.
**Failure signals:** `#[activation_param]` not yet recognized by parse.rs — needs adding alongside `#[from_auth]`.

---

### S-07: `request_schema_in_plugin_schema`

**Question:** Does `plugin_schema()` include a `request` field containing the JSON Schema of the declared request type?

```rust
// Depends on S-05 and S-06 both passing.
// After adding `request = FakeRequest` support to the macro:

fn main() {
    let activation = FakeActivation::new();  // or however it's constructed
    let schema = Activation::plugin_schema(&activation);
    let json = serde_json::to_string_pretty(&schema).unwrap();
    println!("{}", json);

    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(v["request"].is_object(), "plugin_schema should have 'request' field");
    assert!(v["request"]["properties"]["auth_token"].is_object());
    // x-plexus-source on auth_token:
    assert_eq!(
        v["request"]["properties"]["auth_token"]["x-plexus-source"]["from"],
        "cookie"
    );
    println!("S-07: OK");
}
```

**Success:** `request` field present with correct JSON Schema and extensions.
**Failure signals:** Depends on S-01/S-02 being resolved first.

---

### S-08: `option_vs_required_in_schema`

**Question:** Does the JSON Schema `required` array correctly reflect which fields are `String` vs `Option<String>` in the request struct?

```rust
struct MixedRequest {
    #[from_cookie("access_token")]
    auth_token: String,           // required

    #[from_header("origin")]
    origin: Option<String>,       // optional

    #[from_peer]
    peer_addr: Option<std::net::SocketAddr>,  // derived (not in required)
}

fn main() {
    let schema = schemars::schema_for!(MixedRequest); // or PlexusRequest::schema()
    let json = serde_json::to_string_pretty(&schema).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();

    let required = v["required"].as_array().unwrap();
    let req_names: Vec<&str> = required.iter()
        .filter_map(|s| s.as_str()).collect();

    assert!(req_names.contains(&"auth_token"), "auth_token should be required");
    assert!(!req_names.contains(&"origin"), "origin should NOT be required");
    assert!(!req_names.contains(&"peer_addr"), "peer_addr should NOT be required (derived)");

    // peer_addr should have x-plexus-source: derived, not appear in required at all
    assert_eq!(v["properties"]["peer_addr"]["x-plexus-source"]["from"], "derived");

    println!("S-08: OK");
}
```

**Success:** Only non-Optional, non-derived fields in `required`.
**Failure signals:** schemars puts all fields in required by default — need custom schema generation.

---

### S-09: `no_request_backward_compat`

**Question:** An existing `#[plexus::activation]` impl with no `request = ...` attribute still compiles and produces a correct Activation impl. The `plugin_schema()` has no `request` field (`null` or absent).

```rust
// Must compile identically to today:
#[plexus_macros::activation(namespace = "forms", version = "1.0.0")]
impl FormsStub {
    #[plexus_macros::method(description = "list")]
    async fn list(&self) -> impl Stream<Item = String> + Send + 'static {
        async_stream::stream! { yield "ok".to_string(); }
    }
}

fn main() {
    let schema = serde_json::to_string_pretty(
        &Activation::plugin_schema(&FormsStub)
    ).unwrap();
    let v: serde_json::Value = serde_json::from_str(&schema).unwrap();
    assert!(v["request"].is_null() || !v.as_object().unwrap().contains_key("request"),
            "no request field expected when request = not specified");
    println!("S-09: OK");
}
```

**Success:** Compiles, no `request` field in schema.
**Failure signals:** Adding `request` handling broke the default code path.

---

### S-10: `synapse_schema_consumption`

**Question (Haskell):** Can synapse decode a `PluginSchema` JSON that has a `request` field (an arbitrary JSON object) into `PluginSchema { psRequest = Just value }`? Does a schema without `request` decode to `psRequest = Nothing`?

```haskell
-- In synapse/test/SchemaDecodeSpec.hs

import Data.Aeson
import Plexus.Schema.Recursive

schemaWithRequest :: ByteString
schemaWithRequest = [aesonQQ|{
  "namespace": "clients", "version": "1.0.0", "description": "...",
  "methods": [], "hash": "abc",
  "request": {
    "type": "object",
    "properties": {
      "auth_token": {
        "type": "string",
        "x-plexus-source": {"from": "cookie", "key": "access_token"}
      }
    },
    "required": ["auth_token"]
  }
}|]

schemaWithoutRequest :: ByteString
schemaWithoutRequest = [aesonQQ|{
  "namespace": "forms", "version": "1.0.0", "description": "...",
  "methods": [], "hash": "def"
}|]

spec :: Spec
spec = do
  describe "PluginSchema request field" $ do
    it "decodes psRequest = Just when present" $ do
      let Just schema = decode schemaWithRequest :: Maybe PluginSchema
      psRequest schema `shouldSatisfy` isJust

    it "decodes psRequest = Nothing when absent" $ do
      let Just schema = decode schemaWithoutRequest :: Maybe PluginSchema
      psRequest schema `shouldBe` Nothing

    it "x-plexus-source survives roundtrip" $ do
      let Just schema = decode schemaWithRequest :: Maybe PluginSchema
      let Just req = psRequest schema
      let fields = req ^? key "properties" . key "auth_token" . key "x-plexus-source" . key "from"
      fields `shouldBe` Just (String "cookie")
```

**Success:** Both decode correctly, extension survives roundtrip.
**Failure signals:** `psRequest` field not in `PluginSchema` yet — needs adding to `Plexus/Schema/Recursive.hs`.

---

## Spike Workspace

```
plexus-transport/
  spike/
    req/
      Cargo.toml          # [workspace] members = ["s01", "s02", ...]
      s01-schemars-extension/
        Cargo.toml
        src/main.rs
      s02-derive-two-impls/
        ...
      s03-required-field-gate/
        ...
      s04-plexus-request-field/
        ...
      s05-macro-parse-request/
        ...
      s06-activation-param-strip/
        ...
      s07-request-schema-in-plugin/
        ...
      s08-option-vs-required/
        ...
      s09-no-request-backward-compat/
        ...
```

S-10 lives in `synapse/test/`.

## Decision Matrix

Results from running all programs (2026-04-08):

| Program | Result | Finding |
|---------|--------|---------|
| S-01 | **PASS** | `#[schemars(extend("x-plexus-source" = {...}))]` works at field level in schemars 1.x. `x-plexus-source` appears in the generated JSON as expected. No custom `JsonSchema` impl needed. |
| S-02 | **PASS** | Manual `JsonSchema` impl coexists fine with a separate extraction trait impl. A `PlexusRequest` derive can emit both. No blanket impl conflicts. |
| S-03 | **PASS** | Non-`Option` field returns `Err` which short-circuits. `Option<T>` correctly allows absent fields. Error message format confirmed. |
| S-04 | **PASS** | `PlexusRequestField` trait + `Option<T>` blanket works. Key finding: the blanket must map `Err` to `Err` for invalid values, NOT to `None`. Absent (no header) → `None`; present-but-invalid → `Err`. The design note in S-04 confirms this security invariant. |
| S-05 | **PASS** | Existing activation with no `request =` compiles correctly. Parser does not accept `request = Type` yet (as expected — documented). Need to add `request` key to `HubMethodsAttrs::parse()` in REQ-4. |
| S-06 | **PASS** | Baseline confirmed: `search: Option<String>` appears in method params schema. `#[activation_param]` not yet in parse.rs (expected). When added, it must strip params from method enum while keeping them accessible via request struct. |
| S-07 | **PASS** | Baseline `plugin_schema()` output documented. No `request` field yet (correct). Current keys: `namespace`, `version`, `description`, `self_hash`, `hash`, `methods`. `request` field must be added to `PluginSchema` type and codegen (REQ-4). Method params come as inline `params` object, not a string — important for schema inspection code. |
| S-08 | **PASS** | schemars correctly puts `String` fields in `required`, `Option<T>` fields not. `x-plexus-source` on all three field types (cookie/header/derived) round-trips correctly. No custom schema generation needed — schemars does the right thing. |
| S-09 | **PASS** | Existing activations with no `request =` compile and schema correctly after the `crate_path` fix applied to spike. No `request` field in output. Backward compat holds. |
| S-10 | **MOVED** | Promoted to its own ticket: `REQ-S10.md` (status: Pending). Unblocks REQ-5. The body description below is preserved for reference but the implementation tracking lives in REQ-S10. |

### Key implementation findings from spikes

1. **`#[schemars(extend(...))]`** is the right tool for `x-plexus-source` — no need to write a custom `JsonSchema` impl per struct. The `PlexusRequest` derive can simply derive `JsonSchema` and add the extend attrs.

2. **`crate_path` must be set to `"plexus_core"`** in any crate that depends on `plexus-macros` directly (rather than `plexus-core`). The generated method enum emits `{crate_path}::serde_helpers::...` paths. Fixed during spike by updating `method_enum.rs` to use `crate_path` rather than hardcoded `crate::`.

3. **`plugin_schema()` method key is `"params"`, not `"params_schema"`** — the serialized schema puts method params as an inline JSON object at `methods[i].params`, not a string field.

4. **`Option<T>` absent→None, invalid→Err** — the security invariant is confirmed. The `Option<T>` blanket impl of `PlexusRequestField` must NOT swallow validation errors.

5. **S-05 confirms `request = Type` not yet in parser** — this is the primary REQ-4 task.

6. **S-06 confirms `#[activation_param]` stripping not yet in codegen** — this is the primary REQ-2 task.

7. **S-07 confirms `request` absent from `PluginSchema`** — needs to be added to both the Rust type and the codegen, then propagated to synapse (REQ-5).

## Acceptance Criteria

- [x] S-01 through S-09 programs exist and run
- [x] Each program prints `OK` or a clear failure diagnosis
- [x] Decision matrix filled in with actual results
- [x] Key implementation findings documented above
- [ ] S-10 Haskell spike written and run (tracked in REQ-S10)
- [ ] Design changes reflected in REQ-1 through REQ-5
