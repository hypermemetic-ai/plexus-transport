# HTTP Method Metadata: Macro Implementation Explained

## What We Added

We added the ability to annotate methods with HTTP method metadata:

```rust
#[hub_method(http_method = "GET")]
async fn get_user(&self, user_id: String) -> impl Stream<Item = UserEvent> {
    // ...
}
```

This metadata flows through the macro system and ends up in the `MethodSchema`, which transport bridges use to route requests correctly.

---

## The Complete Flow

### Step 1: User Writes Code

```rust
#[hub_methods(namespace = "users", description = "User API")]
impl UserActivation {
    /// Get user by ID
    #[hub_method(http_method = "GET")]
    async fn get_user(&self, user_id: String) -> impl Stream<Item = UserEvent> {
        // implementation
    }

    /// Create new user
    #[hub_method(http_method = "POST")]
    async fn create_user(&self, name: String) -> impl Stream<Item = UserEvent> {
        // implementation
    }
}
```

---

### Step 2: Macro Parsing

#### What We Added to `parse.rs`

**2.1: Parse the `http_method` attribute**

```rust
// plexus-macros/src/parse.rs:22-38
pub struct HubMethodAttrs {
    pub name: Option<String>,
    pub param_docs: HashMap<String, String>,
    pub returns_variants: Vec<String>,
    pub streaming: bool,
    pub bidirectional: BidirType,
    pub http_method: Option<String>,  // ← NEW: Store as string during parsing
}
```

**2.2: Parse and validate the string**

```rust
// plexus-macros/src/parse.rs:70-95 (in Parse impl)
Meta::NameValue(MetaNameValue { path, value, .. }) => {
    if path.is_ident("name") {
        // ... existing name parsing
    } else if path.is_ident("http_method") {  // ← NEW
        if let Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) = value {
            let method = s.value().to_uppercase();  // "get" → "GET"

            // Validate at parse time
            match method.as_str() {
                "GET" | "POST" | "PUT" | "DELETE" | "PATCH" => {
                    http_method = Some(method);  // ✅ Valid
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        s,
                        format!(
                            "Invalid HTTP method '{}'. Valid: GET, POST, PUT, DELETE, PATCH",
                            method
                        ),
                    ));  // ❌ Compile error
                }
            }
        }
    }
}
```

**Result:** `HubMethodAttrs { http_method: Some("GET"), ... }`

---

**2.3: Store in `MethodInfo`**

```rust
// plexus-macros/src/parse.rs:252-269
pub struct MethodInfo {
    pub fn_name: syn::Ident,          // get_user
    pub method_name: String,          // "get_user"
    pub description: String,          // "Get user by ID"
    pub params: Vec<ParamInfo>,       // [ParamInfo { name: "user_id", ty: String }]
    pub streaming: bool,              // false
    pub bidirectional: BidirType,     // None
    pub http_method: Option<String>,  // ← NEW: Some("GET")
}

// Extract from attributes
let http_method = hub_method_attrs
    .and_then(|a| a.http_method.clone());  // ← Gets Some("GET")
```

**Result:** `MethodInfo` for each method with `http_method` field populated

---

### Step 3: Code Generation

#### What We Added to `method_enum.rs`

**3.1: Convert string to enum at compile time**

```rust
// plexus-macros/src/codegen/method_enum.rs:99-111
// Generate HTTP method enum values for each method
let http_methods: Vec<TokenStream> = methods
    .iter()
    .map(|m| {
        match m.http_method.as_deref() {
            Some("GET") => quote! { plexus_core::plexus::schema::HttpMethod::Get },
            Some("POST") => quote! { plexus_core::plexus::schema::HttpMethod::Post },
            Some("PUT") => quote! { plexus_core::plexus::schema::HttpMethod::Put },
            Some("DELETE") => quote! { plexus_core::plexus::schema::HttpMethod::Delete },
            Some("PATCH") => quote! { plexus_core::plexus::schema::HttpMethod::Patch },
            None => quote! { plexus_core::plexus::schema::HttpMethod::Post },  // Default
            _ => quote! { plexus_core::plexus::schema::HttpMethod::Post },     // Fallback
        }
    })
    .collect();
```

**This generates:** `vec![HttpMethod::Get, HttpMethod::Post]` (enum values, not strings!)

---

**3.2: Generate the method_schemas() function**

```rust
// plexus-macros/src/codegen/method_enum.rs:196-310
fn compute_method_schemas() -> Vec<MethodSchema> {
    let method_names: &[&str] = &["get_user", "create_user"];
    let descriptions: &[&str] = &["Get user by ID", "Create new user"];
    let hashes: &[&str] = &["hash1", "hash2"];
    let streaming: &[bool] = &[false, false];

    // ← NEW: Vector of HttpMethod enum values
    let http_methods: Vec<HttpMethod> = vec![
        plexus_core::plexus::schema::HttpMethod::Get,
        plexus_core::plexus::schema::HttpMethod::Post,
    ];

    let return_schemas: Vec<...> = vec![...];

    let mut methods: Vec<_> = method_names
        .iter()
        .zip(descriptions.iter())
        .zip(hashes.iter())
        .zip(streaming.iter())
        .zip(http_methods.into_iter())  // ← NEW: Consume enum values
        .zip(return_schemas.into_iter())
        .enumerate()
        .map(|(i, (((((name, desc), hash), is_streaming), http_method), (returns_opt, variant_filter)))| {
            let mut schema = MethodSchema::new(
                name.to_string(),
                desc.to_string(),
                hash.to_string(),
            );

            if let Some(p) = params {
                schema = schema.with_params(p);
            }
            if let Some(r) = filtered_returns {
                schema = schema.with_returns(r);
            }

            schema = schema.with_streaming(*is_streaming);
            schema = schema.with_http_method(http_method);  // ← NEW: HttpMethod enum

            // Apply bidirectional config...

            schema
        })
        .collect();

    // Add auto-generated "schema" method
    methods.push(MethodSchema::new("schema", ...));

    methods
}
```

---

**3.3: Generated Code Output**

The macro generates code that looks like this:

```rust
// Generated by macro
impl UserActivation {
    fn compute_method_schemas() -> Vec<MethodSchema> {
        // ... setup code ...

        let http_methods: Vec<HttpMethod> = vec![
            plexus_core::plexus::schema::HttpMethod::Get,   // for get_user
            plexus_core::plexus::schema::HttpMethod::Post,  // for create_user
        ];

        let mut methods: Vec<_> = /* ... zip chains ... */
            .map(|(i, (((((name, desc), hash), is_streaming), http_method), ...))| {
                let mut schema = MethodSchema::new(*name, *desc, *hash);

                // ... setup params, returns, streaming ...

                schema = schema.with_http_method(http_method);  // Takes HttpMethod enum!

                schema
            })
            .collect();

        methods
    }
}
```

---

### Step 4: What Gets Generated

#### The `PluginSchema` at Runtime

When you call `activation.plugin_schema()`, you get:

```rust
PluginSchema {
    namespace: "users",
    version: "1.0.0",
    description: "User API",
    methods: vec![
        MethodSchema {
            name: "get_user",
            description: "Get user by ID",
            hash: "abc123",
            params: Some(/* schema */),
            returns: Some(/* schema */),
            streaming: false,
            bidirectional: false,
            http_method: HttpMethod::Get,  // ← Strongly-typed enum!
            request_type: None,
            response_type: None,
        },
        MethodSchema {
            name: "create_user",
            description: "Create new user",
            hash: "def456",
            params: Some(/* schema */),
            returns: Some(/* schema */),
            streaming: false,
            bidirectional: false,
            http_method: HttpMethod::Post,  // ← Enum, not string!
            request_type: None,
            response_type: None,
        },
        MethodSchema {
            name: "schema",
            description: "Get plugin or method schema...",
            hash: "auto_schema",
            http_method: HttpMethod::Post,  // ← Default for protocol methods
            // ...
        },
    ],
    children: None,
}
```

---

## How Transport Bridges Use It

### REST HTTP Bridge

```rust
// plexus-transport/src/http/bridge.rs:55-73
impl MethodRegistry {
    pub fn from_schemas(schemas: Vec<PluginSchema>) -> Self {
        let mut methods = HashMap::new();

        for schema in schemas {
            for method in schema.methods {
                let key = format!("{}.{}", schema.namespace, method.name);

                methods.insert(key, RestMethodInfo {
                    namespace: schema.namespace.clone(),
                    method: method.name.clone(),
                    streaming: method.streaming,
                    http_method: method.http_method,  // ← Copy enum value
                });
            }
        }

        Self { methods: Arc::new(methods) }
    }
}
```

```rust
// plexus-transport/src/http/bridge.rs:88-129
fn schemas_to_rest_routes<A>(schemas: Vec<PluginSchema>) -> Router {
    let registry = MethodRegistry::from_schemas(schemas);
    let mut router = Router::new();

    for method_info in registry.all_methods() {
        let path = format!("/{}/{}",
            method_info.namespace,   // "users"
            method_info.method        // "get_user"
        );

        // Pattern match on strongly-typed enum!
        let method_router = match method_info.http_method {
            HttpMethod::Get => get(rest_method_handler),      // ← Axum GET route
            HttpMethod::Post => post(rest_method_handler),    // ← Axum POST route
            HttpMethod::Put => put(rest_method_handler),      // ← Axum PUT route
            HttpMethod::Delete => delete(rest_method_handler),// ← Axum DELETE route
            HttpMethod::Patch => patch(rest_method_handler),  // ← Axum PATCH route
        };

        router = router.route(&path, method_router);
        // Registers: GET /users/get_user → rest_method_handler
    }

    router.with_state(state)
}
```

---

## Type Safety Throughout

### 1. Parse Time (Compile Error)

```rust
#[hub_method(http_method = "INVALID")]  // ❌ Compile error
async fn bad_method(&self) { }

// Error: Invalid HTTP method 'INVALID'. Valid methods: GET, POST, PUT, DELETE, PATCH
```

### 2. Code Gen Time (String → Enum)

```rust
// String during parsing
http_method: Some("GET")

// ↓ Converted to enum during code generation

quote! { plexus_core::plexus::schema::HttpMethod::Get }
```

### 3. Runtime (Exhaustive Matching)

```rust
match method_info.http_method {
    HttpMethod::Get => { /* ... */ },
    HttpMethod::Post => { /* ... */ },
    HttpMethod::Put => { /* ... */ },
    HttpMethod::Delete => { /* ... */ },
    HttpMethod::Patch => { /* ... */ },
    // ✅ Compiler ensures all variants handled
}
```

---

## The Key Innovation

### What We Added to plexus-core

```rust
// plexus-core/src/plexus/schema.rs:17-60
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl Default for HttpMethod {
    fn default() -> Self {
        HttpMethod::Post  // Backward compatibility
    }
}

impl HttpMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PUT" => Some(HttpMethod::Put),
            "DELETE" => Some(HttpMethod::Delete),
            "PATCH" => Some(HttpMethod::Patch),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
        }
    }
}
```

**Updated MethodSchema:**

```rust
pub struct MethodSchema {
    pub name: String,
    pub description: String,
    pub hash: String,
    pub params: Option<Schema>,
    pub returns: Option<Schema>,
    pub streaming: bool,
    pub bidirectional: bool,
    pub http_method: HttpMethod,  // ← NEW: Strongly-typed enum
    pub request_type: Option<Schema>,
    pub response_type: Option<Schema>,
}
```

**Builder method:**

```rust
impl MethodSchema {
    pub fn with_http_method(mut self, http_method: HttpMethod) -> Self {
        self.http_method = http_method;
        self
    }
}
```

---

## Visual Flow Diagram

```
User Code:
  #[hub_method(http_method = "GET")]
  async fn get_user(...) { }
            ↓
Parse (parse.rs):
  HubMethodAttrs { http_method: Some("GET") }  // String
            ↓
Extract (parse.rs):
  MethodInfo { http_method: Some("GET") }      // String
            ↓
Code Generation (method_enum.rs):
  quote! { HttpMethod::Get }                   // TokenStream → Enum
            ↓
Compiled Code:
  let http_methods = vec![HttpMethod::Get];    // Enum at runtime
            ↓
Schema Construction:
  schema.with_http_method(HttpMethod::Get)     // Enum parameter
            ↓
PluginSchema:
  MethodSchema { http_method: HttpMethod::Get } // Enum in struct
            ↓
Transport Bridge:
  match method_info.http_method {
      HttpMethod::Get => get(handler),          // Exhaustive match
      // ...
  }
            ↓
Axum Router:
  router.route("/users/get_user", get(handler)) // GET route registered
            ↓
HTTP Request:
  GET /users/get_user                           // Only accepts GET!
```

---

## Complete Example: From Source to HTTP

### Source Code

```rust
#[hub_methods(namespace = "users")]
impl UserActivation {
    #[hub_method(http_method = "GET")]
    async fn get_user(&self, user_id: String) -> impl Stream<Item = UserEvent> {
        // ...
    }
}
```

### Macro Expansion (conceptual)

```rust
impl UserActivation {
    fn compute_method_schemas() -> Vec<MethodSchema> {
        let http_methods = vec![HttpMethod::Get];

        vec![
            MethodSchema::new("get_user", "...", "...")
                .with_http_method(HttpMethod::Get)
        ]
    }
}

impl Activation for UserActivation {
    fn plugin_schema(&self) -> PluginSchema {
        PluginSchema {
            methods: Self::compute_method_schemas(),
            // ...
        }
    }
}
```

### Runtime Usage

```rust
// HTTP gateway startup
let schemas = vec![activation.plugin_schema()];

// schemas[0].methods[0] = MethodSchema {
//     name: "get_user",
//     http_method: HttpMethod::Get,  // ← Enum value
// }

let registry = MethodRegistry::from_schemas(schemas);

// registry.methods["users.get_user"] = RestMethodInfo {
//     http_method: HttpMethod::Get,  // ← Copied
// }

let router = schemas_to_rest_routes(...);

// router now has:
//   GET /users/get_user → rest_method_handler
```

### HTTP Request

```bash
# ✅ Works
curl -X GET http://localhost:8888/rest/users/get_user

# ❌ 405 Method Not Allowed
curl -X POST http://localhost:8888/rest/users/get_user
```

---

## Summary

**What we added:**

1. **Parse** `http_method = "GET"` attribute (string validation)
2. **Store** in `HubMethodAttrs` and `MethodInfo` (still string)
3. **Convert** string to `HttpMethod` enum during code generation
4. **Embed** enum in generated `MethodSchema`
5. **Use** enum for Axum route registration (exhaustive matching)

**Result:** Fully type-safe HTTP method routing from source code to runtime!
