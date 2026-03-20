# HTTP Gateway Architecture

**Date:** 2026-03-20
**Status:** Implemented
**Related:** `http-gateway` feature, REST HTTP transport, HTTP method metadata

## Table of Contents

- [Overview](#overview)
- [Design Goals](#design-goals)
- [Architecture](#architecture)
- [HTTP Method Routing](#http-method-routing)
- [Streaming Protection](#streaming-protection)
- [Implementation Details](#implementation-details)
- [Usage Guide](#usage-guide)
- [Testing](#testing)

---

## Overview

The HTTP gateway provides a RESTful HTTP API transport for Plexus activations, exposing each method as a REST endpoint with proper HTTP verb routing.

**Key Features:**
- ✅ Strongly-typed HTTP method routing (GET, POST, PUT, DELETE, PATCH)
- ✅ JSON responses for non-streaming methods
- ✅ Server-Sent Events (SSE) for streaming methods
- ✅ Timeout protection (5 minute max execution time)
- ✅ Memory protection (buffer limits for non-streaming)
- ✅ Authentication via Bearer token
- ✅ Combined gateway support (REST + MCP + WebSocket on same port)

---

## Design Goals

### 1. RESTful Semantics

HTTP methods should match semantic intent:
- **GET** - Idempotent read operations (no side effects)
- **POST** - Create operations or non-idempotent actions
- **PUT** - Replace/update operations (idempotent)
- **DELETE** - Remove operations (idempotent)
- **PATCH** - Partial update operations

### 2. Type Safety

- No string-based HTTP methods in runtime code
- Compile-time validation of HTTP method choices
- Exhaustive pattern matching ensures all cases handled
- IDE autocomplete and type hints

### 3. Streaming Support

- **Non-streaming methods**: Collect all items, return JSON
- **Streaming methods**: Use SSE for real-time updates
- Determined by `streaming` flag in method schema

### 4. Security and Reliability

- Timeout protection against infinite streams
- Memory limits against unbounded buffering
- Optional authentication via API key
- Graceful error handling

---

## Architecture

### Module Structure

```
src/http/
├── mod.rs          - Module exports
├── bridge.rs       - ActivationRestBridge, schema → routes conversion
├── handler.rs      - Request/response processing, SSE streaming
└── server.rs       - Standalone REST server setup
```

### Data Flow

```
User Request
    ↓
Axum Router (route by HTTP method + path)
    ↓
rest_method_handler
    ↓
Lookup MethodInfo (namespace, method, streaming, http_method)
    ↓
Call activation.call(method, params) or route_fn(method, params)
    ↓
Receive PlexusStream
    ↓
handle_method_call (with timeout)
    ↓
Branch on streaming flag:
    ├─ Non-streaming → collect_and_respond (JSON)
    └─ Streaming → stream_sse_response (SSE)
    ↓
HTTP Response
```

---

## HTTP Method Routing

### Type System

**Core Type (plexus-core):**

```rust
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
```

**In MethodSchema:**

```rust
pub struct MethodSchema {
    pub name: String,
    pub description: String,
    pub hash: String,
    pub params: Option<Schema>,
    pub returns: Option<Schema>,
    pub streaming: bool,
    pub bidirectional: bool,
    pub http_method: HttpMethod,  // ← Strongly-typed
    pub request_type: Option<Schema>,
    pub response_type: Option<Schema>,
}
```

### User-Facing API

```rust
#[hub_methods(namespace = "users", description = "User API")]
impl UserActivation {
    /// Get user by ID
    #[hub_method(http_method = "GET")]
    async fn get_user(&self, user_id: String) -> impl Stream<Item = UserEvent> {
        // Implementation
    }

    /// Create new user
    #[hub_method(http_method = "POST")]
    async fn create_user(&self, name: String) -> impl Stream<Item = UserEvent> {
        // Implementation
    }

    /// Update user
    #[hub_method(http_method = "PUT")]
    async fn update_user(&self, user_id: String, name: String) -> impl Stream<Item = UserEvent> {
        // Implementation
    }

    /// Delete user
    #[hub_method(http_method = "DELETE")]
    async fn delete_user(&self, user_id: String) -> impl Stream<Item = UserEvent> {
        // Implementation
    }

    /// Partial update
    #[hub_method(http_method = "PATCH")]
    async fn patch_user(&self, user_id: String, updates: Value) -> impl Stream<Item = UserEvent> {
        // Implementation
    }

    /// Non-RESTful action (defaults to POST)
    #[hub_method]
    async fn send_verification_email(&self, user_id: String) -> impl Stream<Item = UserEvent> {
        // Implementation
    }
}
```

### Route Registration

```rust
// src/http/bridge.rs:88-129
fn schemas_to_rest_routes<A>(schemas: Vec<PluginSchema>) -> Router {
    let registry = MethodRegistry::from_schemas(schemas);
    let mut router = Router::new();

    // Register each method with its specific HTTP method
    for method_info in registry.all_methods() {
        let path = format!("/{}/{}", method_info.namespace, method_info.method);

        // Pattern match on strongly-typed enum
        let method_router = match method_info.http_method {
            HttpMethod::Get => get(rest_method_handler),
            HttpMethod::Post => post(rest_method_handler),
            HttpMethod::Put => put(rest_method_handler),
            HttpMethod::Delete => delete(rest_method_handler),
            HttpMethod::Patch => patch(rest_method_handler),
        };

        router = router.route(&path, method_router);
    }

    router.with_state(state)
}
```

**Resulting Routes:**
- `GET /rest/users/get_user` → UserActivation::get_user
- `POST /rest/users/create_user` → UserActivation::create_user
- `PUT /rest/users/update_user` → UserActivation::update_user
- `DELETE /rest/users/delete_user` → UserActivation::delete_user
- `PATCH /rest/users/patch_user` → UserActivation::patch_user
- `POST /rest/users/send_verification_email` → UserActivation::send_verification_email

---

## Streaming Protection

### Vulnerabilities Addressed

**1. Infinite Stream Attack:**
- Stream never emits `Done` variant
- Handler hangs forever, consuming connection slot
- **Solution:** 5-minute timeout on all method execution

**2. Memory Exhaustion Attack:**
- Stream emits unlimited data items
- Non-streaming methods buffer everything in memory
- **Solution:** Hard limits on item count and byte size

### Protection Mechanisms

```rust
// src/http/handler.rs:23-34
/// Maximum time to wait for a method to complete (5 minutes)
const METHOD_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum number of items to buffer for non-streaming methods
const MAX_BUFFERED_ITEMS: usize = 10_000;

/// Maximum total bytes to buffer for non-streaming methods (100MB)
const MAX_BUFFER_BYTES: usize = 100 * 1024 * 1024;
```

### Timeout Implementation

```rust
// src/http/handler.rs:52-85
pub async fn handle_method_call(stream: PlexusStream, method_info: MethodInfo) -> Response {
    // Apply timeout to prevent infinite streams
    let result = if method_info.streaming {
        tokio::time::timeout(METHOD_TIMEOUT, stream_sse_response(stream)).await
    } else {
        tokio::time::timeout(METHOD_TIMEOUT, collect_and_respond(stream)).await
    };

    match result {
        Ok(response) => response,
        Err(_elapsed) => {
            // Timeout occurred
            (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "error": format!(
                        "Method execution timed out after {} seconds",
                        METHOD_TIMEOUT.as_secs()
                    ),
                    "timeout_seconds": METHOD_TIMEOUT.as_secs(),
                })),
            ).into_response()
        }
    }
}
```

### Buffer Limit Implementation

```rust
// src/http/handler.rs:92-187
async fn collect_and_respond(mut stream: PlexusStream) -> Response {
    let mut data_items = Vec::new();
    let mut total_bytes: usize = 0;

    while let Some(item) = stream.next().await {
        match item {
            PlexusStreamItem::Data { content, .. } => {
                // Check item count limit
                if data_items.len() >= MAX_BUFFERED_ITEMS {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(json!({
                            "error": format!(
                                "Stream exceeded maximum item count of {}",
                                MAX_BUFFERED_ITEMS
                            ),
                        })),
                    ).into_response();
                }

                // Check total byte limit
                let item_bytes = serde_json::to_vec(&content).unwrap_or_default().len();
                total_bytes += item_bytes;

                if total_bytes > MAX_BUFFER_BYTES {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(json!({
                            "error": format!(
                                "Stream exceeded maximum buffer size of {} bytes",
                                MAX_BUFFER_BYTES
                            ),
                        })),
                    ).into_response();
                }

                data_items.push(content);
            }
            // ... handle other item types
        }
    }

    // Build and return response
    (StatusCode::OK, Json(json!({ "data": data_items }))).into_response()
}
```

### Protection Summary

| Attack Vector | Protection | Limit | Status Code |
|--------------|------------|-------|-------------|
| Infinite stream | Timeout | 5 minutes | 504 Gateway Timeout |
| Too many items | Item count check | 10,000 items | 413 Payload Too Large |
| Too much data | Byte size check | 100 MB | 413 Payload Too Large |

---

## Implementation Details

### Bridge Pattern

**ActivationRestBridge** wraps any `Activation` and converts it to REST routes:

```rust
// src/http/bridge.rs:227-287
pub struct ActivationRestBridge<A: Activation> {
    activation: Arc<A>,
    schemas: Vec<PluginSchema>,
    route_fn: Option<RouteFn>,
    server_name: Option<String>,
    server_version: Option<String>,
}

impl<A: Activation> ActivationRestBridge<A> {
    pub fn new(activation: Arc<A>) -> Self {
        let schemas = vec![activation.plugin_schema()];
        Self {
            activation,
            schemas,
            route_fn: None,
            server_name: None,
            server_version: None,
        }
    }

    pub fn with_router(mut self, route_fn: RouteFn) -> Self {
        self.route_fn = Some(route_fn);
        self
    }

    pub fn into_router(self) -> Router {
        schemas_to_rest_routes(self.activation, self.schemas, self.route_fn)
    }
}
```

### Method Registry

**Efficient method lookup:**

```rust
// src/http/bridge.rs:38-86
pub struct MethodRegistry {
    methods: Arc<HashMap<String, RestMethodInfo>>,
}

#[derive(Clone, Debug)]
pub struct RestMethodInfo {
    pub namespace: String,
    pub method: String,
    pub streaming: bool,
    pub http_method: HttpMethod,
}

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
                    http_method: method.http_method,
                });
            }
        }

        Self { methods: Arc::new(methods) }
    }

    pub fn get(&self, namespace: &str, method: &str) -> Option<&RestMethodInfo> {
        let key = format!("{}.{}", namespace, method);
        self.methods.get(&key)
    }
}
```

### Response Types

**Non-Streaming (JSON):**

```json
{
  "data": [
    {"id": "123", "name": "Alice"},
    {"id": "456", "name": "Bob"}
  ],
  "progress": [
    {"message": "Processing...", "percentage": 50},
    {"message": "Complete", "percentage": 100}
  ]
}
```

**Streaming (SSE):**

```
event: data
data: {"content": {"id": "123", "name": "Alice"}}

event: progress
data: {"message": "Processing...", "percentage": 50}

event: data
data: {"content": {"id": "456", "name": "Bob"}}

event: done
data: {}
```

---

## Usage Guide

### Basic Setup

```rust
use plexus_transport::{serve_rest_http, RestHttpConfig};
use std::sync::Arc;

let activation = Arc::new(MyActivation::new());
let config = RestHttpConfig::new(8888);

let handle = serve_rest_http(
    activation,
    None,  // flat_schemas for hub activations
    None,  // route_fn for hub routing
    config,
    None,  // api_key for authentication
).await?;
```

### With Authentication

```rust
let config = RestHttpConfig::new(8888);
let api_key = Some("my-secret-key".to_string());

let handle = serve_rest_http(
    activation,
    None,
    None,
    config,
    api_key,  // Requires "Authorization: Bearer my-secret-key"
).await?;
```

### Hub Activation

```rust
let hub = Arc::new(MyHub::new());
let flat_schemas = Some(hub.list_plugin_schemas());

// Create routing function
let route_fn = {
    let hub = hub.clone();
    Arc::new(move |method: String, params: Value| {
        let hub = hub.clone();
        Box::pin(async move {
            hub.route(&method, params).await
        }) as Pin<Box<dyn Future<Output = Result<PlexusStream, PlexusError>> + Send>>
    })
};

let handle = serve_rest_http(
    hub,
    flat_schemas,
    Some(route_fn),
    config,
    None,
).await?;
```

### Making Requests

**Non-Streaming Method:**

```bash
curl -X GET http://localhost:8888/rest/users/get_user \
  -H "Content-Type: application/json" \
  -d '{"user_id": "123"}'

# Response
{
  "data": [
    {"id": "123", "name": "Alice", "email": "alice@example.com"}
  ]
}
```

**Streaming Method:**

```bash
curl -X POST http://localhost:8888/rest/logs/stream_logs \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{"filter": "errors"}'

# Response (SSE)
event: data
data: {"content": {"level": "error", "message": "Failed to connect"}}

event: data
data: {"content": {"level": "error", "message": "Timeout exceeded"}}

event: done
data: {}
```

---

## Testing

### Test Coverage

**tests/http_gateway_streaming_tests.rs:**

```rust
#[tokio::test]
async fn test_normal_request_completes_successfully() { }

#[tokio::test]
async fn test_memory_exhaustion_protected_by_limits() { }

#[tokio::test]
async fn test_buffer_limit_by_item_count() { }

#[tokio::test]
async fn test_buffer_limit_by_total_bytes() { }

#[tokio::test]
async fn test_slow_completion_within_timeout() { }

#[tokio::test]
async fn test_infinite_stream_hangs_without_timeout() { }

#[tokio::test]
async fn test_infinite_stream_protected_by_timeout() { }

#[tokio::test]
async fn test_memory_exhaustion_unbounded_buffering() { }
```

### Test Results

✅ **7/8 tests pass quickly (<5s)**
⏱️ **1 test takes 5 minutes** (`test_infinite_stream_protected_by_timeout` - by design)

### Attack Scenario Tests

**Infinite Stream:**
```rust
fn create_infinite_stream() -> PlexusStream {
    Box::pin(stream! {
        for i in 0..5 {
            yield PlexusStreamItem::Data { ... };
        }
        // Never yield Done - hang forever
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    })
}

// Test verifies: Returns 504 after 5 minutes
```

**Memory Exhaustion:**
```rust
fn create_memory_exhaustion_stream(items: usize, size_kb: usize) -> PlexusStream {
    Box::pin(stream! {
        let large_data = vec![0u8; size_kb * 1024];
        for i in 0..items {
            yield PlexusStreamItem::Data {
                content: json!({"data": large_data.clone()}),
                ...
            };
        }
        yield PlexusStreamItem::Done { ... };
    })
}

// Test verifies: Returns 413 when limits exceeded
```

---

## Backward Compatibility

- ✅ Methods without `http_method` attribute default to POST
- ✅ Existing code continues to work without modifications
- ✅ Schema serialization includes `http_method` for clients
- ✅ All protections apply uniformly regardless of HTTP method

---

## Future Enhancements

### Potential Additions

1. **Query parameter support for GET**
   - Allow GET methods to accept params in query string
   - Validate params against schema

2. **Path parameter routing**
   - Support `/users/:id` style routes
   - Extract params from URL path

3. **Content negotiation**
   - Support different response formats (JSON, XML, MessagePack)
   - Respect `Accept` header

4. **OpenAPI/Swagger generation**
   - Auto-generate API documentation from schemas
   - Include all HTTP methods and parameters

5. **CORS middleware**
   - Enable browser-based clients
   - Configurable allowed origins

6. **Rate limiting**
   - Protect against abuse
   - Per-method or per-activation limits

---

## Conclusion

The HTTP gateway provides a **production-ready RESTful API** for Plexus activations with:

- ✅ **Type-safe HTTP method routing** via strongly-typed enums
- ✅ **Comprehensive security** via timeout and memory protections
- ✅ **Flexible streaming** via SSE for real-time updates
- ✅ **Clean architecture** following existing transport patterns
- ✅ **Full test coverage** with attack scenario validation

The implementation demonstrates how protocol-level features (like `schema` discovery) integrate seamlessly with user methods, providing a consistent and discoverable API surface.
