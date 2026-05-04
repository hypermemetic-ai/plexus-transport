# Transport-Layer Authentication

**Status**: Implemented (AUTHZ-BEARER-1 supersedes the historical Bearer/Cookie precedence section; see "Header layout (AUTHZ-BEARER-1)" below for the source of truth)
**Date**: 2026-04-07 (AUTHZ-BEARER-1 amendment 2026-05-04)
**Related**: plexus-core authentication-framework.md, plexus-macros authentication-codegen.md, `plans/AUTHZ/AUTHZ-BEARER-1.md`, `plans/AUTHZ/AUTHZ-BEARER-S01-output.md`

> **Reader note (2026-05-04):** the WebSocket-upgrade middleware was rewritten under [AUTHZ-BEARER-1](../../../plans/AUTHZ/AUTHZ-BEARER-1.md). The static `api_key` admission key now lives on a dedicated header (default `X-Plexus-API-Key`); `Authorization: Bearer` is reserved for `SessionValidator` user-identity tokens. Skip directly to the [Header layout (AUTHZ-BEARER-1)](#header-layout-authz-bearer-1) section for the current contract; the older "Implementation" code excerpts below describe the pre-AUTHZ-BEARER-1 shape and have NOT been updated.

## Overview

The Plexus transport layer provides cookie-based authentication for WebSocket connections. During the HTTP → WebSocket upgrade, the server extracts cookies, validates them using a `SessionValidator`, and stores the resulting `AuthContext` in the connection state. This context is then available to all RPC methods invoked on that connection.

## Architecture

### High-Level Flow

```
Client (Browser)                 Transport Layer                   Activation Layer
     │                                  │                                 │
     │── HTTP Upgrade (with Cookie) ───>│                                 │
     │                                  │                                 │
     │                            [CombinedAuthMiddleware]                │
     │                                  │                                 │
     │                            Extract Cookie header                   │
     │                            Call SessionValidator                   │
     │                                  │                                 │
     │                            Valid? ───┐                             │
     │                                  │   │                             │
     │                              ┌── Yes │                             │
     │                              │   No  │                             │
     │                              │   │   │                             │
     │                              │   │   └─> Proceed without auth      │
     │                              │   │                                 │
     │                              └───> Store AuthContext in Extensions │
     │                                  │                                 │
     │<──────── 101 Switching ──────────│                                 │
     │                                  │                                 │
     │══ WebSocket Connected ═══════════│                                 │
     │                                  │                                 │
     │── RPC: call("method", params) ───>│                                 │
     │                                  │                                 │
     │                            Extract AuthContext                     │
     │                            from Extensions                         │
     │                                  │                                 │
     │                                  │── activate(method, params, ───>│
     │                                  │           Some(&auth))          │
     │                                  │                                 │
     │                                  │                        [Check requires_auth]
     │                                  │                        Call implementation
     │                                  │                                 │
     │                                  │<─────── PlexusStream ───────────│
     │                                  │                                 │
     │<──── RPC Response ───────────────│                                 │
```

### Key Components

1. **CombinedAuthMiddleware** (`websocket.rs`): Tower middleware that intercepts HTTP upgrade requests
2. **SessionValidator** (`plexus-core`): Validates cookie values and returns `AuthContext`
3. **Extensions** (jsonrpsee): Request-scoped storage for `Arc<AuthContext>`
4. **RPC Converter**: Extracts `AuthContext` from Extensions and passes to `activate()`

## Implementation Details

### 1. Server Configuration (`server.rs`)

Servers opt-in to cookie-based authentication using the builder pattern:

```rust
use plexus_transport::TransportServer;
use std::sync::Arc;

let validator = Arc::new(MySessionValidator::new());

let server = TransportServer::builder(activation, rpc_converter)
    .with_websocket(8080)
    .with_session_validator(validator)  // ← Enable cookie auth
    .build()
    .await?;
```

**Storage:**
```rust
// server.rs:40
struct TransportServer<A> {
    session_validator: Option<Arc<dyn SessionValidator>>,
    // ...
}

// server.rs:245
pub fn with_session_validator(mut self, validator: Arc<dyn SessionValidator>) -> Self {
    self.session_validator = Some(validator);
    self
}
```

**Propagation to transport** (`server.rs:82`):
```rust
Some(serve_websocket(module, ws_config, self.session_validator.clone()).await?)
```

### 2. Middleware Layer (`websocket.rs`)

The `CombinedAuthMiddleware` is a Tower middleware that wraps the jsonrpsee HTTP service:

**Middleware struct** (`websocket.rs:85-89`):
```rust
#[derive(Clone)]
pub(super) struct CombinedAuthMiddleware<S> {
    pub(super) service: S,
    pub(super) expected_bearer: Option<String>,  // For API key auth (optional)
    pub(super) session_validator: Option<Arc<dyn SessionValidator>>,  // For cookie auth
}
```

**Construction** (`websocket.rs:36-43`):
```rust
#[cfg(feature = "mcp-gateway")]
{
    let expected_bearer = config.api_key.map(|key| format!("Bearer {}", key));
    let middleware = tower::ServiceBuilder::new().layer_fn(move |service| {
        CombinedAuthMiddleware {
            service,
            expected_bearer: expected_bearer.clone(),
            session_validator: session_validator.clone(),
        }
    });
    let server = Server::builder()
        .set_http_middleware(middleware)
        .build(config.addr)
        .await?;
    // ...
}
```

**Note**: Middleware is only active when `mcp-gateway` feature is enabled. Without it, no auth checks occur.

### 3. HTTP Upgrade Flow (`websocket.rs:109-165`)

When a client initiates a WebSocket connection, the middleware intercepts the HTTP upgrade request:

**Step 1: Bearer token check (if configured)**
```rust
// websocket.rs:110-132
if let Some(ref expected) = self.expected_bearer {
    let auth_ok = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == expected)
        .unwrap_or(false);

    if !auth_ok {
        // Return 401 Unauthorized immediately
        return Box::pin(async move {
            Ok(http::Response::builder()
                .status(http::StatusCode::UNAUTHORIZED)
                .header(http::header::WWW_AUTHENTICATE, "Bearer realm=\"plexus\"")
                .body(HttpBody::from("Unauthorized"))
                .unwrap())
        });
    }
}
```

**Step 2: Cookie validation (if configured)**
```rust
// websocket.rs:134-160
if let Some(validator) = session_validator.clone() {
    if let Some(cookie_header) = request.headers().get(http::header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            let cookie_str_owned = cookie_str.to_string();
            return Box::pin(async move {
                // Validate cookie asynchronously
                if let Some(auth_ctx) = validator.validate(&cookie_str_owned).await {
                    tracing::debug!("Cookie validation successful for user: {}", auth_ctx.user_id);

                    // Store AuthContext in request Extensions
                    // Extensions are propagated by jsonrpsee to RPC methods
                    request.extensions_mut().insert(Arc::new(auth_ctx));
                } else {
                    tracing::debug!("Cookie validation failed, proceeding without auth");
                    // Cookie invalid - connection proceeds as anonymous
                }

                // Forward request (with or without AuthContext in Extensions)
                let mut service = self.service.clone();
                service.call(request).await.map_err(Into::into)
            });
        }
    }
    tracing::debug!("No cookie header present, proceeding without auth");
}
```

**Step 3: Pass-through (no auth)**
```rust
// websocket.rs:162-164
// No auth required or no cookie present - forward request unchanged
let fut = self.service.call(request);
Box::pin(async move { fut.await.map_err(Into::into) })
```

### 4. Storage in Extensions

**Extensions** are jsonrpsee's mechanism for request-scoped data:
- Type-safe storage: `extensions.insert(Arc<AuthContext>)`
- Retrieval: `extensions.get::<Arc<AuthContext>>()`
- Propagated through HTTP → WebSocket upgrade → RPC call chain

**Why Arc?**
- `AuthContext` is cloned multiple times (once per RPC call)
- `Arc` makes cloning cheap (pointer copy vs. struct copy)
- Allows sharing auth context across concurrent method calls

### 5. Extraction in RPC Layer

The RPC converter (generated by macros or custom code) extracts `AuthContext` from Extensions and passes it to `activate()`:

**Typical extraction pattern:**
```rust
// In RPC method handler (jsonrpsee)
let auth_ctx: Option<&AuthContext> = extensions
    .get::<Arc<AuthContext>>()
    .map(|arc| arc.as_ref());

// Call activation with auth context
let stream = activation.activate(method, params, auth_ctx).await?;
```

**Note**: This extraction happens once per RPC call, not once per connection. The same `Arc<AuthContext>` is retrieved from Extensions each time.

## Security Considerations

### 1. Feature Gating (`mcp-gateway`)

Cookie authentication middleware is only compiled when the `mcp-gateway` feature is enabled:

```rust
#[cfg(feature = "mcp-gateway")]
mod auth {
    // CombinedAuthMiddleware implementation
}
```

**Rationale:**
- Not all deployments need HTTP-level auth (e.g., stdio-based MCP servers)
- Reduces binary size for non-HTTP transports
- Prevents accidental auth bypass if middleware is misconfigured

**Tradeoff**: Without `mcp-gateway`, cookie auth is **completely disabled**, even if a validator is configured. This is intentional for security.

### 2. Anonymous Connections

If cookie validation returns `None`, the connection **proceeds without auth**:
- Methods requiring `auth: &AuthContext` will fail with `Unauthenticated` error
- Methods without auth requirements continue to work
- No connection rejection (allows mixed auth/non-auth services)

**Design choice**: Reject at method level, not connection level, to support:
- Public + authenticated methods on the same server
- Graceful degradation (public methods work even if auth fails)
- Backward compatibility (add auth to existing services without breaking public endpoints)

**Alternative design** (rejected): Reject WebSocket upgrade if cookie is invalid
- **Pros**: Clear auth boundary, no anonymous connections
- **Cons**: Can't mix public/private methods, breaks backward compat
- **Tradeoff**: We chose flexibility over strict enforcement

### 3. Token Expiration

Cookie validation happens **once per connection**, not per RPC call:
- If a JWT expires mid-connection, the WebSocket remains authenticated
- Client must monitor token expiration and reconnect proactively
- Server can't revoke active sessions without closing the connection

**Mitigation strategies:**
1. **Short-lived tokens** (e.g., 15-minute JWTs) + client-side reconnection logic
2. **Refresh tokens** stored in HttpOnly cookies (refresh before expiry)
3. **Session revocation** via DB lookup in `SessionValidator` (checks DB on every connection, not in cache)

**Example client-side reconnection:**
```typescript
function monitorTokenExpiration(ws: WebSocket, expiresAt: number) {
  const msUntilExpiry = expiresAt - Date.now();
  setTimeout(() => {
    console.log("Token expiring soon, reconnecting...");
    ws.close();  // Trigger reconnection with fresh token
    connect();   // Client handles reconnection + token refresh
  }, msUntilExpiry - 60000);  // Reconnect 1 minute before expiry
}
```

### 4. Cookie Extraction

The middleware receives the raw `Cookie` header value:
```
Cookie: session=abc123; path=/; httponly; secure
```

**SessionValidator responsibility:**
- Parse cookie string to extract relevant value(s)
- Validate signature (for signed cookies or JWT-in-cookie)
- Check expiration
- Return `AuthContext` or `None`

**Transport layer does NOT:**
- Parse individual cookies (passes raw header to validator)
- Check expiration (validator's job)
- Enforce cookie attributes (browser's job: httponly, secure, samesite)

### 5. HTTPS Requirement

Cookie-based auth **requires HTTPS** in production:
- Without HTTPS, cookies can be intercepted (man-in-the-middle attacks)
- Use `secure` flag on cookies to prevent transmission over HTTP
- Use `httponly` flag to prevent JavaScript access (XSS protection)
- Use `samesite=strict` or `samesite=lax` to prevent CSRF attacks

**Development exception**: HTTP is acceptable for localhost testing with TestSessionValidator.

## Header layout (AUTHZ-BEARER-1)

> **Status:** Updated 2026-05-04 by AUTHZ-BEARER-1. The historical "Bearer
> token + Cookie combination" section described a different shape; this
> subsection supersedes it. See `plans/AUTHZ/AUTHZ-BEARER-1.md` and
> `plans/AUTHZ/AUTHZ-BEARER-S01-output.md` §4 for the full contract.

The transport's WebSocket-upgrade middleware separates **two unrelated**
authentication concerns onto **two unrelated headers**:

| Header (default) | Carries | Consumed by | Configurable name? |
|---|---|---|---|
| `X-Plexus-API-Key` | Static deployment-admission key (`api_key`) | Layer-0 admission gate; never reaches `SessionValidator` | Yes — `WebSocketConfig::with_api_key_header(HeaderName)` / `TransportServerBuilder::with_api_key_header(HeaderName)` |
| `Authorization: Bearer <token>` | User-identity token (JWT or opaque) | `SessionValidator::validate(token)` (when wired) | No — fixed by HTTP convention |
| `Cookie: access_token=<...>` | Browser-issued user-identity session | `SessionValidator::validate(cookie_value)` (when wired) | No — fixed by HTTP convention |

**Decision sequence on every WS upgrade:**

1. **Static admission gate (`api_key`).** When configured, the upgrade
   request MUST carry the configured api_key header with the matching
   value. Header missing → HTTP 401 `"api key required"`. Value mismatch
   → HTTP 401 `"api key invalid"`. The `SessionValidator` path is not
   consulted.
2. **Dynamic identity (`SessionValidator`, when wired).** Try inputs in
   order; the first that returns `Some(AuthContext)` wins:
   1. `Cookie` header → `SessionValidator::validate(cookie_str)`
   2. `Authorization: Bearer <token>` (RFC 6750 prefix stripped) →
      `SessionValidator::validate(token)`
3. **Strict-mode reject (RED-9).** If `SessionValidator` was consulted,
   produced no `AuthContext`, AND `reject_upgrade_on_auth_failure` is
   ON → HTTP 401 (`"session invalid or expired"` if any input was
   present, `"session cookie or bearer required"` if no input was
   present).
4. **Pass-through.** Otherwise, dispatch with whatever `AuthContext`
   (if any) was produced.

The two layers compose: when both `api_key` and `SessionValidator` are
configured, the static gate fires first; only after it passes is the
`SessionValidator` consulted.

**Example configuration:**
```rust
use http::HeaderName;
TransportServer::builder(activation, rpc_converter)
    .with_websocket(8080)
    .with_api_key(Some("secret-admission-key".into()))   // X-Plexus-API-Key
    .with_session_validator(my_validator)                // Cookie OR Bearer
    // Optional: override the api_key header name
    // .with_api_key_header(HeaderName::from_static("x-my-key"))
    .reject_upgrade_on_auth_failure()                    // RED-9 strict mode
    .build().await?;
```

**Request scenarios** (`api_key=K`, validator wired, strict mode OFF):

| `X-Plexus-API-Key` | `Cookie` | `Authorization` | Result |
|---|---|---|---|
| absent | any | any | 401 `"api key required"` |
| `K_wrong` | any | any | 401 `"api key invalid"` |
| `K` | absent | absent | 200, no AuthContext (pass through) |
| `K` | `access_token=valid` | absent | 200, AuthContext from cookie |
| `K` | absent | `Bearer valid-jwt` | 200, AuthContext from bearer |
| `K` | `access_token=valid` | `Bearer valid-jwt` | 200, AuthContext from cookie (cookie wins) |
| `K` | `access_token=garbage` | `Bearer valid-jwt` | 200, AuthContext from bearer (fallback) |

### v1 compat shim (deprecated, single release window)

When `api_key` is configured AND the configured api_key header is absent
AND `Authorization: Bearer <value>` matches the api_key exactly AND
`SessionValidator` is NOT wired, the request is accepted as if the
api_key header had matched. A `tracing::warn!` deprecation notice
including the configured header name is emitted. The compat shim is OFF
whenever `SessionValidator` is configured, to prevent re-entry of the
header-conflation defect this contract removes.

A separate, future ticket (AUTHZ-BEARER-2) removes the compat shim
after one release.

### Out-of-scope follow-ups

- **MCP HTTP and REST HTTP gateways** (`mcp/server.rs`, `http/server.rs`,
  `combined.rs`) continue to enforce `api_key` against
  `Authorization: Bearer` until a follow-up ticket aligns them with the
  new header layout.
- **`SessionValidator` trait input disambiguation.** The trait method
  `validate(&str)` now receives either a Cookie-header string or a
  bare bearer token. Existing implementations (e.g. `TrakAuth`) handle
  both shapes; new implementations MUST disambiguate. A future trait
  revision could split inputs by source (`CredentialSource::Cookie(&str)`
  vs `CredentialSource::Bearer(&str)`); not part of this contract.

## Error Handling

### Validation Errors

`SessionValidator::validate()` returns `Option<AuthContext>`:
- `Some(ctx)` → Valid session, store in Extensions
- `None` → Invalid session, proceed without auth

**Transport layer never returns errors for cookie validation failures.** This is intentional:
- Invalid cookies are common (expired, tampered, wrong format)
- Logging as `debug` level: `"Cookie validation failed, proceeding without auth"`
- Methods requiring auth will fail with `Unauthenticated` error later

### Unauthenticated Error Propagation

When activation returns `PlexusError::Unauthenticated`:

**MCP bridge** (`mcp/bridge.rs:96-98`):
```rust
PlexusError::Unauthenticated(reason) => {
    McpError::invalid_request(format!("Authentication required: {}", reason), None)
}
```

**Client receives:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32600,
    "message": "Authentication required: This method requires authentication"
  }
}
```

**HTTP gateway** (if implemented): Would return `401 Unauthorized` or `403 Forbidden` depending on the reason.

## Performance Considerations

### Validation Cost

Cookie validation happens **once per WebSocket connection**, not per RPC call:
- JWT validation: ~100-500µs (RSA signature check + JSON parsing)
- DB session lookup: ~1-10ms (depends on DB latency + connection pool)
- Redis session check: ~1ms (network + lookup)

**Impact**: Connection establishment time increases by validation latency. Once connected, no per-call overhead.

### Async Validation

Validation is asynchronous (`validator.validate().await`):
- Does not block the event loop during I/O (DB/Redis queries)
- Allows concurrent connections to validate in parallel
- Middleware spawns validation as part of HTTP upgrade flow

**Tradeoff**: Async adds complexity (futures, pinning) but prevents blocking on slow validators.

### Extensions Overhead

Storing `Arc<AuthContext>` in Extensions:
- **Cost**: One `Arc::clone()` per RPC call (~5-10 CPU cycles)
- **Memory**: ~8 bytes per connection (pointer to heap-allocated `AuthContext`)
- **Lookup**: O(1) HashMap lookup by TypeId

**Optimization**: Could cache `Arc<AuthContext>` in connection state instead of Extensions, but Extensions are standard jsonrpsee mechanism.

## Testing

### Unit Tests

Test `CombinedAuthMiddleware` directly:
1. **Valid Bearer token** → Request forwarded
2. **Invalid Bearer token** → 401 Unauthorized
3. **Valid cookie** → AuthContext stored in Extensions
4. **Invalid cookie** → Request forwarded without AuthContext
5. **No auth headers** → Request forwarded

### Integration Tests

Test full WebSocket flow:
1. Connect with valid cookie → Call auth method → Success
2. Connect with invalid cookie → Call auth method → `Unauthenticated` error
3. Connect without cookie → Call public method → Success
4. Connect without cookie → Call auth method → `Unauthenticated` error

### E2E Tests (FormVeritasV2)

Playwright tests with real browser cookies:
- Set cookie via `page.context().addCookies([...])`
- Connect WebSocket → Call auth methods → Verify access control
- Clear cookies → Reconnect → Verify access denied

See `FormVeritasV2/uscis-web/tests/auth-flow.spec.ts` for examples.

## Deployment Considerations

### Reverse Proxy Setup

When deploying behind a reverse proxy (nginx, Envoy, Traefik):

**Nginx configuration:**
```nginx
location /ws {
    proxy_pass http://backend:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "Upgrade";
    proxy_set_header Host $host;
    proxy_set_header Cookie $http_cookie;  # Forward cookies
}
```

**Critical**: Proxy must forward `Cookie` header to backend, or auth will fail silently.

### CORS and Cookies

For browser-based clients with cross-origin requests:

**Server-side (WebSocket):**
- WebSocket upgrade bypasses CORS (but browser enforces same-origin for cookie sending)
- Use `Access-Control-Allow-Origin` on HTTP endpoints that set cookies

**Client-side (JavaScript):**
```typescript
// Cookies are sent automatically for same-origin WebSocket connections
const ws = new WebSocket("wss://app.example.com/ws");

// For cross-origin, cookies require specific browser settings (rarely works)
// Better: Use same-origin WebSocket or Bearer tokens for cross-origin
```

**Recommendation**: Serve WebSocket endpoint on same origin as web app to ensure cookies are sent.

### Load Balancing

Cookie-based auth works with stateless load balancing:
- Each connection validates independently (no shared session state required)
- JWT-based auth: Stateless, no coordination needed
- DB/Redis-based auth: Shared session store across backends

**Sticky sessions NOT required** (each RPC call is independent).

## Future Enhancements

### 1. Per-Call Re-validation

Support re-validating auth on every RPC call instead of once per connection:

```rust
#[hub_method(revalidate_auth)]
async fn sensitive_action(&self, auth: &AuthContext) -> Result<()> {
    // Auth is re-checked via SessionValidator on every call
}
```

**Use case**: Immediate session revocation (e.g., logout, account suspension).

**Tradeoff**: Adds latency to every call (~1-10ms depending on validator).

### 2. WebSocket Subprotocol for Token Refresh

Define a custom WebSocket subprotocol for refreshing tokens without reconnection:

**Client sends:**
```json
{ "type": "refresh_token", "refresh_token": "..." }
```

**Server responds:**
```json
{ "type": "token_refreshed", "access_token": "...", "expires_in": 900 }
```

**Benefit**: Avoids connection interruption during token refresh.

### 3. Multiple Cookie Support

Allow validators to extract from multiple cookies:

```rust
#[async_trait]
impl SessionValidator for MultiCookieValidator {
    async fn validate(&self, cookie_header: &str) -> Option<AuthContext> {
        let cookies = parse_cookies(cookie_header);
        let access_token = cookies.get("access_token")?;
        let refresh_token = cookies.get("refresh_token");
        // Validate both...
    }
}
```

**Use case**: Separate access/refresh tokens, multi-domain cookies.

### 4. Custom Error Responses

Allow validators to return custom error messages:

```rust
#[async_trait]
trait SessionValidator {
    async fn validate(&self, cookie: &str) -> Result<AuthContext, AuthError>;
}

enum AuthError {
    Expired(String),
    InvalidSignature,
    Revoked,
}
```

**Benefit**: Clients can differentiate between "expired" (refresh token) and "invalid" (re-authenticate).

## Migration Guide

### Adding Cookie Auth to Existing Server

**Before:**
```rust
TransportServer::builder(activation, rpc_converter)
    .with_websocket(8080)
    .build().await?;
```

**After:**
```rust
let validator = Arc::new(MySessionValidator::new());

TransportServer::builder(activation, rpc_converter)
    .with_websocket(8080)
    .with_session_validator(validator)  // ← Add this
    .build().await?;
```

**Effect:**
- Existing connections without cookies continue to work (anonymous)
- Methods with `auth: &AuthContext` now enforce authentication
- No client changes needed for public methods

### Testing with TestSessionValidator

**Development setup:**
```rust
#[cfg(debug_assertions)]
let validator = Arc::new(plexus_core::plexus::TestSessionValidator::new());

#[cfg(not(debug_assertions))]
let validator = Arc::new(ProductionValidator::new());

server.with_session_validator(validator);
```

**Client testing:**
```bash
# Set test cookie
curl -b "session=alice" http://localhost:8080/ws

# Or with advanced format
curl -b "test_user=bob|tenant=acme|roles=admin,user" http://localhost:8080/ws
```

## Related Documentation

- **Core framework**: `plexus-core/docs/architecture/authentication-framework.md` - AuthContext, SessionValidator traits
- **Code generation**: `plexus-macros/docs/architecture/authentication-codegen.md` - How `auth` parameters are injected
- **FormVeritasV2**: `docs/authentication.md` - Keycloak JWT implementation
- **E2E tests**: `FormVeritasV2/uscis-web/tests/auth-flow.spec.ts` - Playwright test examples
