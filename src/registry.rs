//! Z2H-6: boot-time backend self-registration with the Plexus registry.
//!
//! Per the Z2H-S3 protocol decision (protocol A — transport-level boot
//! push): when a `TransportServer` starts its WebSocket transport, it
//! announces itself to the registry as a first-class, observable act:
//!
//! ```text
//! registry.register {
//!   name, host, port: <actual bound port>, protocol: "ws",
//!   version, metadata: { pid, schema_hash }, source: "auto"
//! }
//! ```
//!
//! Key properties:
//!
//! - **Fail-open**: registration is never a boot dependency. The client
//!   makes at most [`REGISTER_ATTEMPTS`] attempts with a 1 s backoff and
//!   then emits ONE warn; the service serves regardless.
//! - **Upsert semantics**: the registry keys on `name`, so a restarting
//!   backend re-registers without error (see plexus-registry Z2H-6).
//! - **Stop-invalidation**: graceful shutdown sends `registry.deregister`
//!   (is_active=0). Crashed processes are evicted lazily by resolvers
//!   (synapse flips the entry inactive when `_info` verification fails).
//! - **Opt-out**: `TransportServerBuilder::without_registry()` or
//!   `PLEXUS_NO_REGISTRY=1` (the `--no-register` convention).
//! - **Endpoint**: `PLEXUS_REGISTRY_URL` (default `ws://127.0.0.1:4444` —
//!   the registry owns :4444 per the ZERO-TO-100 epic decision).

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};

/// Default registry endpoint. The registry owns :4444 (ZERO-TO-100 epic
/// decision 2026-06-10), aligning with synapse's default connect target.
pub const DEFAULT_REGISTRY_URL: &str = "ws://127.0.0.1:4444";

/// Environment variable overriding the registry endpoint.
pub const REGISTRY_URL_ENV: &str = "PLEXUS_REGISTRY_URL";

/// Environment variable disabling boot-time self-registration entirely.
/// Truthy values: `1`, `true`, `yes` (case-insensitive).
pub const NO_REGISTRY_ENV: &str = "PLEXUS_NO_REGISTRY";

/// Registration attempts before the single fail-open warn.
pub const REGISTER_ATTEMPTS: u32 = 3;

/// Backoff between registration attempts.
pub const REGISTER_BACKOFF: Duration = Duration::from_secs(1);

/// Per-call timeout for registry RPC interactions.
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect timeout for the registry endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Everything needed to announce (and later invalidate) one backend
/// registration.
#[derive(Debug, Clone)]
pub struct RegistryRegistration {
    /// Registry WebSocket endpoint, e.g. `ws://127.0.0.1:4444`.
    pub url: String,
    /// Backend name (the hub root namespace by default).
    pub name: String,
    /// Host the backend is reachable at.
    pub host: String,
    /// ACTUAL bound port (OS-assigned when the server bound port 0).
    pub port: u16,
    /// Backend schema version.
    pub version: String,
    /// Schema identity hash (changes when the backend's surface changes).
    pub schema_hash: String,
}

/// Resolve the registry endpoint: `PLEXUS_REGISTRY_URL` or the default.
pub fn registry_url_from_env() -> String {
    std::env::var(REGISTRY_URL_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_REGISTRY_URL.to_string())
}

/// `PLEXUS_NO_REGISTRY` runtime kill switch (the `--no-register`
/// convention as an env var — wins over builder configuration).
pub fn registry_disabled_by_env() -> bool {
    std::env::var(NO_REGISTRY_ENV)
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        })
        .unwrap_or(false)
}

/// Register `reg` with the registry, retrying up to [`REGISTER_ATTEMPTS`]
/// times. Returns `true` when registered. On final failure emits exactly
/// one `warn` — registration is fail-open and never blocks serving.
pub async fn register_with_retry(reg: &RegistryRegistration) -> bool {
    let mut last_err = None;
    for attempt in 1..=REGISTER_ATTEMPTS {
        match register(reg).await {
            Ok(()) => {
                tracing::info!(
                    "registered with registry at {} as '{}' ({}:{}, schema {})",
                    reg.url, reg.name, reg.host, reg.port, reg.schema_hash,
                );
                return true;
            }
            Err(e) => {
                tracing::debug!("registry registration attempt {attempt} failed: {e:#}");
                last_err = Some(e);
                if attempt < REGISTER_ATTEMPTS {
                    tokio::time::sleep(REGISTER_BACKOFF).await;
                }
            }
        }
    }
    tracing::warn!(
        "could not register '{}' with registry at {} after {} attempts \
         (serving anyway — registration is fail-open; set {}=1 to silence): {:#}",
        reg.name, reg.url, REGISTER_ATTEMPTS, NO_REGISTRY_ENV,
        last_err.unwrap_or_else(|| anyhow!("unknown error")),
    );
    false
}

/// Single registration attempt: connect, discover the registry hub
/// namespace via `_info`, send `registry.register` as an upsert.
pub async fn register(reg: &RegistryRegistration) -> Result<()> {
    let client = connect(&reg.url).await?;
    let hub_ns = discover_namespace(&client).await?;
    let params = json!({
        "name": reg.name,
        "host": reg.host,
        "port": reg.port,
        "protocol": "ws",
        "version": reg.version,
        "metadata": {
            "pid": std::process::id(),
            "schema_hash": reg.schema_hash,
        },
        "source": "auto",
    });
    let events = call_registry(&client, &hub_ns, "register", params).await?;
    let confirmed = events.iter().any(|e| {
        e.get("type").and_then(Value::as_str) == Some("backend_registered")
    });
    if !confirmed {
        bail!("registry did not confirm registration (events: {events:?})");
    }
    Ok(())
}

/// Invalidate the registration (graceful-shutdown path): sends
/// `registry.deregister {name}`, which deactivates the entry without
/// deleting it.
pub async fn deregister(reg: &RegistryRegistration) -> Result<()> {
    let client = connect(&reg.url).await?;
    let hub_ns = discover_namespace(&client).await?;
    let events = call_registry(
        &client,
        &hub_ns,
        "deregister",
        json!({ "name": reg.name }),
    )
    .await?;
    let confirmed = events.iter().any(|e| {
        e.get("type").and_then(Value::as_str) == Some("backend_deregistered")
    });
    if !confirmed {
        bail!("registry did not confirm deregistration (events: {events:?})");
    }
    Ok(())
}

async fn connect(url: &str) -> Result<WsClient> {
    tokio::time::timeout(CONNECT_TIMEOUT, WsClientBuilder::default().build(url))
        .await
        .map_err(|_| anyhow!("timed out connecting to registry at {url}"))?
        .with_context(|| format!("connecting to registry at {url}"))
}

/// Discover the hub namespace serving the registry endpoint via the
/// namespace-free `_info` well-known subscription. The registry's
/// `registry.*` activation lives under this hub root (`registry-hub` for
/// the stock plexus-registry binary, but never assumed).
async fn discover_namespace(client: &WsClient) -> Result<String> {
    let mut sub: Subscription<Value> = client
        .subscribe("_info", ObjectParams::new(), "_info_unsub")
        .await
        .context("subscribing to _info at the registry endpoint")?;
    loop {
        let next = tokio::time::timeout(CALL_TIMEOUT, sub.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for _info from the registry"))?;
        let Some(item) = next else { break };
        let item = item.context("reading _info stream item")?;
        match item.get("type").and_then(Value::as_str) {
            Some("data") => {
                if let Some(name) = item
                    .get("content")
                    .and_then(|c| c.get("backend"))
                    .and_then(Value::as_str)
                {
                    return Ok(name.to_string());
                }
            }
            Some("error") => bail!(
                "_info error from registry endpoint: {}",
                item.get("message").and_then(Value::as_str).unwrap_or("unknown")
            ),
            Some("done") => break,
            _ => {}
        }
    }
    bail!("registry endpoint _info returned no backend name")
}

/// Invoke `registry.<method>` through the hub `.call` subscription and
/// collect the data payloads until the stream completes.
async fn call_registry(
    client: &WsClient,
    hub_ns: &str,
    method: &str,
    params: Value,
) -> Result<Vec<Value>> {
    let sub_method = format!("{hub_ns}.call");
    let unsub_method = format!("{hub_ns}.call_unsub");
    let mut call_params = ObjectParams::new();
    call_params
        .insert("method", format!("registry.{method}"))
        .context("encoding call method")?;
    call_params.insert("params", params).context("encoding call params")?;

    let mut sub: Subscription<Value> = client
        .subscribe(&sub_method, call_params, &unsub_method)
        .await
        .with_context(|| format!("subscribing to {sub_method}"))?;

    let mut out = Vec::new();
    loop {
        let next = tokio::time::timeout(CALL_TIMEOUT, sub.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for registry.{method} response"))?;
        let Some(item) = next else { break };
        let item = item.with_context(|| format!("reading registry.{method} stream item"))?;
        match item.get("type").and_then(Value::as_str) {
            Some("data") => {
                let content = item.get("content").cloned().unwrap_or(Value::Null);
                // RegistryEvent::Error arrives as a DATA payload with
                // {"type":"error"} — surface it as a failure.
                if content.get("type").and_then(Value::as_str) == Some("error") {
                    bail!(
                        "registry.{method} failed: {}",
                        content.get("message").and_then(Value::as_str).unwrap_or("unknown error")
                    );
                }
                out.push(content);
            }
            Some("error") => bail!(
                "registry.{method} failed: {}",
                item.get("message").and_then(Value::as_str).unwrap_or("unknown error")
            ),
            Some("done") => break,
            _ => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_url_default() {
        // Note: don't mutate the real env in tests (parallelism); only the
        // unset path is asserted here. The override path is covered by the
        // integration suite, which launches with the env var set.
        if std::env::var(REGISTRY_URL_ENV).is_err() {
            assert_eq!(registry_url_from_env(), DEFAULT_REGISTRY_URL);
        }
    }
}
