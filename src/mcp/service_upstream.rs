//! TRANSPORT-16: the bridge mounted in-process — the service in memory, as
//! the caller the credential names.
//!
//! The same `plexus_transport_mcp::McpBridge` a sidecar runs, over a
//! [`HubTransport`] instead of a socket: a `Bearer` the configured
//! [`SessionValidator`] accepts is `HubTransport::as_(hub, ctx)`, one it
//! refuses is `Unauthorized` (a 401 with the challenge at the HTTP layer),
//! and no credential is the anonymous caller — exactly what the WebSocket's
//! upgrade decides (AUTH-6). TRANSPORT-11 lifts the upgrade's identify step
//! so both read one piece of code.

use std::sync::Arc;

use plexus_core::plexus::{DynamicHub, HubTransport, SessionValidator};
use plexus_transport_mcp::upstream::{BoxFuture, Credential, DynTransport, Upstream, UpstreamError};

/// The service in this binary, reached as the caller a credential names.
pub struct ServiceUpstream {
    hub: Arc<DynamicHub>,
    validator: Option<Arc<dyn SessionValidator>>,
}

impl std::fmt::Debug for ServiceUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceUpstream").field("service", &self.hub.runtime_namespace()).field("validator", &self.validator.is_some()).finish()
    }
}

impl ServiceUpstream {
    /// `validator`: the one the WebSocket's upgrade uses, so a Bearer names
    /// the same caller on both. `None`: every Bearer is refused; only the
    /// anonymous caller gets in.
    pub fn new(hub: Arc<DynamicHub>, validator: Option<Arc<dyn SessionValidator>>) -> Self {
        Self { hub, validator }
    }
}

impl Upstream for ServiceUpstream {
    fn service(&self) -> &str {
        self.hub.runtime_namespace()
    }

    fn transport_for(&self, credential: &Credential) -> BoxFuture<'_, Result<DynTransport, UpstreamError>> {
        let credential = credential.clone();
        Box::pin(async move {
            let transport = match credential {
                Credential::Anonymous => HubTransport::anonymous(self.hub.clone()),
                Credential::Bearer(token) => {
                    let Some(validator) = &self.validator else { return Err(UpstreamError::Unauthorized) };
                    match validator.validate(&token).await {
                        Some(ctx) => HubTransport::as_(self.hub.clone(), ctx),
                        None => return Err(UpstreamError::Unauthorized),
                    }
                }
            };
            Ok(Arc::new(transport) as DynTransport)
        })
    }
}
