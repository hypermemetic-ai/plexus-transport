//! RETURNS-47 · what the bridges must answer as the socket does: a method
//! that reads `reach: Reach` and one that reads `plexus_core::call::caller()`,
//! each under `auth = "required"`, and the same two `public`.

pub use plexus_client;
use plexus_core::call;
use plexus_core::plexus::{
    current_reach, AuthContext, Gate, Membership, Memberships, PlexusError, PluginSchema, Reach, Resolve, RoleTable,
    ScopeRef, SessionValidator,
};
use plexus_core::DynamicHub;
use plexus_macros::PlexusRequest;
use std::sync::{Arc, Mutex};

pub const ADA: &str = "key:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const BOB: &str = "key:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(PlexusRequest)]
pub struct Req {
    #[from_auth_context]
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct User(pub String);

/// A service's verifier: the principal, within the call's reach.
#[derive(Clone, Default)]
pub struct Access;

impl Access {
    pub async fn verify(&self, ctx: &AuthContext) -> Result<User, PlexusError> {
        let _ = current_reach()?;
        ctx.principal().map(|p| User(p.to_string())).ok_or_else(|| PlexusError::Unauthenticated("no principal".into()))
    }
}

fn held(reach: &Reach) -> String {
    reach.held_on().iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")
}

/// `who reach`, as the call's context says them; `none: …` outside one.
fn seen() -> String {
    match call::caller() {
        Ok(c) => format!("{} {}", c.auth().and_then(|a| a.principal()).map(|p| p.to_string()).unwrap_or("anonymous".into()), held(c.reach())),
        Err(e) => format!("none: {e}"),
    }
}

#[derive(Clone, Default)]
pub struct ProbeActivation {
    access: Access,
}

/// Reads `reach: Reach`.
#[plexus_macros::activation(auth = "required", request = Req, roles(viewer = ["reach", "open"]))]
impl ProbeActivation {
    /// Who, within what.
    #[plexus_macros::method]
    pub async fn reach(&self, reach: Reach, #[from_auth(self.access.verify)] user: User) -> String {
        format!("{} {}", user.0, held(&reach))
    }

    /// Anyone: the reach alone.
    #[plexus_macros::method(public)]
    pub async fn open(&self, reach: Reach) -> String {
        format!("anyone {}", held(&reach))
    }
}

pub mod whom {
    /// Reads `caller()`.
    #[allow(async_fn_in_trait)]
    pub trait Whom {
        async fn whom(&self) -> String;
        async fn seen(&self) -> String;
    }
}

#[derive(Clone, Default)]
pub struct Seer;

/// Reads `caller()`.
#[plexus_macros::activation(auth = "required", request = Req, roles(viewer = ["whom", "seen"]))]
impl whom::Whom for Seer {
    /// Who, within what — signed in.
    async fn whom(&self) -> String {
        seen()
    }

    /// Who, within what — anyone.
    #[plexus_macros::method(public)]
    async fn seen(&self) -> String {
        seen()
    }
}

/// `Bearer ada` / `Bearer bob`; anything else is no one.
pub struct Users;

#[async_trait::async_trait]
impl SessionValidator for Users {
    async fn validate(&self, token: &str) -> Option<AuthContext> {
        let p = match token {
            "ada" => ADA,
            "bob" => BOB,
            _ => return None,
        };
        Some(AuthContext::for_principal(&p.parse().unwrap(), "s1", vec![], serde_json::Value::Null))
    }
}

#[derive(Default)]
struct Members(Mutex<Vec<Membership>>);
impl Memberships for Members {
    fn held(&self, who: &plexus_auth_core::identity::Principal) -> Vec<Membership> {
        self.0.lock().unwrap().iter().filter(|m| &m.principal == who).cloned().collect()
    }
}
struct Shortcodes;
impl Resolve for Shortcodes {
    fn chain(&self, target: &str) -> Option<Vec<ScopeRef>> {
        let code = target.split_once('-').map(|(c, _)| c).unwrap_or(target);
        Some(vec![ScopeRef::named(code), ScopeRef::Everywhere])
    }
}

fn plain() -> DynamicHub {
    DynamicHub::new("svc").register(ProbeActivation::default()).register(Seer)
}

/// A gated service of both: ADA views MENGER, BOB holds nothing.
pub fn gated() -> Arc<DynamicHub> {
    let m = Arc::new(Members::default());
    m.0.lock().unwrap().push(Membership { principal: ADA.parse().unwrap(), role: plexus_auth_core::RoleName::new("viewer"), scope: ScopeRef::parse("MENGER") });
    let declared = plain().declared_roles();
    let roles = RoleTable::from_lists(declared.iter().map(|(r, g)| (r.as_str(), g.iter().map(String::as_str))));
    Arc::new(plain().with_gate(Arc::new(Gate::new(m, Arc::new(roles), Arc::new(Shortcodes)))))
}

/// The two activations' schemas, flat, as a bridge over the service lists them.
pub fn flat_schemas() -> Vec<PluginSchema> {
    use plexus_core::plexus::Activation;
    vec![ProbeActivation::default().plugin_schema(), Seer.plugin_schema()]
}
