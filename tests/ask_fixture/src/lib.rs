//! ASK-5 · a ledger that asks before it moves: `ledger.bump` adds `n` once
//! its caller has seen what would change. With no `n` it proposes with a
//! field (`n`, default 1); with `n: 0` it answers directly ("may ask" is not
//! "always asks"). The plan is the ledger's value, so a confirmed call moves
//! it and the same token sent again meets a fresh proposal.

use std::sync::{Arc, Mutex};

use async_stream::stream;
use futures::Stream;
use plexus_core::plexus::ask::{self, ConfirmToken, Field, NeedsInput, Proposal};
use plexus_core::plexus::{AuthContext, Gate, Membership, Memberships, Resolve, RoleTable, ScopeRef, SessionValidator};
use plexus_core::DynamicHub;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub use plexus_client;

pub const ADA: &str = "key:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const BOB: &str = "key:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerEvent {
    /// What `bump` would do, and the token that does it.
    NeedsInput(NeedsInput),
    /// The ledger moved.
    Bumped { from: u64, to: u64 },
}

#[derive(Clone, Default)]
pub struct LedgerActivation {
    pub value: Arc<Mutex<u64>>,
}

#[plexus_macros::activation(version = "1.0.0", description = "ASK-5: a ledger that asks before it moves", roles(viewer = ["bump"]))]
impl LedgerActivation {
    /// Adds `n` to the ledger once its caller has seen what would change.
    #[plexus_macros::method(asks, params(n = "how much to add", confirm = "the proposal's token"))]
    pub async fn bump(&self, n: Option<u64>, confirm: Option<ConfirmToken>) -> impl Stream<Item = LedgerEvent> + Send + 'static {
        let value = Arc::clone(&self.value);
        stream! {
            let from = *value.lock().unwrap();
            if n == Some(0) {
                yield LedgerEvent::Bumped { from, to: from };
                return;
            }
            let plan = json!({ "value": from });
            match (n, ask::check(confirm.as_ref(), &plan)) {
                (Some(n), Ok(())) => {
                    let to = {
                        let mut v = value.lock().unwrap();
                        *v += n;
                        *v
                    };
                    yield LedgerEvent::Bumped { from, to };
                }
                (n, _) => {
                    let summary = match n {
                        Some(n) => format!("add {n} to {from}"),
                        None => format!("add to {from}"),
                    };
                    let mut p = Proposal::new(summary).effect(json!({ "from": from }));
                    if n.is_none() {
                        p = p.field(Field::new("n").default(1).help("how much to add"));
                    }
                    yield LedgerEvent::NeedsInput(ask::propose(&plan, p));
                }
            }
        }
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
        Some(AuthContext::for_principal(&p.parse().unwrap(), "s1", vec![], Value::Null))
    }
}

struct Members;
impl Memberships for Members {
    fn held(&self, who: &plexus_auth_core::identity::Principal) -> Vec<Membership> {
        [ADA, BOB]
            .iter()
            .filter(|p| p.parse::<plexus_auth_core::identity::Principal>().ok().as_ref() == Some(who))
            .map(|p| Membership { principal: p.parse().unwrap(), role: plexus_auth_core::RoleName::new("viewer"), scope: ScopeRef::Everywhere })
            .collect()
    }
}

struct Everywhere;
impl Resolve for Everywhere {
    fn chain(&self, _target: &str) -> Option<Vec<ScopeRef>> {
        Some(vec![ScopeRef::Everywhere])
    }
}

/// The ledger in a gated service named `svc`: ADA and BOB may bump it.
pub fn gated(ledger: LedgerActivation) -> Arc<DynamicHub> {
    let plain = || DynamicHub::new("svc").register(ledger.clone());
    let declared = plain().declared_roles();
    let roles = RoleTable::from_lists(declared.iter().map(|(r, g)| (r.as_str(), g.iter().map(String::as_str))));
    Arc::new(plain().with_gate(Arc::new(Gate::new(Arc::new(Members), Arc::new(roles), Arc::new(Everywhere)))))
}
