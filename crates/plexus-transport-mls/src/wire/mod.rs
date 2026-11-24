//! Wire profile v1: the records, their limits and their one encoding.
//!
//! `docs/wire-v1.md` is the specification; `vectors/v1.json` holds the byte
//! vectors. In short:
//!
//! - [`DeliveryV1`] is all a relay reads: a mailbox and one MLS message.
//! - [`ApplicationV1`] is one decrypted MLS application message.
//! - [`SealedIntent`] is the signed command: exact bytes, their digest and
//!   their one meaning ([`IntentV1`]).
//!
//! Every record is deterministic CBOR and every decoder is strict: a byte
//! string either is the one encoding of a v1 record within the limits, or is
//! refused with a [`WireError`] before anything could be dispatched.
//!
//! ```
//! use plexus_transport_mls::wire::*;
//!
//! let intent = IntentV1 {
//!     id: RequestId([1; 16]),
//!     principal: PrincipalRef([2; 32]),
//!     credential: CredentialRef([3; 32]),
//!     client: Fingerprint([4; 32]),
//!     audience: Fingerprint([5; 32]),
//!     allowed_presenter: None,
//!     reply_to: Fingerprint([4; 32]),
//!     issued: 1_800_000_000,
//!     expires: 1_800_000_060,
//!     nonce: Nonce([6; 32]),
//!     action: Action::Invoke(Invoke {
//!         method: Method::new("session/prompt")?,
//!         resource: Resource::new("workspace/demo")?,
//!         args: Map::from_entries([("sessionId", Value::text("demo-7"))])?,
//!         attachments: vec![],
//!     }),
//! };
//! let sealed = intent.seal()?;
//! // Every endpoint reads the same meaning from the same bytes.
//! let received = SealedIntent::decode(sealed.bytes())?;
//! assert_eq!(received, sealed);
//! // One more byte is a different, refused, input.
//! let mut longer = sealed.bytes().to_vec();
//! longer.push(0);
//! assert_eq!(SealedIntent::decode(&longer), Err(WireError::TrailingBytes));
//! # Ok::<(), WireError>(())
//! ```

mod cbor;
pub mod conformance;
mod delivery;
mod derive;
mod error;
mod frame;
mod intent;
pub mod limits;
mod proof;
mod types;
mod view;

pub use cbor::{Map, Value};
pub use delivery::{DeliveryV1, MlsWireFormat};
pub use derive::{
    cose_protected, cose_sig_structure, credential_ref, enroll_signing_input, intent_digest,
    leaf_identity, webauthn_challenge, webauthn_register_challenge, CREDENTIAL_DOMAIN,
    ENROLL_DOMAIN, MLS_LEAF_DOMAIN, PROFILE_PIV, PROFILE_WEBAUTHN, WEBAUTHN_DOMAIN,
    WEBAUTHN_REGISTER_DOMAIN,
};
pub use error::{Result, WireError};
pub use frame::{
    ApplicationV1, CancelV1, ChallengeV1, ChunkV1, CreditV1, EndV1, EnrollChallengeV1,
    EnrollEvidence, EnrollV1, InvokeV1, ItemV1, Outcome, ResumeV1,
};
pub use intent::{
    Action, Approve, AttachmentEntry, Decision, IntentV1, Invoke, Method, Resource, SealedIntent,
    INTENT_DOMAIN,
};
pub use proof::{PasskeyProof, PivProof, ProofV1};
pub use types::{hex, CredentialRef, Digest, Fingerprint, Mailbox, Nonce, PrincipalRef, RequestId};

/// The frozen integer labels of each record's fields.
pub mod labels {
    /// Labels of a [`DeliveryV1`](super::DeliveryV1).
    pub mod delivery {
        pub use crate::wire::delivery::label::*;
    }
    /// Labels of an [`ApplicationV1`](super::ApplicationV1), its enrollment
    /// evidence and its outcome.
    pub mod application {
        pub use crate::wire::frame::label::*;
    }
    /// Labels of an [`IntentV1`](super::IntentV1) and its manifest entries.
    pub mod intent {
        pub use crate::wire::intent::label::*;
    }
    /// Labels of a [`ProofV1`](super::ProofV1).
    pub mod proof {
        pub use crate::wire::proof::label::*;
    }
}
