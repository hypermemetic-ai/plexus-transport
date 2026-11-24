//! The signed intent: the bytes a person approves, a credential signs and an
//! executor authorizes and dispatches.
//!
//! An [`IntentV1`] is what the bytes mean; a [`SealedIntent`] is the bytes
//! together with that meaning, and can only be made by encoding an intent or
//! by strictly decoding bytes, so the two cannot drift apart. Everything
//! downstream (the proof check, the grant check, the replay claim, the
//! dispatch) takes the `SealedIntent`.

use alloc::string::String;
use alloc::vec::Vec;

use super::cbor::{Fields, Map, Reader, Writer};
use super::derive;
use super::error::{Result, WireError};
use super::limits::{
    MAX_ATTACHMENTS, MAX_ATTACHMENT_BYTES, MAX_ATTACHMENT_NAME_BYTES, MAX_INTENT_BYTES,
    MAX_INTENT_LIFETIME_SECS, MAX_MEDIA_TYPE_BYTES, MAX_METHOD_BYTES, MAX_RESOURCE_BYTES,
    MAX_SAFE_INTEGER,
};
use super::types::{CredentialRef, Digest, Fingerprint, Nonce, PrincipalRef, RequestId};
use super::view::{self, Object};

/// The `domain` of every v1 intent. Its first field, so no other record's
/// signed bytes can be read as one.
pub const INTENT_DOMAIN: &str = "plexus.agent.intent.v1";

/// The integer labels of an intent's fields. Frozen.
pub mod label {
    /// `domain`: text, [`INTENT_DOMAIN`](super::INTENT_DOMAIN).
    pub const DOMAIN: u64 = 0;
    /// `kind`: text, `"invoke"` or `"approve"`.
    pub const KIND: u64 = 1;
    /// `id`: 16 bytes.
    pub const ID: u64 = 2;
    /// `principal`: 32 bytes.
    pub const PRINCIPAL: u64 = 3;
    /// `credential`: 32 bytes.
    pub const CREDENTIAL: u64 = 4;
    /// `client`: 32 bytes.
    pub const CLIENT: u64 = 5;
    /// `audience`: 32 bytes.
    pub const AUDIENCE: u64 = 6;
    /// `method`: text. `invoke` only.
    pub const METHOD: u64 = 7;
    /// `resource`: text. `invoke` only.
    pub const RESOURCE: u64 = 8;
    /// `args`: map with text keys. `invoke` only.
    pub const ARGS: u64 = 9;
    /// `attachments`: array of manifest entries. `invoke` only.
    pub const ATTACHMENTS: u64 = 10;
    /// `allowed_presenter`: `null` or 32 bytes.
    pub const ALLOWED_PRESENTER: u64 = 11;
    /// `reply_to`: 32 bytes.
    pub const REPLY_TO: u64 = 12;
    /// `issued`: Unix seconds.
    pub const ISSUED: u64 = 13;
    /// `expires`: Unix seconds.
    pub const EXPIRES: u64 = 14;
    /// `nonce`: 32 bytes.
    pub const NONCE: u64 = 15;
    /// `parent`: 16 bytes. `approve` only.
    pub const PARENT: u64 = 16;
    /// `action_hash`: 32 bytes. `approve` only.
    pub const ACTION_HASH: u64 = 17;
    /// `decision`: text. `approve` only.
    pub const DECISION: u64 = 18;
    /// The largest label of the table.
    pub const MAX: u64 = 18;

    /// A manifest entry's `name`: text.
    pub const ATTACHMENT_NAME: u64 = 0;
    /// A manifest entry's `media_type`: text.
    pub const ATTACHMENT_MEDIA_TYPE: u64 = 1;
    /// A manifest entry's `length`: bytes of the content.
    pub const ATTACHMENT_LENGTH: u64 = 2;
    /// A manifest entry's `sha256`: 32 bytes, of the whole content.
    pub const ATTACHMENT_SHA256: u64 = 3;
}

/// The one method an intent invokes: 1 to 128 of `A-Z a-z 0-9 _ . / -`.
///
/// No space, comma or other separator is in the set, so a `method` names one
/// method or is refused.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Method(String);

impl Method {
    /// `s` as a method, or [`WireError::MethodSyntax`].
    pub fn new(s: &str) -> Result<Self> {
        let ok = !s.is_empty()
            && s.len() <= MAX_METHOD_BYTES
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'/' | b'-'));
        if ok {
            Ok(Self(String::from(s)))
        } else {
            Err(WireError::MethodSyntax)
        }
    }

    /// The method's name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The resource a method is invoked on: 1 to 256 printable ASCII characters,
/// no space. Compared as bytes; the wire gives it no structure.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Resource(String);

impl Resource {
    /// `s` as a resource, or [`WireError::ResourceSyntax`].
    pub fn new(s: &str) -> Result<Self> {
        let ok = !s.is_empty()
            && s.len() <= MAX_RESOURCE_BYTES
            && s.bytes().all(|b| b.is_ascii_graphic());
        if ok {
            Ok(Self(String::from(s)))
        } else {
            Err(WireError::ResourceSyntax)
        }
    }

    /// The resource's name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One entry of the signed attachment manifest. The content travels after the
/// intent in chunk frames; the signature covers its length and digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentEntry {
    /// A label for the approval surface: 1 to 255 bytes, no control
    /// characters. Not a path; an executor never opens it.
    pub name: String,
    /// The media type: 1 to 127 printable ASCII characters, no space.
    pub media_type: String,
    /// Bytes of the content, at most
    /// [`MAX_ATTACHMENT_BYTES`](super::limits::MAX_ATTACHMENT_BYTES).
    pub length: u64,
    /// SHA-256 of the whole content.
    pub sha256: Digest,
}

impl AttachmentEntry {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() || self.name.len() > MAX_ATTACHMENT_NAME_BYTES {
            return Err(WireError::Length);
        }
        if self.name.chars().any(char::is_control) {
            return Err(WireError::Text);
        }
        if self.media_type.is_empty() || self.media_type.len() > MAX_MEDIA_TYPE_BYTES {
            return Err(WireError::Length);
        }
        if !self.media_type.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(WireError::Text);
        }
        if self.length > MAX_ATTACHMENT_BYTES {
            return Err(WireError::Range);
        }
        Ok(())
    }

    fn write(&self, w: &mut Writer) {
        w.map(4);
        w.uint(label::ATTACHMENT_NAME);
        w.text(&self.name);
        w.uint(label::ATTACHMENT_MEDIA_TYPE);
        w.text(&self.media_type);
        w.uint(label::ATTACHMENT_LENGTH);
        w.uint(self.length);
        w.uint(label::ATTACHMENT_SHA256);
        w.bytes(&self.sha256.0);
    }

    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let mut f = Fields::open(r)?;
        let (mut name, mut media_type, mut length, mut sha256) = (None, None, None, None);
        while let Some(l) = f.next(r)? {
            match l {
                label::ATTACHMENT_NAME => name = Some(String::from(r.text()?)),
                label::ATTACHMENT_MEDIA_TYPE => media_type = Some(String::from(r.text()?)),
                label::ATTACHMENT_LENGTH => length = Some(r.uint()?),
                label::ATTACHMENT_SHA256 => sha256 = Some(Digest(r.bytes_exact()?)),
                _ => return Err(WireError::UnknownField),
            }
        }
        let entry = Self {
            name: name.ok_or(WireError::MissingField)?,
            media_type: media_type.ok_or(WireError::MissingField)?,
            length: length.ok_or(WireError::MissingField)?,
            sha256: sha256.ok_or(WireError::MissingField)?,
        };
        entry.validate()?;
        Ok(entry)
    }

    fn view(&self) -> String {
        Object::new()
            .str("name", &self.name)
            .str("media_type", &self.media_type)
            .num("length", self.length)
            .hex("sha256", &self.sha256.0)
            .end()
    }
}

/// A method call: what `kind: "invoke"` carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invoke {
    /// The one method.
    pub method: Method,
    /// The resource it is invoked on.
    pub resource: Resource,
    /// The fully materialized arguments.
    pub args: Map,
    /// The manifest of what follows in chunk frames, in order. May be empty.
    pub attachments: Vec<AttachmentEntry>,
}

/// The answer to a pending tool action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    /// `"allow_once"`: this action, this once.
    AllowOnce,
    /// `"reject_once"`: not this action, this once.
    RejectOnce,
}

impl Decision {
    /// Its text on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllowOnce => "allow_once",
            Self::RejectOnce => "reject_once",
        }
    }
}

/// A decision on one pending tool action of a running call: what
/// `kind: "approve"` carries. It names no method; the action it decides is
/// fixed by `action_hash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Approve {
    /// The call the action belongs to.
    pub parent: RequestId,
    /// SHA-256 of the exact pending action, as the executor presented it.
    pub action_hash: Digest,
    /// The decision.
    pub decision: Decision,
}

/// What an intent authorizes. Exactly one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// `kind: "invoke"`.
    Invoke(Invoke),
    /// `kind: "approve"`.
    Approve(Approve),
}

impl Action {
    /// The `kind` text of this action.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invoke(_) => "invoke",
            Self::Approve(_) => "approve",
        }
    }
}

/// The meaning of a v1 intent. Every field is required; none has a default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentV1 {
    /// The request identifier, chosen by the client.
    pub id: RequestId,
    /// The pairwise user reference the host resolves locally.
    pub principal: PrincipalRef,
    /// The enrolled credential that must have signed these bytes.
    pub credential: CredentialRef,
    /// The client MLS leaf identity the intent was made on.
    pub client: Fingerprint,
    /// The host that may execute it.
    pub audience: Fingerprint,
    /// The one application proxy that may present it, or `None` for direct
    /// delivery only.
    pub allowed_presenter: Option<Fingerprint>,
    /// The identity results are returned to.
    pub reply_to: Fingerprint,
    /// When it was approved, Unix seconds.
    pub issued: u64,
    /// When it stops being acceptable, Unix seconds. After `issued`, by at
    /// most [`MAX_INTENT_LIFETIME_SECS`](super::limits::MAX_INTENT_LIFETIME_SECS).
    pub expires: u64,
    /// The executor's single-use challenge.
    pub nonce: Nonce,
    /// What it authorizes.
    pub action: Action,
}

impl IntentV1 {
    /// Whether these values are an intent a decoder accepts.
    fn validate(&self) -> Result<()> {
        if self.issued > MAX_SAFE_INTEGER || self.expires > MAX_SAFE_INTEGER {
            return Err(WireError::Range);
        }
        if self.expires <= self.issued {
            return Err(WireError::TimeWindow);
        }
        if self.expires - self.issued > MAX_INTENT_LIFETIME_SECS {
            return Err(WireError::Lifetime);
        }
        if let Action::Invoke(invoke) = &self.action {
            invoke.args.check_depth()?;
            if invoke.attachments.len() > MAX_ATTACHMENTS {
                return Err(WireError::Attachments);
            }
            invoke
                .attachments
                .iter()
                .try_for_each(AttachmentEntry::validate)?;
        }
        Ok(())
    }

    fn write(&self, w: &mut Writer) {
        w.map(match self.action {
            Action::Invoke(_) => 16,
            Action::Approve(_) => 15,
        });
        w.uint(label::DOMAIN);
        w.text(INTENT_DOMAIN);
        w.uint(label::KIND);
        w.text(self.action.kind());
        w.uint(label::ID);
        w.bytes(&self.id.0);
        w.uint(label::PRINCIPAL);
        w.bytes(&self.principal.0);
        w.uint(label::CREDENTIAL);
        w.bytes(&self.credential.0);
        w.uint(label::CLIENT);
        w.bytes(&self.client.0);
        w.uint(label::AUDIENCE);
        w.bytes(&self.audience.0);
        if let Action::Invoke(invoke) = &self.action {
            w.uint(label::METHOD);
            w.text(invoke.method.as_str());
            w.uint(label::RESOURCE);
            w.text(invoke.resource.as_str());
            w.uint(label::ARGS);
            w.map_value(&invoke.args);
            w.uint(label::ATTACHMENTS);
            w.array(invoke.attachments.len());
            for entry in &invoke.attachments {
                entry.write(w);
            }
        }
        w.uint(label::ALLOWED_PRESENTER);
        match &self.allowed_presenter {
            Some(presenter) => w.bytes(&presenter.0),
            None => w.null(),
        }
        w.uint(label::REPLY_TO);
        w.bytes(&self.reply_to.0);
        w.uint(label::ISSUED);
        w.uint(self.issued);
        w.uint(label::EXPIRES);
        w.uint(self.expires);
        w.uint(label::NONCE);
        w.bytes(&self.nonce.0);
        if let Action::Approve(approve) = &self.action {
            w.uint(label::PARENT);
            w.bytes(&approve.parent.0);
            w.uint(label::ACTION_HASH);
            w.bytes(&approve.action_hash.0);
            w.uint(label::DECISION);
            w.text(approve.decision.as_str());
        }
    }

    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let mut f = Fields::open(r)?;
        f.expect(r, label::DOMAIN)?;
        if r.text()? != INTENT_DOMAIN {
            return Err(WireError::UnknownDomain);
        }
        f.expect(r, label::KIND)?;
        let invoke = match r.text()? {
            "invoke" => true,
            "approve" => false,
            _ => return Err(WireError::UnknownKind),
        };

        let (mut id, mut principal, mut credential, mut client, mut audience) =
            (None, None, None, None, None);
        let (mut presenter, mut reply_to, mut issued, mut expires, mut nonce) =
            (None, None, None, None, None);
        let (mut method, mut resource, mut args, mut attachments) = (None, None, None, None);
        let (mut parent, mut action_hash, mut decision) = (None, None, None);

        while let Some(l) = f.next(r)? {
            match l {
                label::ID => id = Some(RequestId(r.bytes_exact()?)),
                label::PRINCIPAL => principal = Some(PrincipalRef(r.bytes_exact()?)),
                label::CREDENTIAL => credential = Some(CredentialRef(r.bytes_exact()?)),
                label::CLIENT => client = Some(Fingerprint(r.bytes_exact()?)),
                label::AUDIENCE => audience = Some(Fingerprint(r.bytes_exact()?)),
                label::METHOD if invoke => method = Some(Method::new(r.text()?)?),
                label::RESOURCE if invoke => resource = Some(Resource::new(r.text()?)?),
                label::ARGS if invoke => args = Some(r.value_map()?),
                label::ATTACHMENTS if invoke => {
                    let n = r.array()?;
                    if n > MAX_ATTACHMENTS as u64 {
                        return Err(WireError::Attachments);
                    }
                    let mut entries = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        entries.push(AttachmentEntry::read(r)?);
                    }
                    attachments = Some(entries);
                }
                label::ALLOWED_PRESENTER => {
                    presenter = Some(if r.at_null() {
                        r.head()?;
                        None
                    } else {
                        Some(Fingerprint(r.bytes_exact()?))
                    });
                }
                label::REPLY_TO => reply_to = Some(Fingerprint(r.bytes_exact()?)),
                label::ISSUED => issued = Some(r.uint()?),
                label::EXPIRES => expires = Some(r.uint()?),
                label::NONCE => nonce = Some(Nonce(r.bytes_exact()?)),
                label::PARENT if !invoke => parent = Some(RequestId(r.bytes_exact()?)),
                label::ACTION_HASH if !invoke => action_hash = Some(Digest(r.bytes_exact()?)),
                label::DECISION if !invoke => {
                    decision = Some(match r.text()? {
                        "allow_once" => Decision::AllowOnce,
                        "reject_once" => Decision::RejectOnce,
                        _ => return Err(WireError::UnknownKind),
                    });
                }
                // A field of the other kind: a method in an approval, say.
                l if l <= label::MAX => return Err(WireError::FieldNotAllowed),
                _ => return Err(WireError::UnknownField),
            }
        }

        let missing = WireError::MissingField;
        let action = if invoke {
            Action::Invoke(Invoke {
                method: method.ok_or(missing)?,
                resource: resource.ok_or(missing)?,
                args: args.ok_or(missing)?,
                attachments: attachments.ok_or(missing)?,
            })
        } else {
            Action::Approve(Approve {
                parent: parent.ok_or(missing)?,
                action_hash: action_hash.ok_or(missing)?,
                decision: decision.ok_or(missing)?,
            })
        };
        let intent = Self {
            id: id.ok_or(missing)?,
            principal: principal.ok_or(missing)?,
            credential: credential.ok_or(missing)?,
            client: client.ok_or(missing)?,
            audience: audience.ok_or(missing)?,
            allowed_presenter: presenter.ok_or(missing)?,
            reply_to: reply_to.ok_or(missing)?,
            issued: issued.ok_or(missing)?,
            expires: expires.ok_or(missing)?,
            nonce: nonce.ok_or(missing)?,
            action,
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Encodes this intent into the bytes to approve and sign.
    ///
    /// Refuses values a decoder would refuse, so whatever is sealed here
    /// decodes at every endpoint to this same intent.
    pub fn seal(self) -> Result<SealedIntent> {
        self.validate()?;
        let mut w = Writer::new();
        self.write(&mut w);
        let bytes = w.into_bytes();
        if bytes.len() > MAX_INTENT_BYTES {
            return Err(WireError::IntentBytes);
        }
        Ok(SealedIntent {
            digest: derive::intent_digest(&bytes),
            bytes,
            intent: self,
        })
    }

    /// The canonical JSON view of this intent.
    #[must_use]
    pub fn view(&self) -> String {
        let mut o = Object::new()
            .str("domain", INTENT_DOMAIN)
            .str("kind", self.action.kind())
            .hex("id", &self.id.0)
            .hex("principal", &self.principal.0)
            .hex("credential", &self.credential.0)
            .hex("client", &self.client.0)
            .hex("audience", &self.audience.0);
        if let Action::Invoke(invoke) = &self.action {
            o = o
                .str("method", invoke.method.as_str())
                .str("resource", invoke.resource.as_str())
                .map("args", &invoke.args)
                .raw(
                    "attachments",
                    &view::array(invoke.attachments.iter().map(AttachmentEntry::view)),
                );
        }
        o = o
            .opt_hex(
                "allowed_presenter",
                self.allowed_presenter.as_ref().map(|p| &p.0[..]),
            )
            .hex("reply_to", &self.reply_to.0)
            .num("issued", self.issued)
            .num("expires", self.expires)
            .hex("nonce", &self.nonce.0);
        if let Action::Approve(approve) = &self.action {
            o = o
                .hex("parent", &approve.parent.0)
                .hex("action_hash", &approve.action_hash.0)
                .str("decision", approve.decision.as_str());
        }
        o.end()
    }
}

/// An intent and the exact bytes that say it.
///
/// Immutable. The bytes are what was approved and signed; the digest is theirs;
/// the [`IntentV1`] is what they decode to and what encodes to them. There is
/// no other way to obtain one than [`IntentV1::seal`] and
/// [`SealedIntent::decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedIntent {
    bytes: Vec<u8>,
    digest: Digest,
    intent: IntentV1,
}

impl SealedIntent {
    /// Strictly decodes `bytes` as the whole of one v1 intent.
    ///
    /// The length is judged first. The bytes must be the one deterministic
    /// encoding of the intent they decode to: the decoder encodes what it read
    /// and compares.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_INTENT_BYTES {
            return Err(WireError::IntentBytes);
        }
        let mut r = Reader::new(bytes);
        let intent = IntentV1::read(&mut r)?;
        r.finish()?;
        let mut w = Writer::new();
        intent.write(&mut w);
        if w.into_bytes() != bytes {
            return Err(WireError::NonCanonical);
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            digest: derive::intent_digest(bytes),
            intent,
        })
    }

    /// The exact bytes: what a proof signs and a forwarder passes on unchanged.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// SHA-256 of [`bytes`](Self::bytes): the `intent_hash` of stream frames
    /// and the key of the replay record.
    #[must_use]
    pub fn digest(&self) -> Digest {
        self.digest
    }

    /// What the bytes mean.
    #[must_use]
    pub fn intent(&self) -> &IntentV1 {
        &self.intent
    }

    /// The WebAuthn challenge that binds an assertion to these bytes.
    #[must_use]
    pub fn webauthn_challenge(&self) -> Digest {
        derive::webauthn_challenge(&self.bytes)
    }
}

/// The length check of a bounded text field outside the intent's own types.
pub(crate) fn bounded_text(s: &str, max: usize) -> Result<&str> {
    if s.is_empty() || s.len() > max {
        return Err(WireError::Length);
    }
    Ok(s)
}

/// A typed integer field: within 2^53 − 1.
pub(crate) fn safe_integer(n: u64) -> Result<u64> {
    if n > MAX_SAFE_INTEGER {
        return Err(WireError::Range);
    }
    Ok(n)
}
