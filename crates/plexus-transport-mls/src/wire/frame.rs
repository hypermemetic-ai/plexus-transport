//! The application frame: what one decrypted MLS application message is.

use alloc::string::String;
use alloc::vec::Vec;

use super::cbor::{Fields, Reader, Value, Writer};
use super::error::{Result, WireError};
use super::intent::{bounded_text, safe_integer, SealedIntent};
use super::limits::{
    ES256_SIGNATURE_BYTES, MAX_APPLICATION_BYTES, MAX_ATTACHMENTS, MAX_ATTACHMENT_BYTES,
    MAX_ATTESTATION_OBJECT_BYTES, MAX_CERT_BYTES, MAX_CHAIN_CERTS, MAX_CHUNK_BYTES,
    MAX_CLIENT_DATA_JSON_BYTES, MAX_CREDENTIAL_ID_BYTES, MAX_ERROR_CODE_BYTES,
    MAX_ERROR_MESSAGE_BYTES, MIN_CHAIN_CERTS, MLS_SIGNATURE_KEY_BYTES, P256_PUBLIC_KEY_BYTES,
    WIRE_VERSION,
};
use super::proof::ProofV1;
use super::types::{CredentialRef, Digest, Fingerprint, Nonce, RequestId};
use super::view::{self, Object};

/// The integer labels of an application frame's fields. One table for every
/// kind, so a label means one thing. Frozen.
pub mod label {
    /// `v`: 1.
    pub const V: u64 = 0;
    /// `kind`: text.
    pub const KIND: u64 = 1;
    /// `intent`: bytes, one whole intent.
    pub const INTENT: u64 = 2;
    /// `proof`: a proof map.
    pub const PROOF: u64 = 3;
    /// `id`: 16 bytes, the call.
    pub const ID: u64 = 4;
    /// `intent_hash`: 32 bytes, the call's intent digest.
    pub const INTENT_HASH: u64 = 5;
    /// `seq`: position in the call's stream, from 0.
    pub const SEQ: u64 = 6;
    /// `value`: an open value.
    pub const VALUE: u64 = 7;
    /// `next_seq`: the first position the sender has not received.
    pub const NEXT_SEQ: u64 = 8;
    /// `credit`: how many more items the sender will take.
    pub const CREDIT: u64 = 9;
    /// `result`: an outcome map.
    pub const RESULT: u64 = 10;
    /// `manifest_index`: which entry of the intent's attachment manifest.
    pub const MANIFEST_INDEX: u64 = 11;
    /// `offset`: where in the attachment these bytes begin.
    pub const OFFSET: u64 = 12;
    /// `bytes`: content.
    pub const BYTES: u64 = 13;
    /// `host`: 32 bytes, the host fingerprint.
    pub const HOST: u64 = 14;
    /// `pairing`: 32 bytes, the pairing transcript hash.
    pub const PAIRING: u64 = 15;
    /// `leaf_key`: 32 bytes, the MLS leaf signature public key.
    pub const LEAF_KEY: u64 = 16;
    /// `evidence`: an enrollment evidence map.
    pub const EVIDENCE: u64 = 17;
    /// `audience`: 32 bytes, the host the challenge is for.
    pub const AUDIENCE: u64 = 18;
    /// `key`: 32 bytes, the enrolled credential reference.
    pub const KEY: u64 = 19;
    /// `nonce`: 32 bytes.
    pub const NONCE: u64 = 20;
    /// `expires`: Unix seconds.
    pub const EXPIRES: u64 = 21;
    /// The largest label of the table.
    pub const MAX: u64 = 21;

    /// Evidence `profile`: text, `"piv"` or `"webauthn"`.
    pub const EVIDENCE_PROFILE: u64 = 0;
    /// Evidence `signing_pub`: 65 bytes. `piv` only.
    pub const EVIDENCE_SIGNING_PUB: u64 = 1;
    /// Evidence `chain`: array of DER certificates, leaf first. `piv` only.
    pub const EVIDENCE_CHAIN: u64 = 2;
    /// Evidence `proof`: 64 bytes. `piv` only.
    pub const EVIDENCE_PROOF: u64 = 3;
    /// Evidence `credential_id`: bytes. `webauthn` only.
    pub const EVIDENCE_CREDENTIAL_ID: u64 = 4;
    /// Evidence `attestation_object`: bytes. `webauthn` only.
    pub const EVIDENCE_ATTESTATION_OBJECT: u64 = 5;
    /// Evidence `client_data_json`: bytes. `webauthn` only.
    pub const EVIDENCE_CLIENT_DATA_JSON: u64 = 6;
    /// The largest evidence label.
    pub const EVIDENCE_MAX: u64 = 6;

    /// Outcome `status`: text, `"ok"` or `"error"`.
    pub const OUTCOME_STATUS: u64 = 0;
    /// Outcome `value`: an open value. `ok` only.
    pub const OUTCOME_VALUE: u64 = 1;
    /// Outcome `code`: text. `error` only.
    pub const OUTCOME_CODE: u64 = 2;
    /// Outcome `message`: text. `error` only.
    pub const OUTCOME_MESSAGE: u64 = 3;
    /// Outcome `data`: an open value, `null` when there is none. `error` only.
    pub const OUTCOME_DATA: u64 = 4;
    /// The largest outcome label.
    pub const OUTCOME_MAX: u64 = 4;
}

/// `kind: "invoke"`: a signed intent and its proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeV1 {
    /// The intent, bytes and meaning.
    pub intent: SealedIntent,
    /// The proof, already checked to be over those bytes in shape.
    pub proof: ProofV1,
}

/// What a credential presents to be enrolled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnrollEvidence {
    /// `profile: "piv"`.
    Piv {
        /// The P-256 signing key, SEC1 uncompressed.
        signing_pub: [u8; P256_PUBLIC_KEY_BYTES],
        /// DER certificates: the slot attestation first, then the device's
        /// attestation certificate, then any intermediates. 2 to 4.
        chain: Vec<Vec<u8>>,
        /// ES256 `R || S` over
        /// [`enroll_signing_input`](super::enroll_signing_input).
        proof: [u8; ES256_SIGNATURE_BYTES],
    },
    /// `profile: "webauthn"`.
    Webauthn {
        /// The credential ID.
        credential_id: Vec<u8>,
        /// The attestation object of the registration response.
        attestation_object: Vec<u8>,
        /// The client data JSON of the registration response.
        client_data_json: Vec<u8>,
    },
}

/// `kind: "enroll"`: a credential and an MLS leaf key bound to one pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollV1 {
    /// The host being enrolled with.
    pub host: Fingerprint,
    /// The pairing transcript hash.
    pub pairing: Digest,
    /// The client's MLS leaf signature public key.
    pub leaf_key: [u8; MLS_SIGNATURE_KEY_BYTES],
    /// The credential's evidence.
    pub evidence: EnrollEvidence,
    /// The host's enrollment nonce.
    pub nonce: Nonce,
}

/// `kind: "enroll_challenge"`: the host's nonce for one enrollment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnrollChallengeV1 {
    /// The host that issued it.
    pub audience: Fingerprint,
    /// The pairing it is for.
    pub pairing: Digest,
    /// 32 random bytes, good once.
    pub nonce: Nonce,
    /// When it stops being acceptable, Unix seconds.
    pub expires: u64,
}

/// `kind: "command_challenge"`: the host's nonce for one command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeV1 {
    /// The host that issued it.
    pub audience: Fingerprint,
    /// The enrolled credential it is issued to.
    pub key: CredentialRef,
    /// 32 random bytes, good once.
    pub nonce: Nonce,
    /// When it stops being acceptable, Unix seconds.
    pub expires: u64,
}

/// `kind: "chunk"`: bytes of one attachment of a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkV1 {
    /// The call.
    pub id: RequestId,
    /// The entry of the intent's manifest.
    pub manifest_index: u64,
    /// Where these bytes begin.
    pub offset: u64,
    /// 1 to [`MAX_CHUNK_BYTES`](super::limits::MAX_CHUNK_BYTES) bytes.
    pub bytes: Vec<u8>,
}

/// `kind: "item"`: one item of a call's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemV1 {
    /// The call.
    pub id: RequestId,
    /// The call's intent digest.
    pub intent_hash: Digest,
    /// Its position.
    pub seq: u64,
    /// The typed update.
    pub value: Value,
}

/// `kind: "resume"`: continue a stream from a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeV1 {
    /// The call.
    pub id: RequestId,
    /// The call's intent digest.
    pub intent_hash: Digest,
    /// The first position not yet received.
    pub next_seq: u64,
}

/// `kind: "cancel"`: stop a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelV1 {
    /// The call.
    pub id: RequestId,
    /// The call's intent digest.
    pub intent_hash: Digest,
}

/// `kind: "credit"`: take this many more items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditV1 {
    /// The call.
    pub id: RequestId,
    /// The call's intent digest.
    pub intent_hash: Digest,
    /// How many more.
    pub credit: u64,
}

/// How a call ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `status: "ok"`, with the typed result.
    Ok(Value),
    /// `status: "error"`.
    Error {
        /// A machine-readable code: 1 to 64 of `A-Z a-z 0-9 _ . / -`.
        code: String,
        /// Text for a person, at most 1024 bytes. May be empty.
        message: String,
        /// Typed detail; [`Value::Null`] when there is none.
        data: Value,
    },
}

/// `kind: "end"`: the last frame of a call's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndV1 {
    /// The call.
    pub id: RequestId,
    /// The call's intent digest.
    pub intent_hash: Digest,
    /// Its position: one past the last item.
    pub seq: u64,
    /// The outcome.
    pub result: Outcome,
}

/// One application frame. Exactly one kind.
// A frame is decoded, matched once and consumed; an indirection on the
// variant every command takes would buy nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplicationV1 {
    /// `"invoke"`.
    Invoke(InvokeV1),
    /// `"enroll"`.
    Enroll(EnrollV1),
    /// `"enroll_challenge"`.
    EnrollChallenge(EnrollChallengeV1),
    /// `"command_challenge"`.
    Challenge(ChallengeV1),
    /// `"chunk"`.
    Chunk(ChunkV1),
    /// `"item"`.
    Item(ItemV1),
    /// `"resume"`.
    Resume(ResumeV1),
    /// `"cancel"`.
    Cancel(CancelV1),
    /// `"credit"`.
    Credit(CreditV1),
    /// `"end"`.
    End(EndV1),
}

const fn mask(labels: &[u64]) -> u64 {
    let mut m = 0;
    let mut i = 0;
    while i < labels.len() {
        m |= 1 << labels[i];
        i += 1;
    }
    m
}

fn within(len: usize, min: usize, max: usize) -> Result<()> {
    if len < min || len > max {
        return Err(WireError::Length);
    }
    Ok(())
}

impl EnrollEvidence {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Piv {
                signing_pub, chain, ..
            } => {
                if signing_pub[0] != 0x04 {
                    return Err(WireError::KeyEncoding);
                }
                within(chain.len(), MIN_CHAIN_CERTS, MAX_CHAIN_CERTS)?;
                chain
                    .iter()
                    .try_for_each(|cert| within(cert.len(), 1, MAX_CERT_BYTES))
            }
            Self::Webauthn {
                credential_id,
                attestation_object,
                client_data_json,
            } => {
                within(credential_id.len(), 1, MAX_CREDENTIAL_ID_BYTES)?;
                within(attestation_object.len(), 1, MAX_ATTESTATION_OBJECT_BYTES)?;
                within(client_data_json.len(), 1, MAX_CLIENT_DATA_JSON_BYTES)
            }
        }
    }

    fn write(&self, w: &mut Writer) {
        match self {
            Self::Piv {
                signing_pub,
                chain,
                proof,
            } => {
                w.map(4);
                w.uint(label::EVIDENCE_PROFILE);
                w.text(super::derive::PROFILE_PIV);
                w.uint(label::EVIDENCE_SIGNING_PUB);
                w.bytes(signing_pub);
                w.uint(label::EVIDENCE_CHAIN);
                w.array(chain.len());
                for cert in chain {
                    w.bytes(cert);
                }
                w.uint(label::EVIDENCE_PROOF);
                w.bytes(proof);
            }
            Self::Webauthn {
                credential_id,
                attestation_object,
                client_data_json,
            } => {
                w.map(4);
                w.uint(label::EVIDENCE_PROFILE);
                w.text(super::derive::PROFILE_WEBAUTHN);
                w.uint(label::EVIDENCE_CREDENTIAL_ID);
                w.bytes(credential_id);
                w.uint(label::EVIDENCE_ATTESTATION_OBJECT);
                w.bytes(attestation_object);
                w.uint(label::EVIDENCE_CLIENT_DATA_JSON);
                w.bytes(client_data_json);
            }
        }
    }

    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let mut f = Fields::open(r)?;
        f.expect(r, label::EVIDENCE_PROFILE)?;
        let piv = match r.text()? {
            "piv" => true,
            "webauthn" => false,
            _ => return Err(WireError::UnknownKind),
        };
        let (mut signing_pub, mut chain, mut proof) = (None, None, None);
        let (mut credential_id, mut attestation_object, mut client_data_json) = (None, None, None);
        while let Some(l) = f.next(r)? {
            match l {
                label::EVIDENCE_SIGNING_PUB if piv => signing_pub = Some(r.bytes_exact()?),
                label::EVIDENCE_CHAIN if piv => {
                    let n = r.array()?;
                    if n > MAX_CHAIN_CERTS as u64 {
                        return Err(WireError::Length);
                    }
                    let mut certs = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        certs.push(r.bytes()?.to_vec());
                    }
                    chain = Some(certs);
                }
                label::EVIDENCE_PROOF if piv => proof = Some(r.bytes_exact()?),
                label::EVIDENCE_CREDENTIAL_ID if !piv => credential_id = Some(r.bytes()?.to_vec()),
                label::EVIDENCE_ATTESTATION_OBJECT if !piv => {
                    attestation_object = Some(r.bytes()?.to_vec());
                }
                label::EVIDENCE_CLIENT_DATA_JSON if !piv => {
                    client_data_json = Some(r.bytes()?.to_vec());
                }
                l if l <= label::EVIDENCE_MAX => return Err(WireError::FieldNotAllowed),
                _ => return Err(WireError::UnknownField),
            }
        }
        let missing = WireError::MissingField;
        Ok(if piv {
            Self::Piv {
                signing_pub: signing_pub.ok_or(missing)?,
                chain: chain.ok_or(missing)?,
                proof: proof.ok_or(missing)?,
            }
        } else {
            Self::Webauthn {
                credential_id: credential_id.ok_or(missing)?,
                attestation_object: attestation_object.ok_or(missing)?,
                client_data_json: client_data_json.ok_or(missing)?,
            }
        })
    }

    fn view(&self) -> String {
        match self {
            Self::Piv {
                signing_pub,
                chain,
                proof,
            } => Object::new()
                .str("profile", super::derive::PROFILE_PIV)
                .hex("signing_pub", signing_pub)
                .raw(
                    "chain",
                    &view::array(chain.iter().map(|c| Object::new().hex("der", c).end())),
                )
                .hex("proof", proof)
                .end(),
            Self::Webauthn {
                credential_id,
                attestation_object,
                client_data_json,
            } => Object::new()
                .str("profile", super::derive::PROFILE_WEBAUTHN)
                .hex("credential_id", credential_id)
                .hex("attestation_object", attestation_object)
                .hex("client_data_json", client_data_json)
                .end(),
        }
    }
}

fn is_error_code(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'/' | b'-'))
}

impl Outcome {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Ok(value) => value.check_depth(),
            Self::Error {
                code,
                message,
                data,
            } => {
                bounded_text(code, MAX_ERROR_CODE_BYTES)?;
                if !is_error_code(code) {
                    return Err(WireError::Text);
                }
                within(message.len(), 0, MAX_ERROR_MESSAGE_BYTES)?;
                data.check_depth()
            }
        }
    }

    fn write(&self, w: &mut Writer) {
        match self {
            Self::Ok(value) => {
                w.map(2);
                w.uint(label::OUTCOME_STATUS);
                w.text("ok");
                w.uint(label::OUTCOME_VALUE);
                w.value(value);
            }
            Self::Error {
                code,
                message,
                data,
            } => {
                w.map(4);
                w.uint(label::OUTCOME_STATUS);
                w.text("error");
                w.uint(label::OUTCOME_CODE);
                w.text(code);
                w.uint(label::OUTCOME_MESSAGE);
                w.text(message);
                w.uint(label::OUTCOME_DATA);
                w.value(data);
            }
        }
    }

    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let mut f = Fields::open(r)?;
        f.expect(r, label::OUTCOME_STATUS)?;
        let ok = match r.text()? {
            "ok" => true,
            "error" => false,
            _ => return Err(WireError::UnknownKind),
        };
        let (mut value, mut code, mut message, mut data) = (None, None, None, None);
        while let Some(l) = f.next(r)? {
            match l {
                label::OUTCOME_VALUE if ok => value = Some(r.value()?),
                label::OUTCOME_CODE if !ok => code = Some(String::from(r.text()?)),
                label::OUTCOME_MESSAGE if !ok => message = Some(String::from(r.text()?)),
                label::OUTCOME_DATA if !ok => data = Some(r.value()?),
                l if l <= label::OUTCOME_MAX => return Err(WireError::FieldNotAllowed),
                _ => return Err(WireError::UnknownField),
            }
        }
        let missing = WireError::MissingField;
        Ok(if ok {
            Self::Ok(value.ok_or(missing)?)
        } else {
            Self::Error {
                code: code.ok_or(missing)?,
                message: message.ok_or(missing)?,
                data: data.ok_or(missing)?,
            }
        })
    }

    fn view(&self) -> String {
        match self {
            Self::Ok(value) => Object::new()
                .str("status", "ok")
                .value("value", value)
                .end(),
            Self::Error {
                code,
                message,
                data,
            } => Object::new()
                .str("status", "error")
                .str("code", code)
                .str("message", message)
                .value("data", data)
                .end(),
        }
    }
}

/// Every field a frame can carry, as read.
#[derive(Default)]
struct Raw {
    intent: Option<SealedIntent>,
    proof: Option<ProofV1>,
    id: Option<RequestId>,
    intent_hash: Option<Digest>,
    seq: Option<u64>,
    value: Option<Value>,
    next_seq: Option<u64>,
    credit: Option<u64>,
    result: Option<Outcome>,
    manifest_index: Option<u64>,
    offset: Option<u64>,
    bytes: Option<Vec<u8>>,
    host: Option<Fingerprint>,
    pairing: Option<Digest>,
    leaf_key: Option<[u8; MLS_SIGNATURE_KEY_BYTES]>,
    evidence: Option<EnrollEvidence>,
    audience: Option<Fingerprint>,
    key: Option<CredentialRef>,
    nonce: Option<Nonce>,
    expires: Option<u64>,
}

impl ApplicationV1 {
    /// The `kind` text of this frame.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invoke(_) => "invoke",
            Self::Enroll(_) => "enroll",
            Self::EnrollChallenge(_) => "enroll_challenge",
            Self::Challenge(_) => "command_challenge",
            Self::Chunk(_) => "chunk",
            Self::Item(_) => "item",
            Self::Resume(_) => "resume",
            Self::Cancel(_) => "cancel",
            Self::Credit(_) => "credit",
            Self::End(_) => "end",
        }
    }

    /// The labels a frame of `kind` carries after `v` and `kind`.
    fn allowed(kind: &str) -> Result<u64> {
        use label::*;
        Ok(match kind {
            "invoke" => mask(&[INTENT, PROOF]),
            "enroll" => mask(&[HOST, PAIRING, LEAF_KEY, EVIDENCE, NONCE]),
            "enroll_challenge" => mask(&[PAIRING, AUDIENCE, NONCE, EXPIRES]),
            "command_challenge" => mask(&[AUDIENCE, KEY, NONCE, EXPIRES]),
            "chunk" => mask(&[ID, MANIFEST_INDEX, OFFSET, BYTES]),
            "item" => mask(&[ID, INTENT_HASH, SEQ, VALUE]),
            "resume" => mask(&[ID, INTENT_HASH, NEXT_SEQ]),
            "cancel" => mask(&[ID, INTENT_HASH]),
            "credit" => mask(&[ID, INTENT_HASH, CREDIT]),
            "end" => mask(&[ID, INTENT_HASH, SEQ, RESULT]),
            _ => return Err(WireError::UnknownKind),
        })
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Invoke(f) => f.proof.validate(),
            Self::Enroll(f) => f.evidence.validate(),
            Self::EnrollChallenge(f) => safe_integer(f.expires).map(drop),
            Self::Challenge(f) => safe_integer(f.expires).map(drop),
            Self::Chunk(f) => {
                if f.manifest_index >= MAX_ATTACHMENTS as u64 || f.offset >= MAX_ATTACHMENT_BYTES {
                    return Err(WireError::Range);
                }
                within(f.bytes.len(), 1, MAX_CHUNK_BYTES)?;
                if f.offset + f.bytes.len() as u64 > MAX_ATTACHMENT_BYTES {
                    return Err(WireError::Range);
                }
                Ok(())
            }
            Self::Item(f) => {
                safe_integer(f.seq)?;
                f.value.check_depth()
            }
            Self::Resume(f) => safe_integer(f.next_seq).map(drop),
            Self::Cancel(_) => Ok(()),
            Self::Credit(f) => safe_integer(f.credit).map(drop),
            Self::End(f) => {
                safe_integer(f.seq)?;
                f.result.validate()
            }
        }
    }

    fn write(&self, w: &mut Writer) {
        use label::*;
        let call = |w: &mut Writer, id: &RequestId, hash: &Digest| {
            w.uint(ID);
            w.bytes(&id.0);
            w.uint(INTENT_HASH);
            w.bytes(&hash.0);
        };
        let open = |w: &mut Writer, fields: usize| {
            w.map(2 + fields);
            w.uint(V);
            w.uint(WIRE_VERSION);
            w.uint(KIND);
            w.text(self.kind());
        };
        match self {
            Self::Invoke(f) => {
                open(w, 2);
                w.uint(INTENT);
                w.bytes(f.intent.bytes());
                w.uint(PROOF);
                f.proof.write(w, &f.intent);
            }
            Self::Enroll(f) => {
                open(w, 5);
                w.uint(HOST);
                w.bytes(&f.host.0);
                w.uint(PAIRING);
                w.bytes(&f.pairing.0);
                w.uint(LEAF_KEY);
                w.bytes(&f.leaf_key);
                w.uint(EVIDENCE);
                f.evidence.write(w);
                w.uint(NONCE);
                w.bytes(&f.nonce.0);
            }
            Self::EnrollChallenge(f) => {
                open(w, 4);
                w.uint(PAIRING);
                w.bytes(&f.pairing.0);
                w.uint(AUDIENCE);
                w.bytes(&f.audience.0);
                w.uint(NONCE);
                w.bytes(&f.nonce.0);
                w.uint(EXPIRES);
                w.uint(f.expires);
            }
            Self::Challenge(f) => {
                open(w, 4);
                w.uint(AUDIENCE);
                w.bytes(&f.audience.0);
                w.uint(KEY);
                w.bytes(&f.key.0);
                w.uint(NONCE);
                w.bytes(&f.nonce.0);
                w.uint(EXPIRES);
                w.uint(f.expires);
            }
            Self::Chunk(f) => {
                open(w, 4);
                w.uint(ID);
                w.bytes(&f.id.0);
                w.uint(MANIFEST_INDEX);
                w.uint(f.manifest_index);
                w.uint(OFFSET);
                w.uint(f.offset);
                w.uint(BYTES);
                w.bytes(&f.bytes);
            }
            Self::Item(f) => {
                open(w, 4);
                call(w, &f.id, &f.intent_hash);
                w.uint(SEQ);
                w.uint(f.seq);
                w.uint(VALUE);
                w.value(&f.value);
            }
            Self::Resume(f) => {
                open(w, 3);
                call(w, &f.id, &f.intent_hash);
                w.uint(NEXT_SEQ);
                w.uint(f.next_seq);
            }
            Self::Cancel(f) => {
                open(w, 2);
                call(w, &f.id, &f.intent_hash);
            }
            Self::Credit(f) => {
                open(w, 3);
                call(w, &f.id, &f.intent_hash);
                w.uint(CREDIT);
                w.uint(f.credit);
            }
            Self::End(f) => {
                open(w, 4);
                call(w, &f.id, &f.intent_hash);
                w.uint(SEQ);
                w.uint(f.seq);
                w.uint(RESULT);
                f.result.write(w);
            }
        }
    }

    fn read(r: &mut Reader<'_>) -> Result<Self> {
        use label::*;
        let mut f = Fields::open(r)?;
        f.expect(r, V)?;
        if r.uint()? != WIRE_VERSION {
            return Err(WireError::UnsupportedVersion);
        }
        f.expect(r, KIND)?;
        let kind = r.text()?;
        let allowed = Self::allowed(kind)?;

        let mut raw = Raw::default();
        while let Some(l) = f.next(r)? {
            if allowed & (1 << l) == 0 {
                return Err(if l <= MAX {
                    WireError::FieldNotAllowed
                } else {
                    WireError::UnknownField
                });
            }
            match l {
                INTENT => raw.intent = Some(SealedIntent::decode(r.bytes()?)?),
                PROOF => {
                    // `intent` has the smaller label, so it has been read.
                    let intent = raw.intent.as_ref().ok_or(WireError::MissingField)?;
                    raw.proof = Some(ProofV1::read(r, intent)?);
                }
                ID => raw.id = Some(RequestId(r.bytes_exact()?)),
                INTENT_HASH => raw.intent_hash = Some(Digest(r.bytes_exact()?)),
                SEQ => raw.seq = Some(r.uint()?),
                VALUE => raw.value = Some(r.value()?),
                NEXT_SEQ => raw.next_seq = Some(r.uint()?),
                CREDIT => raw.credit = Some(r.uint()?),
                RESULT => raw.result = Some(Outcome::read(r)?),
                MANIFEST_INDEX => raw.manifest_index = Some(r.uint()?),
                OFFSET => raw.offset = Some(r.uint()?),
                BYTES => raw.bytes = Some(r.bytes()?.to_vec()),
                HOST => raw.host = Some(Fingerprint(r.bytes_exact()?)),
                PAIRING => raw.pairing = Some(Digest(r.bytes_exact()?)),
                LEAF_KEY => raw.leaf_key = Some(r.bytes_exact()?),
                EVIDENCE => raw.evidence = Some(EnrollEvidence::read(r)?),
                AUDIENCE => raw.audience = Some(Fingerprint(r.bytes_exact()?)),
                KEY => raw.key = Some(CredentialRef(r.bytes_exact()?)),
                NONCE => raw.nonce = Some(Nonce(r.bytes_exact()?)),
                EXPIRES => raw.expires = Some(r.uint()?),
                _ => return Err(WireError::UnknownField),
            }
        }

        let m = WireError::MissingField;
        let frame = match kind {
            "invoke" => Self::Invoke(InvokeV1 {
                intent: raw.intent.ok_or(m)?,
                proof: raw.proof.ok_or(m)?,
            }),
            "enroll" => Self::Enroll(EnrollV1 {
                host: raw.host.ok_or(m)?,
                pairing: raw.pairing.ok_or(m)?,
                leaf_key: raw.leaf_key.ok_or(m)?,
                evidence: raw.evidence.ok_or(m)?,
                nonce: raw.nonce.ok_or(m)?,
            }),
            "enroll_challenge" => Self::EnrollChallenge(EnrollChallengeV1 {
                audience: raw.audience.ok_or(m)?,
                pairing: raw.pairing.ok_or(m)?,
                nonce: raw.nonce.ok_or(m)?,
                expires: raw.expires.ok_or(m)?,
            }),
            "command_challenge" => Self::Challenge(ChallengeV1 {
                audience: raw.audience.ok_or(m)?,
                key: raw.key.ok_or(m)?,
                nonce: raw.nonce.ok_or(m)?,
                expires: raw.expires.ok_or(m)?,
            }),
            "chunk" => Self::Chunk(ChunkV1 {
                id: raw.id.ok_or(m)?,
                manifest_index: raw.manifest_index.ok_or(m)?,
                offset: raw.offset.ok_or(m)?,
                bytes: raw.bytes.ok_or(m)?,
            }),
            "item" => Self::Item(ItemV1 {
                id: raw.id.ok_or(m)?,
                intent_hash: raw.intent_hash.ok_or(m)?,
                seq: raw.seq.ok_or(m)?,
                value: raw.value.ok_or(m)?,
            }),
            "resume" => Self::Resume(ResumeV1 {
                id: raw.id.ok_or(m)?,
                intent_hash: raw.intent_hash.ok_or(m)?,
                next_seq: raw.next_seq.ok_or(m)?,
            }),
            "cancel" => Self::Cancel(CancelV1 {
                id: raw.id.ok_or(m)?,
                intent_hash: raw.intent_hash.ok_or(m)?,
            }),
            "credit" => Self::Credit(CreditV1 {
                id: raw.id.ok_or(m)?,
                intent_hash: raw.intent_hash.ok_or(m)?,
                credit: raw.credit.ok_or(m)?,
            }),
            _ => Self::End(EndV1 {
                id: raw.id.ok_or(m)?,
                intent_hash: raw.intent_hash.ok_or(m)?,
                seq: raw.seq.ok_or(m)?,
                result: raw.result.ok_or(m)?,
            }),
        };
        frame.validate()?;
        Ok(frame)
    }

    /// Strictly decodes `bytes` as the whole of one v1 application frame.
    ///
    /// The length is judged first. An `invoke` frame's intent is decoded as
    /// [`SealedIntent::decode`] decodes it, and its proof must be over those
    /// bytes in shape; no signature is verified here.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_APPLICATION_BYTES {
            return Err(WireError::ApplicationBytes);
        }
        let mut r = Reader::new(bytes);
        let frame = Self::read(&mut r)?;
        r.finish()?;
        let mut w = Writer::new();
        frame.write(&mut w);
        if w.into_bytes() != bytes {
            return Err(WireError::NonCanonical);
        }
        Ok(frame)
    }

    /// The one encoding of this frame, if a decoder accepts it.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::new();
        self.write(&mut w);
        let bytes = w.into_bytes();
        if bytes.len() > MAX_APPLICATION_BYTES {
            return Err(WireError::ApplicationBytes);
        }
        Ok(bytes)
    }

    /// The canonical JSON view of this frame.
    #[must_use]
    pub fn view(&self) -> String {
        let o = Object::new()
            .num("v", WIRE_VERSION)
            .str("kind", self.kind());
        match self {
            Self::Invoke(f) => o
                .raw("intent", &f.intent.intent().view())
                .raw("proof", &f.proof.view()),
            Self::Enroll(f) => o
                .hex("host", &f.host.0)
                .hex("pairing", &f.pairing.0)
                .hex("leaf_key", &f.leaf_key)
                .raw("evidence", &f.evidence.view())
                .hex("nonce", &f.nonce.0),
            Self::EnrollChallenge(f) => o
                .hex("pairing", &f.pairing.0)
                .hex("audience", &f.audience.0)
                .hex("nonce", &f.nonce.0)
                .num("expires", f.expires),
            Self::Challenge(f) => o
                .hex("audience", &f.audience.0)
                .hex("key", &f.key.0)
                .hex("nonce", &f.nonce.0)
                .num("expires", f.expires),
            Self::Chunk(f) => o
                .hex("id", &f.id.0)
                .num("manifest_index", f.manifest_index)
                .num("offset", f.offset)
                .hex("bytes", &f.bytes),
            Self::Item(f) => o
                .hex("id", &f.id.0)
                .hex("intent_hash", &f.intent_hash.0)
                .num("seq", f.seq)
                .value("value", &f.value),
            Self::Resume(f) => o
                .hex("id", &f.id.0)
                .hex("intent_hash", &f.intent_hash.0)
                .num("next_seq", f.next_seq),
            Self::Cancel(f) => o.hex("id", &f.id.0).hex("intent_hash", &f.intent_hash.0),
            Self::Credit(f) => o
                .hex("id", &f.id.0)
                .hex("intent_hash", &f.intent_hash.0)
                .num("credit", f.credit),
            Self::End(f) => o
                .hex("id", &f.id.0)
                .hex("intent_hash", &f.intent_hash.0)
                .num("seq", f.seq)
                .raw("result", &f.result.view()),
        }
        .end()
    }
}
