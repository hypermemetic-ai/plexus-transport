//! The proof that travels beside an intent.
//!
//! This crate checks a proof's shape and its binding to the enclosed intent
//! bytes. It verifies no signature: that is the credential verifier's, which
//! is handed the exact inputs from here.

use alloc::string::String;
use alloc::vec::Vec;

use super::cbor::{Fields, Head, Reader, Writer};
use super::derive;
use super::error::{Result, WireError};
use super::intent::SealedIntent;
use super::limits::{
    COSE_ALG_ES256, COSE_HEADER_ALG, COSE_HEADER_KID, ES256_SIGNATURE_BYTES,
    MAX_AUTHENTICATOR_DATA_BYTES, MAX_CLIENT_DATA_JSON_BYTES, MAX_CREDENTIAL_ID_BYTES,
    MAX_SIGN1_BYTES, MAX_USER_HANDLE_BYTES, MAX_WEBAUTHN_SIGNATURE_BYTES,
    MIN_AUTHENTICATOR_DATA_BYTES,
};
use super::types::CredentialRef;
use super::view::Object;

/// The integer labels of a proof's fields. Frozen.
pub mod label {
    /// `type`: text, `"piv-cose"` or `"webauthn"`.
    pub const TYPE: u64 = 0;
    /// `sign1`: bytes, an untagged COSE_Sign1. `piv-cose` only.
    pub const SIGN1: u64 = 1;
    /// `credential_id`: bytes. `webauthn` only.
    pub const CREDENTIAL_ID: u64 = 2;
    /// `authenticator_data`: bytes. `webauthn` only.
    pub const AUTHENTICATOR_DATA: u64 = 3;
    /// `client_data_json`: bytes, as the client produced them. `webauthn` only.
    pub const CLIENT_DATA_JSON: u64 = 4;
    /// `signature`: bytes. `webauthn` only.
    pub const SIGNATURE: u64 = 5;
    /// `user_handle`: `null` or bytes. `webauthn` only.
    pub const USER_HANDLE: u64 = 6;
    /// The largest label of the table.
    pub const MAX: u64 = 6;
}

/// A PIV command signature: ES256 in a COSE_Sign1 whose payload is the
/// enclosed intent.
///
/// Everything in the COSE_Sign1 but the signature is fixed by the intent
/// (`protected = {1: -7, 4: intent.credential}`, `unprotected = {}`,
/// `payload = intent bytes`), so the signature is all that is kept.
#[derive(Clone, PartialEq, Eq)]
pub struct PivProof {
    /// `R || S`.
    pub signature: [u8; ES256_SIGNATURE_BYTES],
}

impl core::fmt::Debug for PivProof {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PivProof({})", super::types::hex(&self.signature))
    }
}

impl PivProof {
    /// The bytes ES256 signs for `intent`: its COSE `Sig_structure`.
    #[must_use]
    pub fn signing_input(intent: &SealedIntent) -> Vec<u8> {
        derive::cose_sig_structure(&intent.intent().credential, intent.bytes())
    }

    /// The untagged COSE_Sign1 of this proof over `intent`.
    #[must_use]
    pub fn sign1(&self, intent: &SealedIntent) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(4);
        w.bytes(&derive::cose_protected(&intent.intent().credential));
        w.map(0);
        w.bytes(intent.bytes());
        w.bytes(&self.signature);
        w.into_bytes()
    }

    /// Reads a COSE_Sign1 that must be over `intent`.
    fn from_sign1(sign1: &[u8], intent: &SealedIntent) -> Result<Self> {
        if sign1.len() > MAX_SIGN1_BYTES {
            return Err(WireError::Length);
        }
        let mut r = Reader::new(sign1);
        if r.array()? != 4 {
            return Err(WireError::Length);
        }

        let protected = r.bytes()?;
        let mut p = Reader::new(protected);
        let mut f = Fields::open(&mut p)?;
        let (mut alg, mut kid) = (false, None);
        while let Some(l) = f.next(&mut p)? {
            match l {
                COSE_HEADER_ALG => match p.head()? {
                    Head::Nint(n) if n == !(COSE_ALG_ES256 as u64) => alg = true,
                    Head::Nint(_) | Head::Uint(_) | Head::Text(_) => {
                        return Err(WireError::UnknownAlgorithm)
                    }
                    _ => return Err(WireError::WrongType),
                },
                COSE_HEADER_KID => kid = Some(CredentialRef(p.bytes_exact()?)),
                _ => return Err(WireError::UnknownField),
            }
        }
        p.finish()?;
        if !alg {
            return Err(WireError::MissingField);
        }
        let kid = kid.ok_or(WireError::MissingField)?;

        match r.head()? {
            Head::Map(0) => {}
            Head::Map(_) => return Err(WireError::Unprotected),
            _ => return Err(WireError::WrongType),
        }
        if r.bytes()? != intent.bytes() {
            return Err(WireError::PayloadMismatch);
        }
        let signature = r.bytes_exact()?;
        r.finish()?;
        if kid != intent.intent().credential {
            return Err(WireError::CredentialMismatch);
        }
        Ok(Self { signature })
    }
}

/// A WebAuthn assertion. The fields are the authenticator's and the client's
/// bytes, untouched; a WebAuthn verifier checks them against
/// [`SealedIntent::webauthn_challenge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasskeyProof {
    /// The credential ID, 1 to 1023 bytes.
    pub credential_id: Vec<u8>,
    /// The authenticator data, 37 to 1024 bytes.
    pub authenticator_data: Vec<u8>,
    /// The client data JSON exactly as the client serialized it, 1 to 4096
    /// bytes.
    pub client_data_json: Vec<u8>,
    /// The assertion signature in the credential algorithm's own encoding,
    /// 1 to 512 bytes.
    pub signature: Vec<u8>,
    /// The user handle, 1 to 64 bytes, when the authenticator returned one.
    pub user_handle: Option<Vec<u8>>,
}

fn within(bytes: &[u8], min: usize, max: usize) -> Result<()> {
    if bytes.len() < min || bytes.len() > max {
        return Err(WireError::Length);
    }
    Ok(())
}

impl PasskeyProof {
    fn validate(&self) -> Result<()> {
        within(&self.credential_id, 1, MAX_CREDENTIAL_ID_BYTES)?;
        within(
            &self.authenticator_data,
            MIN_AUTHENTICATOR_DATA_BYTES,
            MAX_AUTHENTICATOR_DATA_BYTES,
        )?;
        within(&self.client_data_json, 1, MAX_CLIENT_DATA_JSON_BYTES)?;
        within(&self.signature, 1, MAX_WEBAUTHN_SIGNATURE_BYTES)?;
        if let Some(handle) = &self.user_handle {
            within(handle, 1, MAX_USER_HANDLE_BYTES)?;
        }
        Ok(())
    }
}

/// The proof of one intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofV1 {
    /// `type: "piv-cose"`.
    Piv(PivProof),
    /// `type: "webauthn"`.
    Passkey(PasskeyProof),
}

impl ProofV1 {
    /// The `type` text of this proof.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Piv(_) => "piv-cose",
            Self::Passkey(_) => "webauthn",
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Piv(_) => Ok(()),
            Self::Passkey(p) => p.validate(),
        }
    }

    pub(crate) fn write(&self, w: &mut Writer, intent: &SealedIntent) {
        match self {
            Self::Piv(piv) => {
                w.map(2);
                w.uint(label::TYPE);
                w.text(self.type_name());
                w.uint(label::SIGN1);
                w.bytes(&piv.sign1(intent));
            }
            Self::Passkey(p) => {
                w.map(6);
                w.uint(label::TYPE);
                w.text(self.type_name());
                w.uint(label::CREDENTIAL_ID);
                w.bytes(&p.credential_id);
                w.uint(label::AUTHENTICATOR_DATA);
                w.bytes(&p.authenticator_data);
                w.uint(label::CLIENT_DATA_JSON);
                w.bytes(&p.client_data_json);
                w.uint(label::SIGNATURE);
                w.bytes(&p.signature);
                w.uint(label::USER_HANDLE);
                match &p.user_handle {
                    Some(handle) => w.bytes(handle),
                    None => w.null(),
                }
            }
        }
    }

    /// Reads the proof of `intent`.
    pub(crate) fn read(r: &mut Reader<'_>, intent: &SealedIntent) -> Result<Self> {
        let mut f = Fields::open(r)?;
        f.expect(r, label::TYPE)?;
        let piv = match r.text()? {
            "piv-cose" => true,
            "webauthn" => false,
            _ => return Err(WireError::UnknownKind),
        };
        let mut sign1 = None;
        let (mut credential_id, mut authenticator_data, mut client_data_json) = (None, None, None);
        let (mut signature, mut user_handle) = (None, None);
        while let Some(l) = f.next(r)? {
            match l {
                label::SIGN1 if piv => sign1 = Some(PivProof::from_sign1(r.bytes()?, intent)?),
                label::CREDENTIAL_ID if !piv => credential_id = Some(r.bytes()?.to_vec()),
                label::AUTHENTICATOR_DATA if !piv => authenticator_data = Some(r.bytes()?.to_vec()),
                label::CLIENT_DATA_JSON if !piv => client_data_json = Some(r.bytes()?.to_vec()),
                label::SIGNATURE if !piv => signature = Some(r.bytes()?.to_vec()),
                label::USER_HANDLE if !piv => {
                    user_handle = Some(if r.at_null() {
                        r.head()?;
                        None
                    } else {
                        Some(r.bytes()?.to_vec())
                    });
                }
                l if l <= label::MAX => return Err(WireError::FieldNotAllowed),
                _ => return Err(WireError::UnknownField),
            }
        }
        let missing = WireError::MissingField;
        let proof = if piv {
            Self::Piv(sign1.ok_or(missing)?)
        } else {
            Self::Passkey(PasskeyProof {
                credential_id: credential_id.ok_or(missing)?,
                authenticator_data: authenticator_data.ok_or(missing)?,
                client_data_json: client_data_json.ok_or(missing)?,
                signature: signature.ok_or(missing)?,
                user_handle: user_handle.ok_or(missing)?,
            })
        };
        proof.validate()?;
        Ok(proof)
    }

    pub(crate) fn view(&self) -> String {
        let o = Object::new().str("type", self.type_name());
        match self {
            Self::Piv(piv) => o.hex("signature", &piv.signature).end(),
            Self::Passkey(p) => o
                .hex("credential_id", &p.credential_id)
                .hex("authenticator_data", &p.authenticator_data)
                .hex("client_data_json", &p.client_data_json)
                .hex("signature", &p.signature)
                .opt_hex("user_handle", p.user_handle.as_deref())
                .end(),
        }
    }
}
