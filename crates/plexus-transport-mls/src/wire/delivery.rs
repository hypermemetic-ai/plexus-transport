//! The delivery: the one record a relay reads.

use alloc::string::String;
use alloc::vec::Vec;

use super::cbor::{Fields, Reader, Writer};
use super::error::{Result, WireError};
use super::limits::{
    MAX_DELIVERY_BYTES, MAX_MLS_MESSAGE_BYTES, MIN_MLS_MESSAGE_BYTES, MLS_VERSION, WIRE_VERSION,
};
use super::types::Mailbox;
use super::view::Object;

/// The integer labels of a delivery's fields. Frozen.
pub mod label {
    /// `v`: 1.
    pub const V: u64 = 0;
    /// `to`: 32 bytes, the mailbox.
    pub const TO: u64 = 1;
    /// `mls`: bytes, one TLS-serialized MLSMessage.
    pub const MLS: u64 = 2;
}

/// The RFC 9420 wire format an MLSMessage declares in its first four bytes.
///
/// Read from cleartext framing: it says what the message claims to be, not
/// that it is valid. The MLS library decides that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MlsWireFormat {
    /// `mls_public_message` (1).
    PublicMessage,
    /// `mls_private_message` (2).
    PrivateMessage,
    /// `mls_welcome` (3).
    Welcome,
    /// `mls_group_info` (4).
    GroupInfo,
    /// `mls_key_package` (5).
    KeyPackage,
}

impl MlsWireFormat {
    /// Its name in RFC 9420.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicMessage => "mls_public_message",
            Self::PrivateMessage => "mls_private_message",
            Self::Welcome => "mls_welcome",
            Self::GroupInfo => "mls_group_info",
            Self::KeyPackage => "mls_key_package",
        }
    }

    fn peek(mls: &[u8]) -> Result<Self> {
        if mls.len() < MIN_MLS_MESSAGE_BYTES || mls.len() > MAX_MLS_MESSAGE_BYTES {
            return Err(WireError::Length);
        }
        if u16::from_be_bytes([mls[0], mls[1]]) != MLS_VERSION {
            return Err(WireError::MlsVersion);
        }
        match u16::from_be_bytes([mls[2], mls[3]]) {
            1 => Ok(Self::PublicMessage),
            2 => Ok(Self::PrivateMessage),
            3 => Ok(Self::Welcome),
            4 => Ok(Self::GroupInfo),
            5 => Ok(Self::KeyPackage),
            _ => Err(WireError::MlsWireFormat),
        }
    }
}

/// One relay delivery: one bounded MLS message for one mailbox.
///
/// Nothing else is in it. What the relay learns from the record is the
/// version, the mailbox and the length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryV1 {
    to: Mailbox,
    mls: Vec<u8>,
    wire_format: MlsWireFormat,
}

impl DeliveryV1 {
    /// A delivery of `mls` to `to`. Refuses bytes that do not open as an
    /// `mls10` MLSMessage of a defined wire format, or are too long.
    pub fn new(to: Mailbox, mls: Vec<u8>) -> Result<Self> {
        let wire_format = MlsWireFormat::peek(&mls)?;
        Ok(Self {
            to,
            mls,
            wire_format,
        })
    }

    /// Strictly decodes `bytes` as the whole of one v1 delivery. The length
    /// is judged before anything is read.
    pub fn decode_bounded(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_DELIVERY_BYTES {
            return Err(WireError::DeliveryBytes);
        }
        let mut r = Reader::new(bytes);
        let mut f = Fields::open(&mut r)?;
        f.expect(&mut r, label::V)?;
        if r.uint()? != WIRE_VERSION {
            return Err(WireError::UnsupportedVersion);
        }
        let (mut to, mut mls) = (None, None);
        while let Some(l) = f.next(&mut r)? {
            match l {
                label::TO => to = Some(Mailbox(r.bytes_exact()?)),
                label::MLS => mls = Some(r.bytes()?),
                _ => return Err(WireError::UnknownField),
            }
        }
        r.finish()?;
        let to = to.ok_or(WireError::MissingField)?;
        let mls = mls.ok_or(WireError::MissingField)?;
        Self::new(to, mls.to_vec())
    }

    /// The one encoding of this delivery.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.map(3);
        w.uint(label::V);
        w.uint(WIRE_VERSION);
        w.uint(label::TO);
        w.bytes(&self.to.0);
        w.uint(label::MLS);
        w.bytes(&self.mls);
        w.into_bytes()
    }

    /// The mailbox.
    #[must_use]
    pub fn to(&self) -> Mailbox {
        self.to
    }

    /// The MLSMessage bytes, for the MLS library.
    #[must_use]
    pub fn mls(&self) -> &[u8] {
        &self.mls
    }

    /// The wire format the MLSMessage declares.
    #[must_use]
    pub fn wire_format(&self) -> MlsWireFormat {
        self.wire_format
    }

    /// The canonical JSON view of this delivery.
    #[must_use]
    pub fn view(&self) -> String {
        Object::new()
            .num("v", WIRE_VERSION)
            .hex("to", &self.to.0)
            .str("mls_wire_format", self.wire_format.as_str())
            .hex("mls", &self.mls)
            .end()
    }
}
