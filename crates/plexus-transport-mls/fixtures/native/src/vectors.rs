//! The v1 vectors, built from literals with the byte-level builder.
//!
//! Every identifier here is a constant chosen to be recognizable in a hex
//! dump, and every signature is made by [`TEST_SCALAR`], a private key that
//! is published and protects nothing. Certificates, attestation objects and
//! MLS messages are synthetic stand-ins: the wire profile bounds them and
//! does not read them.

use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use sha2::{Digest, Sha256};

use plexus_transport_mls::wire::conformance::{line, observe, Record};
use plexus_transport_mls::wire::limits::{
    MAX_APPLICATION_BYTES, MAX_DELIVERY_BYTES, MAX_INTENT_BYTES,
};

use crate::raw::{arr, b, boolean, head, map, neg, null, t, tmap, u, Rec};

/// The P-256 private scalar of every signature in the vectors. Public test
/// material.
pub const TEST_SCALAR: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];

const ID: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];
const APPROVAL_ID: [u8; 16] = [
    0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
];
const PRINCIPAL: [u8; 32] = [0x11; 32];
const LEAF_KEY: [u8; 32] = [0x22; 32];
const HOST: [u8; 32] = [0xaa; 32];
const PRESENTER: [u8; 32] = [0xbb; 32];
const NONCE: [u8; 32] = [0x4e; 32];
const MAILBOX: [u8; 32] = [0x6d; 32];
const PAIRING: [u8; 32] = [0x70; 32];
const PASSKEY_ID: [u8; 16] = [0xc1; 16];
const ISSUED: u64 = 1_800_000_000;
const EXPIRES: u64 = 1_800_000_060;
const RP_ID: &str = "agent.example";
const ORIGIN: &str = "https://agent.example";

/// A piece of a vector's input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// These bytes.
    Bytes(Vec<u8>),
    /// These bytes, this many times over.
    Repeat(Vec<u8>, usize),
}

/// What a decoder must make of a vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Accepted.
    Ok {
        /// Hex of the exact intent bytes carried, if an intent is carried.
        intent: Option<String>,
        /// Hex of their SHA-256.
        digest: Option<String>,
        /// The canonical JSON view.
        view: String,
    },
    /// Refused, with this code.
    Error(String),
}

/// One vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vector {
    /// Its name.
    pub name: String,
    /// The decoder it is for.
    pub record: Record,
    /// What it shows.
    pub about: String,
    /// The input, in pieces.
    pub input: Vec<Segment>,
    /// The required observation.
    pub expect: Expect,
}

impl Vector {
    /// The input, whole.
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for segment in &self.input {
            match segment {
                Segment::Bytes(bytes) => out.extend_from_slice(bytes),
                Segment::Repeat(bytes, times) => {
                    (0..*times).for_each(|_| out.extend_from_slice(bytes))
                }
            }
        }
        out
    }

    /// The line an adapter must report for this vector.
    pub fn expected_line(&self) -> String {
        match &self.expect {
            Expect::Ok {
                intent,
                digest,
                view,
            } => format!(
                "ok\t{}\t{}\t{}",
                intent.as_deref().unwrap_or("-"),
                digest.as_deref().unwrap_or("-"),
                view
            ),
            Expect::Error(code) => format!("error\t{code}"),
        }
    }
}

/// Lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Bytes of lower-case hex.
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, b)| acc | u32::from(*b) << (16 - 8 * i));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 63] as char);
        }
    }
    out
}

/// The test key.
pub fn signing_key() -> SigningKey {
    SigningKey::from_slice(&TEST_SCALAR).expect("a valid scalar")
}

/// The test key's public half: SEC1 uncompressed, 65 bytes.
pub fn signing_pub() -> Vec<u8> {
    signing_key()
        .verifying_key()
        .to_encoded_point(false)
        .as_bytes()
        .to_vec()
}

/// ES256 over `message` by the test key: `R || S`.
fn sign(message: &[u8]) -> Vec<u8> {
    let signature: Signature = signing_key().sign(message);
    signature.to_bytes().to_vec()
}

/// An `R || S` signature as the ASN.1 DER a WebAuthn ES256 assertion carries.
fn der(rs: &[u8]) -> Vec<u8> {
    let int = |half: &[u8]| {
        let mut v: Vec<u8> = half.iter().copied().skip_while(|b| *b == 0).collect();
        if v.first().is_none_or(|b| b & 0x80 != 0) {
            v.insert(0, 0);
        }
        [vec![0x02, v.len() as u8], v].concat()
    };
    let body = [int(&rs[..32]), int(&rs[32..])].concat();
    [vec![0x30, body.len() as u8], body].concat()
}

/// The PIV credential's reference:
/// SHA-256 of `["plexus.agent.credential.v1", "piv", bstr(pub)]`.
fn piv_credential() -> [u8; 32] {
    sha256(&arr(&[
        t("plexus.agent.credential.v1"),
        t("piv"),
        b(&signing_pub()),
    ]))
}

fn passkey_credential() -> [u8; 32] {
    sha256(&arr(&[
        t("plexus.agent.credential.v1"),
        t("webauthn"),
        b(&PASSKEY_ID),
    ]))
}

/// The client leaf identity:
/// SHA-256 of `["plexus.agent.mls-leaf.v1", 1, bstr(leaf_key)]`.
fn client() -> [u8; 32] {
    sha256(&arr(&[t("plexus.agent.mls-leaf.v1"), u(1), b(&LEAF_KEY)]))
}

/// The design's example command, for direct delivery.
fn invoke_intent() -> Rec {
    Rec::of(&[
        (0, t("plexus.agent.intent.v1")),
        (1, t("invoke")),
        (2, b(&ID)),
        (3, b(&PRINCIPAL)),
        (4, b(&piv_credential())),
        (5, b(&client())),
        (6, b(&HOST)),
        (7, t("session/prompt")),
        (8, t("workspace/demo")),
        (
            9,
            tmap(&[
                ("sessionId", t("demo-7")),
                (
                    "prompt",
                    arr(&[tmap(&[
                        ("type", t("text")),
                        ("text", t("Summarize README")),
                    ])]),
                ),
            ]),
        ),
        (10, arr(&[])),
        (11, null()),
        (12, b(&client())),
        (13, u(ISSUED)),
        (14, u(EXPIRES)),
        (15, b(&NONCE)),
    ])
}

/// A command for a named presenter, with every type an argument can have and
/// one attachment.
fn proxied_intent() -> Rec {
    invoke_intent()
        .with(7, t("fs/write_text_file"))
        .with(8, t("workspace/demo/notes"))
        .with(
            9,
            tmap(&[
                ("n", u(42)),
                ("neg", neg(6)),
                ("big", u(1 << 53)),
                ("min", neg(u64::MAX)),
                ("raw", b(&[0x00, 0xff])),
                ("ok", boolean(true)),
                ("none", null()),
                ("name", t("caf\u{e9} \u{2615} \"quoted\"\n")),
                ("$bytes", t("a key that begins with a dollar sign")),
                (
                    "nested",
                    tmap(&[(
                        "a",
                        arr(&[u(1), arr(&[u(2), tmap(&[("b", boolean(false))])])]),
                    )]),
                ),
                ("empty", tmap(&[])),
            ]),
        )
        .with(
            10,
            arr(&[map(&[
                (u(0), t("notes.txt")),
                (u(1), t("text/plain")),
                (u(2), u(11)),
                (u(3), b(&sha256(b"hello world"))),
            ])]),
        )
        .with(11, b(&PRESENTER))
}

/// A decision on a pending tool action of the example call.
fn approve_intent() -> Rec {
    Rec::of(&[
        (0, t("plexus.agent.intent.v1")),
        (1, t("approve")),
        (2, b(&APPROVAL_ID)),
        (3, b(&PRINCIPAL)),
        (4, b(&piv_credential())),
        (5, b(&client())),
        (6, b(&HOST)),
        (11, null()),
        (12, b(&client())),
        (13, u(ISSUED + 30)),
        (14, u(EXPIRES + 30)),
        (15, b(&[0x4f; 32])),
        (16, b(&ID)),
        (17, b(&sha256(b"the exact pending tool action"))),
        (18, t("allow_once")),
    ])
}

fn protected(kid: &[u8]) -> Vec<u8> {
    map(&[(u(1), neg(6)), (u(4), b(kid))])
}

/// The COSE `Sig_structure` of `intent` under `protected`.
fn sig_structure(protected: &[u8], intent: &[u8]) -> Vec<u8> {
    arr(&[t("Signature1"), b(protected), b(&[]), b(intent)])
}

/// The four members of the COSE_Sign1 of a PIV proof over `intent`.
fn sign1_members(intent: &[u8]) -> Vec<Vec<u8>> {
    let protected = protected(&piv_credential());
    let signature = sign(&sig_structure(&protected, intent));
    vec![b(&protected), map(&[]), b(intent), b(&signature)]
}

fn piv_proof(sign1: &[u8]) -> Vec<u8> {
    map(&[(u(0), t("piv-cose")), (u(1), b(sign1))])
}

fn invoke_frame(intent: &[u8], proof: Vec<u8>) -> Rec {
    Rec::of(&[(0, u(1)), (1, t("invoke")), (2, b(intent)), (3, proof)])
}

/// An `invoke` frame over `intent` with a true PIV proof.
fn piv_frame(intent: &[u8]) -> Rec {
    invoke_frame(intent, piv_proof(&arr(&sign1_members(intent))))
}

/// The passkey variant of the example command and its assertion.
fn passkey_frame(user_handle: Vec<u8>) -> Rec {
    let intent = invoke_intent().with(4, b(&passkey_credential())).enc();
    let challenge = sha256(&arr(&[t("plexus.agent.webauthn.v1"), b(&intent)]));
    let client_data = format!(
        r#"{{"type":"webauthn.get","challenge":"{}","origin":"{ORIGIN}","crossOrigin":false}}"#,
        base64url(&challenge)
    );
    // RP ID hash, flags UP|UV, signature counter 0.
    let authenticator_data = [&sha256(RP_ID.as_bytes())[..], &[0x05], &[0, 0, 0, 0]].concat();
    let signed = [&authenticator_data[..], &sha256(client_data.as_bytes())].concat();
    let proof = map(&[
        (u(0), t("webauthn")),
        (u(2), b(&PASSKEY_ID)),
        (u(3), b(&authenticator_data)),
        (u(4), b(client_data.as_bytes())),
        (u(5), b(&der(&sign(&signed)))),
        (u(6), user_handle),
    ]);
    invoke_frame(&intent, proof)
}

fn enroll_piv() -> Rec {
    let signing_pub = signing_pub();
    let input = arr(&[
        t("plexus.agent.enroll.v1"),
        b(&HOST),
        b(&PAIRING),
        b(&NONCE),
        b(&LEAF_KEY),
        b(&signing_pub),
    ]);
    Rec::of(&[
        (0, u(1)),
        (1, t("enroll")),
        (14, b(&HOST)),
        (15, b(&PAIRING)),
        (16, b(&LEAF_KEY)),
        (
            17,
            map(&[
                (u(0), t("piv")),
                (u(1), b(&signing_pub)),
                (
                    u(2),
                    arr(&[
                        b(b"synthetic stand-in for the slot 9c attestation certificate"),
                        b(b"synthetic stand-in for the f9 attestation certificate"),
                    ]),
                ),
                (u(3), b(&sign(&input))),
            ]),
        ),
        (20, b(&NONCE)),
    ])
}

fn enroll_webauthn() -> Rec {
    let challenge = sha256(&arr(&[
        t("plexus.agent.webauthn.register.v1"),
        b(&HOST),
        b(&PAIRING),
        b(&NONCE),
        b(&LEAF_KEY),
    ]));
    let client_data = format!(
        r#"{{"type":"webauthn.create","challenge":"{}","origin":"{ORIGIN}","crossOrigin":false}}"#,
        base64url(&challenge)
    );
    enroll_piv().with(
        17,
        map(&[
            (u(0), t("webauthn")),
            (u(4), b(&PASSKEY_ID)),
            (u(5), b(b"synthetic stand-in for an attestation object")),
            (u(6), b(client_data.as_bytes())),
        ]),
    )
}

fn call(kind: &str, digest: &[u8]) -> Rec {
    Rec::of(&[(0, u(1)), (1, t(kind)), (4, b(&ID)), (5, b(digest))])
}

fn delivery(mls: &[u8]) -> Rec {
    Rec::of(&[(0, u(1)), (1, b(&MAILBOX)), (2, b(mls))])
}

/// The opening of an `mls10` PrivateMessage and bytes that are not one.
fn mls_stand_in() -> Vec<u8> {
    let mut mls = vec![0x00, 0x01, 0x00, 0x02];
    mls.extend((0u8..60).map(|i| i.wrapping_mul(7)));
    mls
}

/// `make(n)` with `n` set so that the result is exactly `total` bytes long.
fn sized(total: usize, make: impl Fn(usize) -> Vec<u8>) -> Vec<u8> {
    let mut n = total;
    for _ in 0..4 {
        let len = make(n).len();
        if len == total {
            return make(n);
        }
        n = n + total - len;
    }
    panic!("no padding makes {total} bytes");
}

/// An otherwise valid intent of exactly `total` bytes.
pub fn intent_of_len(total: usize) -> Vec<u8> {
    sized(total, |n| {
        invoke_intent()
            .with(9, tmap(&[("pad", b(&vec![0; n]))]))
            .enc()
    })
}

/// An otherwise valid application frame of exactly `total` bytes.
pub fn application_of_len(total: usize) -> Vec<u8> {
    sized(total, |n| {
        call("item", &[0x1d; 32])
            .with(6, u(0))
            .with(7, b(&vec![0; n]))
            .enc()
    })
}

/// An otherwise valid delivery of exactly `total` bytes.
pub fn delivery_of_len(total: usize) -> Vec<u8> {
    sized(total, |n| {
        delivery(&[&[0x00, 0x01, 0x00, 0x02][..], &vec![0; n]].concat()).enc()
    })
}

/// `bytes` with its longest run of one byte, if long, written as a repeat.
fn segments(bytes: Vec<u8>) -> Vec<Segment> {
    let (mut best, mut at) = (0, 0);
    let mut i = 0;
    while i < bytes.len() {
        let run = bytes[i..].iter().take_while(|b| **b == bytes[i]).count();
        if run > best {
            (best, at) = (run, i);
        }
        i += run;
    }
    if best < 256 {
        return vec![Segment::Bytes(bytes)];
    }
    [
        Segment::Bytes(bytes[..at].to_vec()),
        Segment::Repeat(vec![bytes[at]], best),
        Segment::Bytes(bytes[at + best..].to_vec()),
    ]
    .into_iter()
    .filter(|s| !matches!(s, Segment::Bytes(b) if b.is_empty()))
    .collect()
}

struct Builder(Vec<Vector>);

impl Builder {
    /// A vector that must be accepted. What it is observed as is recorded
    /// here from the decoder and frozen by the committed file.
    fn ok(&mut self, name: &str, record: Record, about: &str, bytes: Vec<u8>) {
        let observed = observe(record, &bytes);
        let o = observed
            .as_ref()
            .unwrap_or_else(|e| panic!("{name} must be accepted: {}", line(&Err(*e))));
        self.0.push(Vector {
            name: name.into(),
            record,
            about: about.into(),
            expect: Expect::Ok {
                intent: o.intent.as_deref().map(hex),
                digest: o.digest.map(|d| hex(&d.0)),
                view: o.view.clone(),
            },
            input: segments(bytes),
        });
    }

    /// A vector that must be refused with `code`.
    fn err(&mut self, name: &str, record: Record, code: &str, about: &str, bytes: Vec<u8>) {
        self.0.push(Vector {
            name: name.into(),
            record,
            about: about.into(),
            expect: Expect::Error(code.into()),
            input: segments(bytes),
        });
    }
}

/// Every v1 vector, in file order.
pub fn build() -> Vec<Vector> {
    use Record::{Application, Delivery, Intent};
    let mut v = Builder(Vec::new());
    let base = invoke_intent();
    let intent = base.enc();
    let digest = sha256(&intent);

    // --- accepted ---------------------------------------------------------
    v.ok(
        "intent-invoke-direct",
        Intent,
        "The design's example command: session/prompt on workspace/demo, direct delivery.",
        intent.clone(),
    );
    v.ok(
        "intent-invoke-proxied",
        Intent,
        "A command for a named presenter, with one attachment and every argument type: \
         integers either side of 2^53, bytes, booleans, null, non-ASCII text, a key that \
         begins with '$', nested and empty containers.",
        proxied_intent().enc(),
    );
    v.ok(
        "intent-approve",
        Intent,
        "A decision on one pending tool action. It carries no method.",
        approve_intent().enc(),
    );
    v.ok(
        "application-invoke-piv",
        Application,
        "The example command with a PIV proof: ES256 by the test key over the COSE \
         Sig_structure, in an untagged COSE_Sign1 whose payload is the intent.",
        piv_frame(&intent).enc(),
    );
    v.ok(
        "application-invoke-piv-proxied",
        Application,
        "The proxied command with a PIV proof. A presenter forwards these bytes unchanged.",
        piv_frame(&proxied_intent().enc()).enc(),
    );
    v.ok(
        "application-approve-piv",
        Application,
        "The approval with a PIV proof: an approval is an invoke frame around an approve intent.",
        piv_frame(&approve_intent().enc()).enc(),
    );
    v.ok(
        "application-invoke-webauthn",
        Application,
        "The example command under a passkey credential, with a WebAuthn assertion whose \
         challenge is SHA-256 of [\"plexus.agent.webauthn.v1\", bstr(intent)]. UP and UV set, \
         no user handle.",
        passkey_frame(null()).enc(),
    );
    v.ok(
        "application-invoke-webauthn-user-handle",
        Application,
        "The same assertion with a user handle.",
        passkey_frame(b(&[0x75; 16])).enc(),
    );
    v.ok(
        "application-enroll-piv",
        Application,
        "A PIV enrollment: the signing key, a two-certificate chain (synthetic stand-ins) and \
         ES256 over [\"plexus.agent.enroll.v1\", host, pairing, nonce, leaf_key, signing_pub].",
        enroll_piv().enc(),
    );
    v.ok(
        "application-enroll-webauthn",
        Application,
        "A passkey enrollment: the registration response's fields (attestation object synthetic).",
        enroll_webauthn().enc(),
    );
    v.ok(
        "application-enroll-challenge",
        Application,
        "The host's nonce for one enrollment.",
        Rec::of(&[
            (0, u(1)),
            (1, t("enroll_challenge")),
            (15, b(&PAIRING)),
            (18, b(&HOST)),
            (20, b(&NONCE)),
            (21, u(EXPIRES)),
        ])
        .enc(),
    );
    let challenge = Rec::of(&[
        (0, u(1)),
        (1, t("command_challenge")),
        (18, b(&HOST)),
        (19, b(&piv_credential())),
        (20, b(&NONCE)),
        (21, u(EXPIRES)),
    ]);
    v.ok(
        "application-command-challenge",
        Application,
        "The host's single-use nonce for one command by one enrolled credential.",
        challenge.enc(),
    );
    let chunk = Rec::of(&[
        (0, u(1)),
        (1, t("chunk")),
        (4, b(&ID)),
        (11, u(0)),
        (12, u(0)),
        (13, b(b"hello world")),
    ]);
    v.ok(
        "application-chunk",
        Application,
        "The whole content of the proxied command's one attachment.",
        chunk.enc(),
    );
    let item = call("item", &digest).with(6, u(0)).with(
        7,
        tmap(&[
            ("sessionUpdate", t("agent_message_chunk")),
            (
                "content",
                tmap(&[("type", t("text")), ("text", t("The README says"))]),
            ),
        ]),
    );
    v.ok(
        "application-item",
        Application,
        "The first item of the example call's stream.",
        item.enc(),
    );
    v.ok(
        "application-resume",
        Application,
        "Continue the example call's stream from position 1.",
        call("resume", &digest).with(8, u(1)).enc(),
    );
    v.ok(
        "application-cancel",
        Application,
        "Stop the example call.",
        call("cancel", &digest).enc(),
    );
    v.ok(
        "application-credit",
        Application,
        "Take 16 more items of the example call.",
        call("credit", &digest).with(9, u(16)).enc(),
    );
    let end = call("end", &digest).with(6, u(1)).with(
        10,
        map(&[
            (u(0), t("ok")),
            (u(1), tmap(&[("stopReason", t("end_turn"))])),
        ]),
    );
    v.ok(
        "application-end-ok",
        Application,
        "The example call's terminal frame, with its typed result.",
        end.enc(),
    );
    v.ok(
        "application-end-error",
        Application,
        "A terminal frame with a typed error and no detail.",
        call("end", &digest)
            .with(6, u(0))
            .with(
                10,
                map(&[
                    (u(0), t("error")),
                    (u(2), t("grant/denied")),
                    (u(3), t("the scope does not cover workspace/demo")),
                    (u(4), null()),
                ]),
            )
            .enc(),
    );
    v.ok(
        "delivery-private-message",
        Delivery,
        "All a relay reads: version, mailbox and one MLS message (synthetic: it opens as an \
         mls10 PrivateMessage and is not one).",
        delivery(&mls_stand_in()).enc(),
    );

    // --- a duplicate field ------------------------------------------------
    v.err(
        "intent-duplicate-method",
        Intent,
        "cbor.duplicate_key",
        "The method field twice in a row, the second naming another method.",
        base.clone().after(7, u(7), t("session/cancel")).enc(),
    );
    v.err(
        "intent-duplicate-method-at-end",
        Intent,
        "cbor.duplicate_key",
        "The method field again as the last entry: a duplicate wherever it sits.",
        base.clone().then(u(7), t("session/cancel")).enc(),
    );
    v.err(
        "intent-duplicate-nonce",
        Intent,
        "cbor.duplicate_key",
        "The nonce field twice with the same value.",
        base.clone().then(u(15), b(&NONCE)).enc(),
    );
    v.err(
        "intent-args-duplicate-key",
        Intent,
        "cbor.duplicate_key",
        "An argument map with the key \"sessionId\" twice.",
        base.clone()
            .with(
                9,
                map(&[(t("sessionId"), t("demo-7")), (t("sessionId"), t("demo-8"))]),
            )
            .enc(),
    );
    v.err(
        "application-duplicate-intent",
        Application,
        "cbor.duplicate_key",
        "An invoke frame with the intent field twice.",
        piv_frame(&intent)
            .after(2, u(2), b(&proxied_intent().enc()))
            .enc(),
    );
    v.err(
        "delivery-duplicate-mls",
        Delivery,
        "cbor.duplicate_key",
        "A delivery with two MLS messages.",
        delivery(&mls_stand_in())
            .then(u(2), b(&mls_stand_in()))
            .enc(),
    );

    // --- a second method --------------------------------------------------
    v.err(
        "intent-second-method-array",
        Intent,
        "cbor.wrong_type",
        "The method field as an array of two methods.",
        base.clone()
            .with(7, arr(&[t("session/prompt"), t("session/cancel")]))
            .enc(),
    );
    v.err(
        "intent-second-method-separator",
        Intent,
        "intent.method_syntax",
        "Two method names in one text, separated by a space.",
        base.clone()
            .with(7, t("session/prompt session/cancel"))
            .enc(),
    );
    v.err(
        "intent-second-method-comma",
        Intent,
        "intent.method_syntax",
        "Two method names in one text, separated by a comma.",
        base.clone()
            .with(7, t("session/prompt,session/cancel"))
            .enc(),
    );
    v.err(
        "intent-second-method-text-key",
        Intent,
        "cbor.map_key",
        "A second method under the text key \"method\" beside label 7.",
        base.clone().then(t("method"), t("session/cancel")).enc(),
    );
    v.err(
        "intent-second-intent-appended",
        Intent,
        "cbor.trailing_bytes",
        "Two whole intents back to back: the second is trailing bytes.",
        [intent.clone(), proxied_intent().enc()].concat(),
    );
    v.err(
        "intent-approve-with-method",
        Intent,
        "wire.field_not_allowed",
        "An approval that also names a method.",
        approve_intent().with(7, t("session/prompt")).enc(),
    );
    v.err(
        "intent-invoke-with-decision",
        Intent,
        "wire.field_not_allowed",
        "A command that also carries an approval's decision.",
        base.clone().with(18, t("allow_once")).enc(),
    );
    v.err(
        "application-second-intent-array",
        Application,
        "cbor.wrong_type",
        "An invoke frame whose intent field is an array of two intents.",
        piv_frame(&intent)
            .with(2, arr(&[b(&intent), b(&proxied_intent().enc())]))
            .enc(),
    );
    v.err(
        "application-second-frame-appended",
        Application,
        "cbor.trailing_bytes",
        "Two whole frames in one application message.",
        [piv_frame(&intent).enc(), call("cancel", &digest).enc()].concat(),
    );
    v.err(
        "application-cancel-with-intent",
        Application,
        "wire.field_not_allowed",
        "A cancel control that also carries an intent.",
        call("cancel", &digest).with(2, b(&intent)).enc(),
    );

    // --- an unknown critical value ------------------------------------------
    v.err(
        "intent-unknown-field",
        Intent,
        "wire.unknown_field",
        "A field label v1 does not define (19). Every field is critical.",
        base.clone().with(19, t("x")).enc(),
    );
    v.err(
        "intent-unknown-field-high",
        Intent,
        "wire.unknown_field",
        "A field label far outside the table (1000).",
        base.clone().with(1000, null()).enc(),
    );
    v.err(
        "intent-unknown-kind",
        Intent,
        "wire.unknown_kind",
        "kind \"delegate\".",
        base.clone().with(1, t("delegate")).enc(),
    );
    v.err(
        "intent-unknown-domain",
        Intent,
        "wire.unknown_domain",
        "The domain of a later version.",
        base.clone().with(0, t("plexus.agent.intent.v2")).enc(),
    );
    v.err(
        "intent-approve-unknown-decision",
        Intent,
        "wire.unknown_kind",
        "decision \"allow_always\": v1 has no standing approval.",
        approve_intent().with(18, t("allow_always")).enc(),
    );
    v.err(
        "application-unsupported-version",
        Application,
        "wire.unsupported_version",
        "v 2.",
        piv_frame(&intent).with(0, u(2)).enc(),
    );
    v.err(
        "application-unknown-kind",
        Application,
        "wire.unknown_kind",
        "kind \"delegate\".",
        piv_frame(&intent).with(1, t("delegate")).enc(),
    );
    v.err(
        "application-unknown-field",
        Application,
        "wire.unknown_field",
        "A field label v1 does not define (22).",
        piv_frame(&intent).with(22, null()).enc(),
    );
    v.err(
        "application-proof-unknown-type",
        Application,
        "wire.unknown_kind",
        "A proof of type \"hmac\".",
        invoke_frame(&intent, map(&[(u(0), t("hmac")), (u(1), b(&[0; 32]))])).enc(),
    );
    v.err(
        "application-proof-unknown-algorithm",
        Application,
        "proof.unknown_algorithm",
        "A COSE_Sign1 whose alg is ES384 (-35).",
        {
            let mut members = sign1_members(&intent);
            members[0] = b(&map(&[(u(1), neg(34)), (u(4), b(&piv_credential()))]));
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-proof-critical-header",
        Application,
        "wire.unknown_field",
        "A COSE protected header with a crit parameter (label 2).",
        {
            let mut members = sign1_members(&intent);
            members[0] = b(&map(&[
                (u(1), neg(6)),
                (u(2), arr(&[u(4)])),
                (u(4), b(&piv_credential())),
            ]));
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-end-unknown-status",
        Application,
        "wire.unknown_kind",
        "A terminal frame whose status is \"partial\".",
        end.clone()
            .with(10, map(&[(u(0), t("partial")), (u(1), null())]))
            .enc(),
    );
    v.err(
        "application-enroll-unknown-profile",
        Application,
        "wire.unknown_kind",
        "An enrollment of profile \"totp\".",
        enroll_piv()
            .with(17, map(&[(u(0), t("totp")), (u(1), b(&[0; 20]))]))
            .enc(),
    );
    v.err(
        "delivery-unsupported-version",
        Delivery,
        "wire.unsupported_version",
        "v 2.",
        delivery(&mls_stand_in()).with(0, u(2)).enc(),
    );
    v.err(
        "delivery-unknown-field",
        Delivery,
        "wire.unknown_field",
        "A field beside the three: a relay hint, say.",
        delivery(&mls_stand_in()).with(3, t("urgent")).enc(),
    );
    v.err(
        "delivery-mls-version",
        Delivery,
        "mls.version",
        "An MLSMessage of protocol version 2.",
        delivery(&[0x00, 0x02, 0x00, 0x02, 0x00, 0x00]).enc(),
    );
    v.err(
        "delivery-mls-wire-format",
        Delivery,
        "mls.wire_format",
        "An MLSMessage of wire format 6, which RFC 9420 does not define.",
        delivery(&[0x00, 0x01, 0x00, 0x06, 0x00, 0x00]).enc(),
    );

    // --- an oversized body --------------------------------------------------
    v.err(
        "intent-oversized",
        Intent,
        "limit.intent_bytes",
        "A well-formed intent one byte over the 65536-byte limit.",
        intent_of_len(MAX_INTENT_BYTES + 1),
    );
    v.err(
        "application-oversized",
        Application,
        "limit.application_bytes",
        "A well-formed frame one byte over the 983040-byte limit.",
        application_of_len(MAX_APPLICATION_BYTES + 1),
    );
    v.err(
        "application-intent-oversized",
        Application,
        "limit.intent_bytes",
        "A frame within its limit around an intent over the intent limit.",
        invoke_frame(&intent_of_len(MAX_INTENT_BYTES + 1), piv_proof(&[0x80])).enc(),
    );
    v.err(
        "delivery-oversized",
        Delivery,
        "limit.delivery_bytes",
        "A well-formed delivery one byte over the 1048576-byte limit.",
        delivery_of_len(MAX_DELIVERY_BYTES + 1),
    );
    v.err(
        "application-chunk-oversized",
        Application,
        "wire.length",
        "A chunk of 262145 content bytes.",
        chunk.clone().with(13, b(&vec![0; 262_145])).enc(),
    );
    v.err(
        "intent-too-many-attachments",
        Intent,
        "limit.attachments",
        "A manifest of 17 entries.",
        base.clone()
            .with(
                10,
                arr(&vec![
                    map(&[
                        (u(0), t("a")),
                        (u(1), t("text/plain")),
                        (u(2), u(0)),
                        (u(3), b(&sha256(b"")))
                    ]);
                    17
                ]),
            )
            .enc(),
    );
    v.err(
        "intent-attachment-too-long",
        Intent,
        "wire.range",
        "A manifest entry declaring 64 MiB + 1 bytes.",
        proxied_intent()
            .with(
                10,
                arr(&[map(&[
                    (u(0), t("notes.txt")),
                    (u(1), t("text/plain")),
                    (u(2), u(67_108_865)),
                    (u(3), b(&sha256(b"hello world"))),
                ])]),
            )
            .enc(),
    );
    v.err(
        "intent-args-too-deep",
        Intent,
        "limit.depth",
        "Arguments nested 17 containers deep.",
        base.clone()
            .with(9, {
                let mut value = u(0);
                for _ in 0..16 {
                    value = arr(&[value]);
                }
                tmap(&[("deep", value)])
            })
            .enc(),
    );
    v.err(
        "intent-method-too-long",
        Intent,
        "intent.method_syntax",
        "A method of 129 characters.",
        base.clone().with(7, t(&"m".repeat(129))).enc(),
    );
    v.err(
        "intent-length-claims-too-much",
        Intent,
        "cbor.truncated",
        "An argument whose byte string head claims 2^64 - 1 bytes.",
        base.clone()
            .with(9, tmap(&[("x", [&[0x5b][..], &[0xff; 8]].concat())]))
            .enc(),
    );

    // --- not the one encoding -------------------------------------------------
    v.err(
        "intent-unsorted-fields",
        Intent,
        "cbor.unsorted_keys",
        "The example intent with resource (8) written before method (7).",
        {
            let mut rec = base.clone();
            rec.0.swap(7, 8);
            rec.enc()
        },
    );
    v.err(
        "intent-args-unsorted",
        Intent,
        "cbor.unsorted_keys",
        "The example arguments with \"sessionId\" before the shorter \"prompt\".",
        base.clone()
            .with(
                9,
                map(&[(t("sessionId"), t("demo-7")), (t("prompt"), arr(&[]))]),
            )
            .enc(),
    );
    v.err(
        "intent-non-minimal-integer",
        Intent,
        "cbor.non_minimal",
        "issued written in eight bytes where four hold it.",
        base.clone()
            .with(13, [&[0x1b][..], &ISSUED.to_be_bytes()].concat())
            .enc(),
    );
    v.err(
        "intent-non-minimal-length",
        Intent,
        "cbor.non_minimal",
        "The method's length written in two bytes where the head holds it.",
        base.clone()
            .with(7, [&[0x78, 14][..], b"session/prompt"].concat())
            .enc(),
    );
    v.err(
        "intent-indefinite-map",
        Intent,
        "cbor.indefinite",
        "The example intent as an indefinite-length map.",
        {
            let mut bytes = vec![0xbf];
            bytes.extend_from_slice(&intent[1..]);
            bytes.push(0xff);
            bytes
        },
    );
    v.err(
        "intent-indefinite-text",
        Intent,
        "cbor.indefinite",
        "The method as an indefinite-length text string of two chunks.",
        base.clone()
            .with(
                7,
                [&[0x7f][..], &t("session/"), &t("prompt"), &[0xff]].concat(),
            )
            .enc(),
    );
    v.err(
        "intent-args-float",
        Intent,
        "cbor.float",
        "A floating-point argument (1.5).",
        base.clone()
            .with(9, tmap(&[("temperature", vec![0xf9, 0x3e, 0x00])]))
            .enc(),
    );
    v.err(
        "intent-args-tag",
        Intent,
        "cbor.tag",
        "A tagged argument (tag 1, an epoch time).",
        base.clone()
            .with(9, tmap(&[("at", [head(6, 1), u(ISSUED)].concat())]))
            .enc(),
    );
    v.err(
        "intent-args-undefined",
        Intent,
        "cbor.simple",
        "An argument that is the simple value undefined.",
        base.clone().with(9, tmap(&[("x", vec![0xf7])])).enc(),
    );
    v.err(
        "intent-args-integer-key",
        Intent,
        "cbor.map_key",
        "An argument map with an integer key.",
        base.clone().with(9, map(&[(u(1), t("x"))])).enc(),
    );
    v.err(
        "intent-args-not-a-map",
        Intent,
        "cbor.wrong_type",
        "Arguments that are an array.",
        base.clone().with(9, arr(&[t("demo-7")])).enc(),
    );
    v.err(
        "intent-invalid-utf8",
        Intent,
        "cbor.utf8",
        "A resource that is not UTF-8.",
        base.clone().with(8, vec![0x62, 0xc3, 0x28]).enc(),
    );
    v.err(
        "intent-truncated",
        Intent,
        "cbor.truncated",
        "The example intent without its last byte.",
        intent[..intent.len() - 1].to_vec(),
    );
    v.err(
        "intent-empty",
        Intent,
        "cbor.truncated",
        "No bytes.",
        vec![],
    );
    v.err(
        "intent-tagged",
        Intent,
        "cbor.tag",
        "The example intent under the self-described CBOR tag (55799).",
        [head(6, 55799), intent.clone()].concat(),
    );
    v.err(
        "intent-padded",
        Intent,
        "cbor.trailing_bytes",
        "The example intent followed by one zero byte.",
        [intent.clone(), vec![0]].concat(),
    );

    // --- a field outside its bounds ---------------------------------------------
    v.err(
        "intent-missing-nonce",
        Intent,
        "wire.missing_field",
        "The example intent without its nonce.",
        base.clone().without(15).enc(),
    );
    v.err(
        "intent-missing-domain",
        Intent,
        "wire.missing_field",
        "The example intent without its domain: the first field is not label 0.",
        base.clone().without(0).enc(),
    );
    v.err(
        "intent-missing-allowed-presenter",
        Intent,
        "wire.missing_field",
        "No allowed_presenter field: direct delivery is an explicit null, never an absence.",
        base.clone().without(11).enc(),
    );
    v.err(
        "intent-id-length",
        Intent,
        "wire.length",
        "A 15-byte id.",
        base.clone().with(2, b(&ID[..15])).enc(),
    );
    v.err(
        "intent-nonce-length",
        Intent,
        "wire.length",
        "A 16-byte nonce.",
        base.clone().with(15, b(&NONCE[..16])).enc(),
    );
    v.err(
        "intent-expires-before-issued",
        Intent,
        "intent.time_window",
        "expires equal to issued.",
        base.clone().with(14, u(ISSUED)).enc(),
    );
    v.err(
        "intent-lifetime",
        Intent,
        "intent.lifetime",
        "expires 301 seconds after issued.",
        base.clone().with(14, u(ISSUED + 301)).enc(),
    );
    v.err(
        "intent-time-range",
        Intent,
        "wire.range",
        "Times of 2^53 and 2^53 + 60: beyond what a JavaScript number holds exactly.",
        base.clone()
            .with(13, u(1 << 53))
            .with(14, u((1 << 53) + 60))
            .enc(),
    );
    v.err(
        "intent-negative-time",
        Intent,
        "cbor.wrong_type",
        "issued -1.",
        base.clone().with(13, neg(0)).enc(),
    );
    v.err(
        "intent-resource-space",
        Intent,
        "intent.resource_syntax",
        "A resource with a space in it.",
        base.clone()
            .with(8, t("workspace/demo workspace/other"))
            .enc(),
    );
    v.err(
        "intent-method-empty",
        Intent,
        "intent.method_syntax",
        "An empty method.",
        base.clone().with(7, t("")).enc(),
    );
    v.err(
        "intent-attachment-name-control",
        Intent,
        "wire.text",
        "An attachment name with a line feed in it.",
        proxied_intent()
            .with(
                10,
                arr(&[map(&[
                    (u(0), t("notes.txt\nApproved by owner")),
                    (u(1), t("text/plain")),
                    (u(2), u(11)),
                    (u(3), b(&sha256(b"hello world"))),
                ])]),
            )
            .enc(),
    );
    v.err(
        "application-seq-range",
        Application,
        "wire.range",
        "A stream position of 2^53.",
        item.clone().with(6, u(1 << 53)).enc(),
    );
    v.err(
        "application-enroll-compressed-key",
        Application,
        "wire.length",
        "A PIV signing key as a 33-byte compressed point.",
        {
            let mut compressed = signing_pub()[..33].to_vec();
            compressed[0] = 0x02;
            let evidence = map(&[
                (u(0), t("piv")),
                (u(1), b(&compressed)),
                (u(2), arr(&[b(b"a"), b(b"b")])),
                (u(3), b(&[0; 64])),
            ]);
            enroll_piv().with(17, evidence).enc()
        },
    );
    v.err(
        "application-enroll-key-encoding",
        Application,
        "wire.key_encoding",
        "A 65-byte PIV signing key that does not begin with 0x04.",
        {
            let mut key = signing_pub();
            key[0] = 0x06;
            let evidence = map(&[
                (u(0), t("piv")),
                (u(1), b(&key)),
                (u(2), arr(&[b(b"a"), b(b"b")])),
                (u(3), b(&[0; 64])),
            ]);
            enroll_piv().with(17, evidence).enc()
        },
    );
    v.err(
        "application-enroll-short-chain",
        Application,
        "wire.length",
        "A PIV enrollment with the slot certificate alone.",
        {
            let evidence = map(&[
                (u(0), t("piv")),
                (u(1), b(&signing_pub())),
                (u(2), arr(&[b(b"a")])),
                (u(3), b(&[0; 64])),
            ]);
            enroll_piv().with(17, evidence).enc()
        },
    );
    v.err(
        "delivery-mailbox-length",
        Delivery,
        "wire.length",
        "A 16-byte mailbox.",
        delivery(&mls_stand_in()).with(1, b(&MAILBOX[..16])).enc(),
    );
    v.err(
        "delivery-mls-too-short",
        Delivery,
        "wire.length",
        "An MLS message of three bytes.",
        delivery(&[0x00, 0x01, 0x00]).enc(),
    );
    v.err(
        "delivery-text-mailbox",
        Delivery,
        "cbor.wrong_type",
        "A mailbox that is a text string: a name, not random bytes.",
        delivery(&mls_stand_in())
            .with(1, t("alice@example.com/agents/build-host"))
            .enc(),
    );

    // --- a proof that is not over the enclosed bytes ------------------------------
    v.err(
        "application-proof-payload-mismatch",
        Application,
        "proof.payload_mismatch",
        "A true COSE_Sign1 over the proxied command, beside the example command.",
        invoke_frame(
            &intent,
            piv_proof(&arr(&sign1_members(&proxied_intent().enc()))),
        )
        .enc(),
    );
    v.err(
        "application-proof-detached-payload",
        Application,
        "cbor.wrong_type",
        "A COSE_Sign1 with a nil (detached) payload.",
        {
            let mut members = sign1_members(&intent);
            members[2] = null();
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-proof-credential-mismatch",
        Application,
        "proof.credential_mismatch",
        "A COSE_Sign1 whose kid is another credential than the intent names.",
        {
            let mut members = sign1_members(&intent);
            members[0] = b(&protected(&passkey_credential()));
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-proof-unprotected-header",
        Application,
        "proof.unprotected",
        "A COSE_Sign1 with a kid in its unprotected header.",
        {
            let mut members = sign1_members(&intent);
            members[1] = map(&[(u(4), b(&passkey_credential()))]);
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-proof-tagged-sign1",
        Application,
        "cbor.tag",
        "A COSE_Sign1 under tag 18. v1 carries it untagged.",
        invoke_frame(
            &intent,
            piv_proof(&[head(6, 18), arr(&sign1_members(&intent))].concat()),
        )
        .enc(),
    );
    v.err(
        "application-proof-der-signature",
        Application,
        "wire.length",
        "A COSE_Sign1 whose signature is ASN.1 DER, not the 64 bytes R || S.",
        {
            let mut members = sign1_members(&intent);
            let rs = members[3][2..].to_vec();
            members[3] = b(&der(&rs));
            invoke_frame(&intent, piv_proof(&arr(&members))).enc()
        },
    );
    v.err(
        "application-proof-missing",
        Application,
        "wire.missing_field",
        "An invoke frame with an intent and no proof.",
        piv_frame(&intent).without(3).enc(),
    );
    v.err(
        "application-proof-without-intent",
        Application,
        "wire.missing_field",
        "An invoke frame with a proof and no intent.",
        piv_frame(&intent).without(2).enc(),
    );
    v.err(
        "application-intent-not-canonical",
        Application,
        "cbor.unsorted_keys",
        "An invoke frame whose enclosed intent has its fields out of order: the enclosed \
         bytes are held to the same rule as a bare intent.",
        {
            let mut rec = base.clone();
            rec.0.swap(7, 8);
            piv_frame(&rec.enc()).enc()
        },
    );
    v.err(
        "application-webauthn-short-authenticator-data",
        Application,
        "wire.length",
        "A WebAuthn proof with 36 bytes of authenticator data.",
        {
            let frame = passkey_frame(null());
            let intent = frame.get(2).to_vec();
            let proof = map(&[
                (u(0), t("webauthn")),
                (u(2), b(&PASSKEY_ID)),
                (u(3), b(&[0; 36])),
                (u(4), b(b"{}")),
                (u(5), b(&[0x30; 8])),
                (u(6), null()),
            ]);
            Rec::of(&[(0, u(1)), (1, t("invoke")), (2, intent), (3, proof)]).enc()
        },
    );

    v.0
}
