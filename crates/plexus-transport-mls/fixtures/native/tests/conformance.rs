//! The native half of the conformance evidence, over the committed vector
//! file. The browser half is `web/run-node.mjs`; `scripts/cross-check.sh`
//! compares the two.

use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use sha2::{Digest as _, Sha256};

use mls_wire_native_fixture::vectors::{
    application_of_len, delivery_of_len, hex, intent_of_len, signing_pub, unhex, Expect, Vector,
};
use mls_wire_native_fixture::{file, report, vectors};
use plexus_transport_mls::wire::limits::{
    MAX_APPLICATION_BYTES, MAX_DELIVERY_BYTES, MAX_INTENT_BYTES, MLS_CIPHERSUITE,
};
use plexus_transport_mls::wire::{
    credential_ref, enroll_signing_input, leaf_identity, webauthn_challenge,
    webauthn_register_challenge, Action, ApplicationV1, DeliveryV1, EnrollEvidence, MlsWireFormat,
    PivProof, ProofV1, SealedIntent, Value, WireError, PROFILE_PIV, PROFILE_WEBAUTHN,
};

const COMMITTED: &str = include_str!("../../../vectors/v1.json");

fn committed() -> Vec<Vector> {
    file::read(COMMITTED).expect("vectors/v1.json")
}

fn vector(name: &str) -> Vec<u8> {
    committed()
        .into_iter()
        .find(|v| v.name == name)
        .unwrap_or_else(|| panic!("no vector {name}"))
        .bytes()
}

fn verifying_key() -> VerifyingKey {
    VerifyingKey::from_sec1_bytes(&signing_pub()).unwrap()
}

/// The vectors are frozen: the committed file is, byte for byte, what the
/// builder writes, and reading it gives back what was written.
#[test]
fn the_committed_file_is_the_builders() {
    let built = vectors::build();
    assert!(
        file::write(&built) == COMMITTED,
        "vectors/v1.json is stale: `cargo run -p mls-wire-native-fixture -- generate vectors/v1.json`"
    );
    assert_eq!(committed(), built);
}

#[test]
fn the_native_decoder_observes_what_the_file_expects() {
    let vectors = committed();
    let report = report(&vectors);
    assert!(report.mismatches.is_empty(), "{:#?}", report.mismatches);
    assert!(report
        .text
        .ends_with("summary\tvectors=108\taccepted=20\trefused=88\tdispatched=8\n"));
}

/// The requirement's four rejections each have vectors, and no refused
/// vector yields anything to dispatch.
#[test]
fn every_named_rejection_has_vectors() {
    let vectors = committed();
    let refused = |prefix: &str| {
        vectors
            .iter()
            .filter(|v| v.name.contains(prefix) && matches!(v.expect, Expect::Error(_)))
            .count()
    };
    assert!(refused("duplicate") >= 6, "duplicate fields");
    assert!(
        refused("unknown") + refused("unsupported") >= 12,
        "unknown critical values"
    );
    assert!(refused("second") >= 7, "a second method");
    assert!(refused("oversized") >= 5, "an oversized body");
    for v in &vectors {
        if let Expect::Error(code) = &v.expect {
            let observed = plexus_transport_mls::wire::conformance::observe(v.record, &v.bytes());
            assert_eq!(observed.unwrap_err().code(), code, "{}", v.name);
        }
    }
}

/// The design's example, checked against bytes and text written out by hand
/// here, not taken from the decoder or the builder.
#[test]
fn the_example_intent_is_these_bytes_and_this_meaning() {
    let bytes = vector("intent-invoke-direct");
    let h = hex(&bytes);
    // map(16), 0: "plexus.agent.intent.v1", 1: "invoke", 2: h'0011..ff'
    assert!(h.starts_with(concat!(
        "b0",
        "00",
        "76",
        "706c657875732e6167656e742e696e74656e742e7631",
        "01",
        "66",
        "696e766f6b65",
        "02",
        "50",
        "00112233445566778899aabbccddeeff",
        "03",
        "5820",
    )));
    // 7: "session/prompt", 8: "workspace/demo",
    // 9: {"prompt": [{"text": "Summarize README", "type": "text"}], "sessionId": "demo-7"},
    // 10: [], 11: null, 12: h'..'
    assert!(h.contains(concat!(
        "07",
        "6e",
        "73657373696f6e2f70726f6d7074",
        "08",
        "6e",
        "776f726b73706163652f64656d6f",
        "09",
        "a2",
        "66",
        "70726f6d7074",
        "81",
        "a2",
        "64",
        "74657874",
        "70",
        "53756d6d6172697a6520524541444d45",
        "64",
        "74797065",
        "64",
        "74657874",
        "69",
        "73657373696f6e4964",
        "66",
        "64656d6f2d37",
        "0a",
        "80",
        "0b",
        "f6",
        "0c",
        "5820",
    )));
    // 13: 1800000000, 14: 1800000060, 15: h'4e' * 32
    assert!(h.ends_with(&format!(
        "0d1a6b49d2000e1a6b49d23c0f5820{}",
        "4e".repeat(32)
    )));

    let sealed = SealedIntent::decode(&bytes).unwrap();
    assert_eq!(sealed.bytes(), bytes);
    assert_eq!(sealed.digest().0, <[u8; 32]>::from(Sha256::digest(&bytes)));
    let intent = sealed.intent();
    assert_eq!(intent.issued, 1_800_000_000);
    assert_eq!(intent.expires, 1_800_000_060);
    assert_eq!(intent.allowed_presenter, None);
    assert_eq!(intent.client, intent.reply_to);
    assert_eq!(intent.client, leaf_identity(MLS_CIPHERSUITE, &[0x22; 32]));
    assert_eq!(
        intent.credential,
        credential_ref(PROFILE_PIV, &signing_pub())
    );
    let Action::Invoke(invoke) = &intent.action else {
        panic!("an invoke");
    };
    assert_eq!(invoke.method.as_str(), "session/prompt");
    assert_eq!(invoke.resource.as_str(), "workspace/demo");
    assert_eq!(invoke.args.get("sessionId"), Some(&Value::text("demo-7")));
    assert!(invoke.attachments.is_empty());

    let id = "00112233445566778899aabbccddeeff";
    let client = hex(&intent.client.0);
    let credential = hex(&intent.credential.0);
    assert_eq!(
        intent.view(),
        format!(
            concat!(
                r#"{{"domain":"plexus.agent.intent.v1","kind":"invoke","id":"{id}","#,
                r#""principal":"{p}","credential":"{credential}","client":"{client}","#,
                r#""audience":"{a}","method":"session/prompt","resource":"workspace/demo","#,
                r#""args":{{"prompt":[{{"text":"Summarize README","type":"text"}}],"sessionId":"demo-7"}},"#,
                r#""attachments":[],"allowed_presenter":null,"reply_to":"{client}","#,
                r#""issued":1800000000,"expires":1800000060,"nonce":"{n}"}}"#
            ),
            id = id,
            p = "11".repeat(32),
            a = "aa".repeat(32),
            n = "4e".repeat(32),
            credential = credential,
            client = client,
        )
    );
}

/// Sealing the decoded meaning gives the input back, for every accepted
/// vector: one meaning, one encoding.
#[test]
fn every_accepted_vector_encodes_back_to_its_input() {
    use plexus_transport_mls::wire::conformance::Record;
    for v in committed() {
        if !matches!(v.expect, Expect::Ok { .. }) {
            continue;
        }
        let bytes = v.bytes();
        let again = match v.record {
            Record::Intent => SealedIntent::decode(&bytes)
                .unwrap()
                .intent()
                .clone()
                .seal()
                .unwrap()
                .bytes()
                .to_vec(),
            Record::Application => ApplicationV1::decode(&bytes).unwrap().encode().unwrap(),
            Record::Delivery => DeliveryV1::decode_bounded(&bytes).unwrap().encode(),
        };
        assert_eq!(hex(&again), hex(&bytes), "{}", v.name);
    }
}

/// The signing inputs this crate hands a verifier are the ones the vectors'
/// signatures were made over, by a builder that shares no code with it.
#[test]
fn the_vectors_signatures_verify_over_the_crates_signing_inputs() {
    let key = verifying_key();
    for name in [
        "application-invoke-piv",
        "application-invoke-piv-proxied",
        "application-approve-piv",
    ] {
        let ApplicationV1::Invoke(invoke) = ApplicationV1::decode(&vector(name)).unwrap() else {
            panic!("{name}: an invoke");
        };
        let ProofV1::Piv(piv) = &invoke.proof else {
            panic!("{name}: a PIV proof");
        };
        let signature = Signature::from_slice(&piv.signature).unwrap();
        key.verify(&PivProof::signing_input(&invoke.intent), &signature)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        // A changed byte of the intent is another signing input.
        let other = SealedIntent::decode(&vector("intent-approve")).unwrap();
        if other != invoke.intent {
            assert!(key
                .verify(&PivProof::signing_input(&other), &signature)
                .is_err());
        }
    }

    let ApplicationV1::Invoke(invoke) =
        ApplicationV1::decode(&vector("application-invoke-webauthn")).unwrap()
    else {
        panic!("an invoke");
    };
    let ProofV1::Passkey(passkey) = &invoke.proof else {
        panic!("a WebAuthn proof");
    };
    assert_eq!(
        invoke.intent.intent().credential,
        credential_ref(PROFILE_WEBAUTHN, &passkey.credential_id)
    );
    assert_eq!(passkey.user_handle, None);
    // The client data names the challenge this crate derives from the bytes.
    let client_data = std::str::from_utf8(&passkey.client_data_json).unwrap();
    let challenge = invoke.intent.webauthn_challenge();
    assert_eq!(challenge, webauthn_challenge(invoke.intent.bytes()));
    assert!(client_data.contains(&format!(r#""challenge":"{}""#, base64url(&challenge.0))));
    let signed = [
        &passkey.authenticator_data[..],
        &Sha256::digest(&passkey.client_data_json),
    ]
    .concat();
    key.verify(&signed, &Signature::from_der(&passkey.signature).unwrap())
        .unwrap();

    let ApplicationV1::Enroll(enroll) =
        ApplicationV1::decode(&vector("application-enroll-piv")).unwrap()
    else {
        panic!("an enrollment");
    };
    let EnrollEvidence::Piv {
        signing_pub: enrolled,
        proof,
        chain,
    } = &enroll.evidence
    else {
        panic!("PIV evidence");
    };
    assert_eq!(enrolled[..], signing_pub()[..]);
    assert_eq!(chain.len(), 2);
    let input = enroll_signing_input(
        &enroll.host,
        &enroll.pairing,
        &enroll.nonce,
        &enroll.leaf_key,
        enrolled,
    );
    key.verify(&input, &Signature::from_slice(proof).unwrap())
        .unwrap();

    let ApplicationV1::Enroll(enroll) =
        ApplicationV1::decode(&vector("application-enroll-webauthn")).unwrap()
    else {
        panic!("an enrollment");
    };
    let EnrollEvidence::Webauthn {
        client_data_json, ..
    } = &enroll.evidence
    else {
        panic!("WebAuthn evidence");
    };
    let challenge = webauthn_register_challenge(
        &enroll.host,
        &enroll.pairing,
        &enroll.nonce,
        &enroll.leaf_key,
    );
    assert!(std::str::from_utf8(client_data_json)
        .unwrap()
        .contains(&base64url(&challenge.0)));
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bits = String::new();
    for b in bytes {
        bits.push_str(&format!("{b:08b}"));
    }
    while !bits.len().is_multiple_of(6) {
        bits.push('0');
    }
    (0..bits.len())
        .step_by(6)
        .map(|i| ALPHABET[usize::from_str_radix(&bits[i..i + 6], 2).unwrap()] as char)
        .collect()
}

/// Each whole-record limit is exact: the limit is accepted, one byte more is
/// refused, and refused on its length alone.
#[test]
fn the_limits_are_exact() {
    let at = intent_of_len(MAX_INTENT_BYTES);
    assert_eq!(
        SealedIntent::decode(&at).unwrap().bytes().len(),
        MAX_INTENT_BYTES
    );
    assert_eq!(
        SealedIntent::decode(&intent_of_len(MAX_INTENT_BYTES + 1)),
        Err(WireError::IntentBytes)
    );
    assert_eq!(
        SealedIntent::decode(&vec![0xff; MAX_INTENT_BYTES + 1]),
        Err(WireError::IntentBytes)
    );

    assert!(ApplicationV1::decode(&application_of_len(MAX_APPLICATION_BYTES)).is_ok());
    assert_eq!(
        ApplicationV1::decode(&application_of_len(MAX_APPLICATION_BYTES + 1)),
        Err(WireError::ApplicationBytes)
    );
    assert_eq!(
        ApplicationV1::decode(&vec![0xff; MAX_APPLICATION_BYTES + 1]),
        Err(WireError::ApplicationBytes)
    );

    let delivery = DeliveryV1::decode_bounded(&delivery_of_len(MAX_DELIVERY_BYTES)).unwrap();
    assert_eq!(delivery.wire_format(), MlsWireFormat::PrivateMessage);
    assert_eq!(delivery.encode().len(), MAX_DELIVERY_BYTES);
    assert_eq!(
        DeliveryV1::decode_bounded(&delivery_of_len(MAX_DELIVERY_BYTES + 1)),
        Err(WireError::DeliveryBytes)
    );
    assert_eq!(
        DeliveryV1::decode_bounded(&vec![0xff; MAX_DELIVERY_BYTES + 1]),
        Err(WireError::DeliveryBytes)
    );
}

/// Any one changed byte of the example intent is refused or is a different
/// intent with a different digest: no two inputs share a meaning.
#[test]
fn no_changed_byte_keeps_the_meaning() {
    let bytes = vector("intent-invoke-direct");
    let original = SealedIntent::decode(&bytes).unwrap();
    let (mut refused, mut different) = (0, 0);
    for at in 0..bytes.len() {
        for bit in 0..8 {
            let mut changed = bytes.clone();
            changed[at] ^= 1 << bit;
            match SealedIntent::decode(&changed) {
                Err(_) => refused += 1,
                Ok(sealed) => {
                    assert_ne!(sealed.intent(), original.intent(), "byte {at} bit {bit}");
                    assert_ne!(sealed.digest(), original.digest());
                    assert_eq!(sealed.bytes(), changed);
                    different += 1;
                }
            }
        }
    }
    assert_eq!(refused + different, bytes.len() * 8);
    assert!(refused > 0 && different > 0);
    // Every prefix is refused.
    for len in 0..bytes.len() {
        assert!(SealedIntent::decode(&bytes[..len]).is_err(), "prefix {len}");
    }
}

#[test]
fn the_hex_helpers_agree() {
    assert_eq!(unhex("00ff10").unwrap(), [0x00, 0xff, 0x10]);
    assert_eq!(unhex("0"), None);
    assert_eq!(
        hex(&[0x00, 0xff, 0x10]),
        plexus_transport_mls::wire::hex(&[0x00, 0xff, 0x10])
    );
}
