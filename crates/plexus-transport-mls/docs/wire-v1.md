# Wire profile v1

What a signed intent is, byte for byte, at every endpoint of the MLS agent
transport (LMLS-2; the flows are LTRANSPORT-1's Design). This document, the
constants in `src/wire/limits.rs`, the labels in `wire::labels` and the
vectors in `vectors/v1.json` are one frozen thing: a change to any of them is
a new wire version.

The profile checks shape and binding. It verifies no signature, reads no
clock, and holds no state: those belong to the credential verifiers (LMLS-4,
LMLS-5), the authority check (LMLS-6) and the replay claim (LMLS-7), which
take their inputs from here.

## 1. Encoding

Every record is one CBOR item (RFC 8949) in the core deterministic encoding of
§4.2.1, narrowed:

| Rule | Refused as |
| --- | --- |
| Integers and lengths are in their shortest form | `cbor.non_minimal` |
| Lengths are definite | `cbor.indefinite` |
| Map keys are unique | `cbor.duplicate_key` |
| Map keys are in the bytewise order of their encodings | `cbor.unsorted_keys` |
| No tags (a COSE_Sign1 is carried untagged) | `cbor.tag` |
| No floating-point numbers | `cbor.float` |
| Of the simple values, only `false`, `true`, `null` | `cbor.simple` |
| Text strings are UTF-8; they are not normalized, and compare as bytes | `cbor.utf8` |
| A record is the whole input | `cbor.trailing_bytes`, `cbor.truncated` |
| A record's map keys are unsigned integers; an open map's keys are text | `cbor.map_key` |
| Additional-information values 28–30 do not occur | `cbor.reserved` |

Under these rules a value has exactly one encoding. A decoder enforces every
rule, so the bytes it accepts are exactly the bytes an encoder writes; the
intent and application decoders also encode what they read and compare
(`wire.non_canonical`), as a second line.

A number that needs a fraction travels as text or as a scaled integer.

**Records** are maps with integer labels (§3–§6). Every field of a record is
required unless its table says which kind carries it, there are no defaults,
and every field is critical: an undefined label is `wire.unknown_field`, a
defined label in a kind that does not carry it is `wire.field_not_allowed`, an
absent one is `wire.missing_field`, and an undefined `v`, `domain`, `kind`,
`type`, `profile`, `decision`, `status` or COSE `alg` is refused
(`wire.unsupported_version`, `wire.unknown_domain`, `wire.unknown_kind`,
`proof.unknown_algorithm`). An optional value is an explicit `null`.

**Open values** (`args`, a stream `value`, a result) are any CBOR within the
rules above, with text map keys, nested at most 16 containers deep
(`limit.depth`).

**Typed integers** (times, sequence numbers, offsets, lengths, credit) are
unsigned and at most 2^53 − 1 (`wire.range`), so a browser holds each exactly.
Integers inside open values keep the full CBOR range.

Times are Unix seconds. UUIDs are their 16 bytes. Fingerprints and digests are
SHA-256, 32 bytes.

## 2. Suite, algorithms and key encodings

| | v1 |
| --- | --- |
| MLS protocol version | `mls10` (0x0001) |
| MLS ciphersuite | 0x0001, `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519`, on both ends |
| MLS leaf signature key (`leaf_key`) | Ed25519 public key, 32 bytes (RFC 8032) |
| MLS structures | the library's RFC 9420 TLS encoding, opaque here |
| PIV command signature | COSE ES256 (`alg` −7): ECDSA P-256 with SHA-256 |
| PIV signing key (`signing_pub`) | SEC1 uncompressed point, 65 bytes, `0x04 ‖ X ‖ Y` |
| ES256 signature (COSE, enrollment `proof`) | `R ‖ S`, 64 bytes, not DER |
| WebAuthn signature | the credential algorithm's own encoding, as the authenticator returned it |
| Certificates | X.509 DER |
| Digest | SHA-256 |

The command credential's algorithm is independent of the MLS leaf signature
scheme.

### Derived values

Each is, or is the SHA-256 of, a deterministic CBOR array that opens with a
domain text.

| Value | Definition |
| --- | --- |
| Intent digest (`intent_hash`) | SHA-256(intent bytes). The intent opens with its own domain. |
| Credential reference | SHA-256(`["plexus.agent.credential.v1", profile, bstr(key)]`); `key` is `signing_pub` for `"piv"`, the credential ID for `"webauthn"` |
| MLS leaf identity | SHA-256(`["plexus.agent.mls-leaf.v1", ciphersuite, bstr(leaf_key)]`) |
| COSE protected header | `{1: -7, 4: bstr(credential reference)}` |
| COSE `Sig_structure` | `["Signature1", bstr(protected), h'', bstr(intent bytes)]` (RFC 9052 §4.4), signed once by ES256 |
| WebAuthn assertion challenge | SHA-256(`["plexus.agent.webauthn.v1", bstr(intent bytes)]`) |
| WebAuthn registration challenge | SHA-256(`["plexus.agent.webauthn.register.v1", host, pairing, nonce, bstr(leaf_key)]`) |
| PIV enrollment signing input | `["plexus.agent.enroll.v1", host, pairing, nonce, bstr(leaf_key), bstr(signing_pub)]`, signed by ES256 |

A host fingerprint, a principal reference, a mailbox and a pairing transcript
hash are opaque 32-byte values here; who derives them is the channel's and the
enrollment's to say (LMLS-3, LMLS-4, LMLS-5).

## 3. Delivery

All a relay reads. `DeliveryV1::decode_bounded`.

| Label | Field | Type |
| --- | --- | --- |
| 0 | `v` | 1 |
| 1 | `to` | bytes(32), the mailbox |
| 2 | `mls` | bytes(4..=1048532), one TLS-serialized MLSMessage |

The whole delivery is at most 1 048 576 bytes (`limit.delivery_bytes`),
judged before anything is read. The MLSMessage must open with version `mls10`
(`mls.version`) and a wire format RFC 9420 defines (`mls.wire_format`); the
decoder reports which. That is a reading of cleartext framing, not a
validation: the MLS library validates.

## 4. Application frame

One decrypted MLS application message. `ApplicationV1::decode`. At most
983 040 bytes (`limit.application_bytes`), judged first; the 64 KiB under the
delivery limit is the room of MLS framing, the AEAD tag and padding. The MLS
`authenticated_data` is empty.

Every frame opens with `0: v = 1` and `1: kind`. One label table serves every
kind:

| Label | Field | Type | Kinds |
| --- | --- | --- | --- |
| 2 | `intent` | bytes, one whole intent (§5) | `invoke` |
| 3 | `proof` | proof map (§6) | `invoke` |
| 4 | `id` | bytes(16) | `chunk` `item` `resume` `cancel` `credit` `end` |
| 5 | `intent_hash` | bytes(32) | `item` `resume` `cancel` `credit` `end` |
| 6 | `seq` | integer | `item` `end` |
| 7 | `value` | open value | `item` |
| 8 | `next_seq` | integer | `resume` |
| 9 | `credit` | integer | `credit` |
| 10 | `result` | outcome map | `end` |
| 11 | `manifest_index` | integer, 0..=15 | `chunk` |
| 12 | `offset` | integer | `chunk` |
| 13 | `bytes` | bytes(1..=262144) | `chunk` |
| 14 | `host` | bytes(32) | `enroll` |
| 15 | `pairing` | bytes(32) | `enroll` `enroll_challenge` |
| 16 | `leaf_key` | bytes(32) | `enroll` |
| 17 | `evidence` | evidence map | `enroll` |
| 18 | `audience` | bytes(32) | `enroll_challenge` `command_challenge` |
| 19 | `key` | bytes(32), credential reference | `command_challenge` |
| 20 | `nonce` | bytes(32) | `enroll` `enroll_challenge` `command_challenge` |
| 21 | `expires` | integer, Unix seconds | `enroll_challenge` `command_challenge` |

A new prompt and a tool approval are both `invoke` frames: what differs is the
kind of the intent inside. A chunk's `offset + len(bytes)` is at most
67 108 864.

**Outcome** (`result`): `{0: "ok", 1: value}` or
`{0: "error", 2: code, 3: message, 4: data}`. `code` is 1–64 of
`A-Z a-z 0-9 _ . / -`; `message` is text of at most 1024 bytes; `data` is an
open value, `null` when there is none.

**Evidence**: `{0: "piv", 1: signing_pub bytes(65), 2: chain, 3: proof
bytes(64)}`, where `chain` is 2–4 DER certificates of 1–4096 bytes, the slot
attestation first; or `{0: "webauthn", 4: credential_id bytes(1..=1023),
5: attestation_object bytes(1..=16384), 6: client_data_json bytes(1..=4096)}`.

## 5. Intent

The signed bytes. `SealedIntent::decode`. At most 65 536 bytes
(`limit.intent_bytes`), judged first.

| Label | Field | Type | Kinds |
| --- | --- | --- | --- |
| 0 | `domain` | `"plexus.agent.intent.v1"` | both |
| 1 | `kind` | `"invoke"` or `"approve"` | both |
| 2 | `id` | bytes(16) | both |
| 3 | `principal` | bytes(32), pairwise user reference | both |
| 4 | `credential` | bytes(32), credential reference | both |
| 5 | `client` | bytes(32), client MLS leaf identity | both |
| 6 | `audience` | bytes(32), host fingerprint | both |
| 7 | `method` | text, 1–128 of `A-Z a-z 0-9 _ . / -` | `invoke` |
| 8 | `resource` | text, 1–256 printable ASCII, no space | `invoke` |
| 9 | `args` | open map | `invoke` |
| 10 | `attachments` | array of at most 16 manifest entries | `invoke` |
| 11 | `allowed_presenter` | `null`, or bytes(32) | both |
| 12 | `reply_to` | bytes(32) | both |
| 13 | `issued` | integer, Unix seconds | both |
| 14 | `expires` | integer, Unix seconds | both |
| 15 | `nonce` | bytes(32) | both |
| 16 | `parent` | bytes(16), the call | `approve` |
| 17 | `action_hash` | bytes(32) | `approve` |
| 18 | `decision` | `"allow_once"` or `"reject_once"` | `approve` |

`expires` is after `issued` (`intent.time_window`) by at most 300 seconds
(`intent.lifetime`). Whether either is near the present is the verifier's
question.

**Manifest entry**: `{0: name, 1: media_type, 2: length, 3: sha256}`. `name`
is 1–255 bytes of text without control characters, a label and never a path;
`media_type` is 1–127 printable ASCII; `length` is at most 67 108 864;
`sha256` is of the whole content.

An intent names one method or none. The method character set has no
separator; a second `method` entry is a duplicate key; a `method` that is an
array is the wrong type; a second intent after the first is trailing bytes; an
approval carries no method at all.

## 6. Proof

`{0: "piv-cose", 1: sign1}` or `{0: "webauthn", 2: credential_id,
3: authenticator_data, 4: client_data_json, 5: signature, 6: user_handle}`.

**`piv-cose`.** `sign1` is the bytes (at most 65 792) of an untagged
COSE_Sign1 `[bstr(protected), {}, bstr(payload), bstr(signature)]`. The
decoder requires `protected` to be exactly `{1: -7, 4: kid}`, the unprotected
header to be empty (`proof.unprotected`), `payload` to be present and equal to
the enclosed `intent` byte for byte (`proof.payload_mismatch`), `signature` to
be 64 bytes, and `kid` to be the intent's `credential`
(`proof.credential_mismatch`). Everything but the signature is thereby fixed
by the intent.

**`webauthn`.** `credential_id` is 1–1023 bytes, `authenticator_data`
37–1024, `client_data_json` 1–4096 (the client's bytes, untouched),
`signature` 1–512, `user_handle` `null` or 1–64 bytes. The decoder bounds
them; a WebAuthn verifier checks them against the assertion challenge of §2.

## 7. What a decoder returns

`SealedIntent` holds the exact bytes, their digest and the `IntentV1` they
decode to. It has no constructor but `IntentV1::seal` and
`SealedIntent::decode`, so the bytes that were approved and signed are the
bytes that are authorized, claimed and dispatched, and their meaning is read
once. A forwarder passes `bytes()` on unchanged.

A decoder returns a record or a `WireError`; never a partial record. The first
violation met reading front to back is the one reported, and its `code()` is
stable: the vectors name it.

### The canonical JSON view

`view()` renders a decoded record as compact JSON with fields in label order:
the text an approval surface renders from and the conformance fixtures
compare. It is a rendering and never an input. Typed byte strings are
lower-case hex strings. Inside an open value: a byte string is
`{"$bytes": "<hex>"}`; an integer outside ±(2^53 − 1) is
`{"$int": "<decimal>"}`; a map key that begins with `$` is written with one
more `$` in front, so a map cannot forge either form.

## 8. Vectors and bindings

`vectors/v1.json` (format in `vectors/README.md`) has 20 accepted and 88
refused inputs, among them, for the four rejections the requirement names:
duplicate fields (`*duplicate*`), unknown critical values (`*unknown*`,
`*unsupported*`, `*critical*`), a second method (`*second*`,
`intent-approve-with-method`) and oversized bodies (`*oversized*`).

- **Native**: the crate itself. `fixtures/native` consumes it through its
  public API, builds the vectors with a byte-level encoder that shares no code
  with it, and verifies the vectors' signatures over the signing inputs the
  crate derives.
- **Browser**: the same crate compiled for `wasm32-unknown-unknown`
  (`fixtures/browser`), a module with no imports behind a four-function ABI,
  wrapped by `web/wire.mjs` on the standard WebAssembly API.
  `web/run-node.mjs` runs the vectors through it and checks each digest
  against WebCrypto's SHA-256.

`scripts/cross-check.sh` runs both over the committed file and compares their
reports byte for byte.

## 9. Open for review

Set here because a v1 needed a value, and each is a one-line change before the
freeze is ratified:

- the limits of §3–§6, the 300-second lifetime and the 16-level depth;
- `kind: "enroll_challenge"` and the `nonce` of `enroll`, which the Design's
  sketch implies (an enrollment nonce) without naming a record;
- the enrollment signing input and the registration challenge of §2;
- `"reject_once"` beside the Design's `"allow_once"`;
- the derivations of the credential reference and the MLS leaf identity;
- no frame by which a client asks for a command challenge: v1 leaves the
  request to the flow that carries it.
