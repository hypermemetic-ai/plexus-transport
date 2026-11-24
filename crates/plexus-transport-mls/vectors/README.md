# Wire v1 vectors

`v1.json` holds the byte vectors of wire profile v1 (`../docs/wire-v1.md`).
It is frozen: a decoder of v1 makes of every input exactly what the file
expects, and a change to an existing vector is a change of wire version.

```json
{ "name": "intent-duplicate-method", "record": "intent", "about": "…",
  "input": ["b0006…", {"repeat": "00", "times": 65464}, "0b…"],
  "expect": {"result": "error", "code": "cbor.duplicate_key"} }
```

- `record` is the decoder the input is for: `delivery`, `application` or
  `intent`.
- `input` is the concatenation of its pieces. A string is lower-case hex; an
  object is its `repeat` hex, `times` times over (the oversized vectors).
- `expect.result` is `ok` or `error`.
  - `ok`: `intent` is the hex of the exact intent bytes the record carries and
    `digest` their SHA-256, both `null` when the record carries no intent;
    `view` is the canonical JSON view of the decoded record, as a string.
  - `error`: `code` is the stable code of the rejection.

Every signature is by `test_key`, a P-256 key published in the file: it
protects nothing. The PIV proofs are true ES256 signatures over the COSE
`Sig_structure`; the WebAuthn assertions are true ES256 signatures over
`authenticator_data || SHA-256(client_data_json)`. Certificates, attestation
objects and MLS messages are synthetic stand-ins, which the wire profile
bounds and does not read.

The file is written by the native fixture's builder, which assembles every
input with a byte-level encoder of its own, and is checked from both sides:

```sh
cargo run -p mls-wire-native-fixture -- generate vectors/v1.json   # rewrite
bash scripts/cross-check.sh                                        # check
```
