# plexus-transport-mls

The MLS agent transport of Plexus. This crate is versioned and published on
its own: it depends on neither `plexus-transport` nor an application runtime.

What is here is the **wire profile v1** (`plexus_transport_mls::wire`): the
bounded records a relay and two endpoints exchange, deterministic CBOR for the
signed intent, and strict decoders. The same signed intent has one meaning at
every endpoint, because it has one encoding and every decoder refuses all
others.

```rust
use plexus_transport_mls::wire::{ApplicationV1, DeliveryV1, SealedIntent};

let delivery = DeliveryV1::decode_bounded(bytes)?;   // all a relay reads
// ... the MLS library validates and decrypts delivery.mls() ...
let frame = ApplicationV1::decode(plaintext)?;       // strict, bounded
if let ApplicationV1::Invoke(invoke) = frame {
    let intent: &SealedIntent = &invoke.intent;
    intent.bytes();   // exactly what was approved and signed
    intent.digest();  // SHA-256 of those bytes
    intent.intent();  // their one meaning: method, resource, typed arguments
}
```

- `docs/wire-v1.md` — the specification: encoding rules, labels, limits, time
  units, the MLS suite, algorithm identifiers, key encodings, derived values.
- `vectors/v1.json` — 20 accepted and 88 refused byte vectors.
- `fixtures/native`, `fixtures/browser` — the two adapters. The browser one is
  this crate compiled for `wasm32-unknown-unknown`, loaded with the standard
  WebAssembly API (`fixtures/browser/web/wire.mjs`).

The crate is `no_std` + `alloc`, forbids `unsafe`, and its one dependency is
`sha2`. It verifies no signature and reads no clock: it checks shape and the
binding of a proof to the enclosed bytes, and hands the credential verifiers
their exact inputs.

## Checks

```sh
cargo test --workspace          # unit tests, the vectors natively, the doctest
bash scripts/cross-check.sh     # native <-> browser, byte for byte
```

`scripts/cross-check.sh` needs the `wasm32-unknown-unknown` target and `node`.
It asserts the dependency graph (this crate depends on nothing of ours; each
fixture depends on it), that `vectors/v1.json` is what the builder writes, that
each adapter observes of every vector what the file expects, and that the two
adapters' reports are the same bytes.
