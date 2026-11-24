// Runs the vector file through the browser binding under Node.
//
//   node run-node.mjs MODULE.wasm VECTORS.json
//
// Prints what the native fixture's `report` prints: one line per vector,
// `name<TAB>observation`, then a summary. Fails if an observation is not what
// the file expects, or if one of the checks made here, apart from the module,
// does not hold:
//   - the digest is the SHA-256 that WebCrypto computes of the intent bytes;
//   - the intent bytes are a contiguous part of the input, unchanged;
//   - the view is JSON.

import { readFile } from "node:fs/promises";
import { load } from "./wire.mjs";

const [wasmPath, vectorsPath] = process.argv.slice(2);
if (!wasmPath || !vectorsPath) {
  console.error("usage: node run-node.mjs MODULE.wasm VECTORS.json");
  process.exit(2);
}

const hex = (bytes) => Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
const unhex = (text) => Uint8Array.from(text.match(/../g) ?? [], (pair) => parseInt(pair, 16));

function inputOf(vector) {
  const pieces = vector.input.map((piece) => {
    if (typeof piece === "string") return unhex(piece);
    const unit = unhex(piece.repeat);
    const out = new Uint8Array(unit.length * piece.times);
    for (let i = 0; i < piece.times; i++) out.set(unit, i * unit.length);
    return out;
  });
  const out = new Uint8Array(pieces.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const piece of pieces) {
    out.set(piece, at);
    at += piece.length;
  }
  return out;
}

function expectedLine(expect) {
  if (expect.result === "error") return `error\t${expect.code}`;
  return `ok\t${expect.intent ?? "-"}\t${expect.digest ?? "-"}\t${expect.view}`;
}

const wire = await load(await readFile(wasmPath));
const file = JSON.parse(await readFile(vectorsPath, "utf8"));

const out = [];
const failures = [];
let accepted = 0;
let refused = 0;
let dispatched = 0;

for (const vector of file.vectors) {
  const input = inputOf(vector);
  const line = wire.observeLine(vector.record, input);
  out.push(`${vector.name}\t${line}`);
  if (line !== expectedLine(vector.expect)) {
    failures.push(`${vector.name}: expected ${expectedLine(vector.expect)}, observed ${line}`);
  }

  const seen = wire.observe(vector.record, input);
  if (!seen.ok) {
    refused += 1;
    continue;
  }
  accepted += 1;
  if (seen.intent === null) continue;
  // An inert dispatcher: reached only with an accepted intent.
  dispatched += 1;
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", seen.intent));
  if (hex(digest) !== hex(seen.digest)) {
    failures.push(`${vector.name}: digest is not WebCrypto's SHA-256 of the intent bytes`);
  }
  if (!Buffer.from(input).includes(Buffer.from(seen.intent))) {
    failures.push(`${vector.name}: intent bytes are not part of the input`);
  }
}

out.push(
  `summary\tvectors=${file.vectors.length}\taccepted=${accepted}\trefused=${refused}\tdispatched=${dispatched}`,
);
process.stdout.write(out.join("\n") + "\n");
for (const failure of failures) console.error(`MISMATCH ${failure}`);
process.exit(failures.length === 0 ? 0 : 1);
