// The browser binding of the wire profile: the wire crate, compiled to
// WebAssembly, behind the WebAssembly API every browser has. Nothing here is
// Node's; `run-node.mjs` is the only file that touches a file system.
//
//   const wire = await load(await (await fetch("mls_wire_browser.wasm")).arrayBuffer());
//   const seen = wire.observe("intent", bytes);
//   if (seen.ok) render(seen.value);   // seen.intent are the bytes to sign
//
// The module imports nothing: it cannot reach the network, a clock or
// randomness, and decoding the same bytes gives the same answer every time.

/** The record numbers of the ABI. */
export const RECORD = Object.freeze({ delivery: 0, application: 1, intent: 2 });

const ABI_VERSION = 1;

const unhex = (text) => Uint8Array.from(text.match(/../g) ?? [], (pair) => parseInt(pair, 16));

/** The decoders, over one instance of the module. */
export class Wire {
  #exports;

  constructor(instance) {
    this.#exports = instance.exports;
    if (this.#exports.pmls_abi_version() !== ABI_VERSION) {
      throw new Error("mls_wire_browser: ABI version mismatch");
    }
  }

  /**
   * What the decoder of `record` makes of `bytes`, as the one line both
   * adapters report: `ok<TAB>intent hex<TAB>digest hex<TAB>view`, or
   * `error<TAB>code`.
   */
  observeLine(record, bytes) {
    const code = RECORD[record];
    if (code === undefined) throw new Error(`unknown record ${record}`);
    const { memory, pmls_alloc, pmls_free, pmls_observe } = this.#exports;
    const input = pmls_alloc(bytes.length);
    // The buffer is looked up after each call into the module: memory growth
    // replaces it.
    new Uint8Array(memory.buffer, input, bytes.length).set(bytes);
    const packed = pmls_observe(code, input, bytes.length);
    pmls_free(input, bytes.length);
    const at = Number(packed >> 32n);
    const length = Number(packed & 0xffff_ffffn);
    const line = new TextDecoder("utf-8", { fatal: true }).decode(
      new Uint8Array(memory.buffer, at, length),
    );
    pmls_free(at, length);
    return line;
  }

  /**
   * The same, typed. Accepted: `{ ok: true, intent, digest, view, value }`,
   * where `intent` is the exact intent bytes (or null when the record carries
   * none), `digest` their SHA-256, `view` the canonical JSON view and `value`
   * that view parsed. Refused: `{ ok: false, code }`.
   */
  observe(record, bytes) {
    const [result, ...fields] = this.observeLine(record, bytes).split("\t");
    if (result !== "ok") return { ok: false, code: fields[0] };
    const [intent, digest, ...view] = fields;
    const text = view.join("\t");
    return {
      ok: true,
      intent: intent === "-" ? null : unhex(intent),
      digest: digest === "-" ? null : unhex(digest),
      view: text,
      value: JSON.parse(text),
    };
  }
}

/** Instantiates the module from its bytes. */
export async function load(wasmBytes) {
  const module = await WebAssembly.compile(wasmBytes);
  const imports = WebAssembly.Module.imports(module);
  if (imports.length !== 0) {
    throw new Error(`mls_wire_browser: unexpected imports: ${JSON.stringify(imports)}`);
  }
  return new Wire(await WebAssembly.instantiate(module, {}));
}
