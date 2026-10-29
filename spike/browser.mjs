// BIDIR-3 (spike): the duplex frames and today's question from the
// browser's WebSocket API (Node 22's global WebSocket is the WHATWG one a
// browser has). `node spike/browser.mjs <port> <server pid>`
const [port, pid] = process.argv.slice(2);
const fs = await import("node:fs");
const rssKb = () => +/VmRSS:\s+(\d+)/.exec(fs.readFileSync(`/proc/${pid}/status`, "utf8"))[1];
const ws = new WebSocket(`ws://127.0.0.1:${port}`);
await new Promise((ok, no) => { ws.onopen = ok; ws.onerror = no; });
let id = 0;
const waiting = new Map(); // request id → resolve
const subs = new Map();    // subscription → handler(item)
const early = new Map();   // subscription → items before it was known
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.id !== undefined && waiting.has(m.id)) { waiting.get(m.id)(m); waiting.delete(m.id); return; }
  if (!m.params) return;
  // An item may arrive before the call's answer has been read (several
  // frames in one event-loop turn): kept until its subscription is known.
  if (subs.has(m.params.subscription)) subs.get(m.params.subscription)(m.params.result);
  else (early.get(m.params.subscription) ?? early.set(m.params.subscription, []).get(m.params.subscription)).push(m.params.result);
};
const request = (method, params) => new Promise((ok) => { const i = ++id; waiting.set(i, ok); ws.send(JSON.stringify({ jsonrpc: "2.0", id: i, method, params })); });
const notify = (method, params) => ws.send(JSON.stringify({ jsonrpc: "2.0", method, params }));
async function open(method, params = {}) {
  let wake = null;
  const r = await request(method, params);
  if (r.error) { console.log("open refused:", JSON.stringify(r.error)); return { sub: undefined }; }
  const sub = r.result;
  const queue = early.get(sub) ?? []; early.delete(sub);
  subs.set(sub, (it) => { queue.push(it); if (wake) { wake(); wake = null; } });
  const next = async () => { while (!queue.length) await new Promise((ok) => (wake = ok)); return queue.shift(); };
  return { sub, next, queue };
}
const pad = "x".repeat(1000);

// 1 · order: 10,000 × 1 KB, one per frame, under credit 64.
{
  const c = await open("spike.duplex");
  let credit = 0, seq = 0; const t0 = performance.now();
  while (seq < 10000) {
    while (credit === 0) { const it = await c.next(); if (it.type === "credit") credit += it.n; }
    notify("spike.input", { subscription: c.sub, seq, items: [{ i: seq, pad }] });
    seq++; credit--;
    while (c.queue.length) { const it = c.queue.shift(); if (it.type === "credit") credit += it.n; }
  }
  notify("spike.end", { subscription: c.sub, seq });
  let last; for (;;) { const it = await c.next(); if (it.type === "data") last = it.content; if (it.type === "done") break; }
  const s = (performance.now() - t0) / 1000;
  console.log(`JS ORDER 10k×1KB credit 64: ${JSON.stringify(last)}; ${(10000 / s).toFixed(0)} items/s`);
  if (!(last.received === 10000 && last.in_order)) process.exit(1);
}
// 2 · credit exceeded.
{
  const c = await open("spike.hold");
  await c.next();
  for (let s = 0; s < 65; s++) notify("spike.input", { subscription: c.sub, seq: s, items: [{ i: s }] });
  const e = await c.next();
  console.log(`JS CREDIT exceeded: ${JSON.stringify(e)}`);
  if (e.code !== "credit_exceeded") process.exit(1);
}
// 3 · today's question, 200 round trips.
{
  const c = await open("spike.ask", [{ n: 200 }]);
  const rt = [];
  for (let i = 0; i < 200; i++) {
    const q = await c.next();
    const t0 = performance.now();
    await request("spike.respond", { request_id: q.requestId, response_data: { type: "confirmed", value: true } });
    const a = await c.next();
    rt.push(performance.now() - t0);
    if (a.content.answer !== true) { console.log("bad answer", a); process.exit(1); }
  }
  rt.sort((a, b) => a - b);
  console.log(`JS question round trip: p50 ${rt[100].toFixed(3)} ms p99 ${rt[198].toFixed(3)} ms`);
}
// 4 · memory: 1,000 open duplex calls, the server's RSS.
{
  const warm = await open("spike.hold"); await warm.next();
  await new Promise((ok) => setTimeout(ok, 300));
  const before = rssKb();
  for (let k = 0; k < 1000; k++) { const c = await open("spike.hold"); if (c.sub === undefined) { console.log("refused at", k); break; } await c.next(); }
  await new Promise((ok) => setTimeout(ok, 300));
  const after = rssKb();
  console.log(`JS MEMORY: server RSS ${before} → ${after} KB for 1,000 open calls: ${((after - before) / 1000).toFixed(1)} KB per call`);
}
ws.close();
process.exit(0);
