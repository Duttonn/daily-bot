// Source of the bot's daily gain cap (owner 2026-11-07): what the platform made on the previous UTC day, read on chain, and
// today's cap, written to day-cap.json next to the bot, which re-reads it every minute.
//   node platform-day.mjs            poll every 2 min, forever (VPS, tmux session platform-day, cwd ~/daily-bot)
//   node platform-day.mjs --once     catch up, write today's cap, exit
//   node platform-day.mjs --selftest the day sums and the rules on made-up rows
// Every transaction of the market maker vault's USDC account is kept as one row of platform-tx.jsonl: the vault's USDC change,
// the platform fee in it, whether it is ours. A day's platform result is then its fees plus the vault's trading result
// (trades and claims; refills and withdrawals, kind 'other', are left out and reported). Our own result is our cash vault's
// USDC balance change over the day, so a deposit or withdrawal that day moves it.
// CAP_RULE picks which gain the cap is a share of (CAP_SHARE, 0.5); the owner chooses, see RULES.
import fs from 'node:fs';

const RPC = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const MM = 'F9sT6bbtmT1bo7VA5eCbGe5QTiZ3AMW36ERH9DohGEj5';  // market maker vault, every fill's counterparty
const FEE = 'DULx9BVUyF3N27NfGpEbtSn9WPvFkeDupmBudbjPUkxm'; // platform fee wallet
const US = process.env.US_OWNER || '7AVZX22zfpqJLYcysVF3obt8BEUGL2AxtktEtDfM9xpD'; // our cash vault
// cash vaults of the accounts we host on the VPS (owner 2026-11-09: another person's account, ~/daily-bot-2): their trades and
// payouts are not the others' and are taken out of net_from_others
const HOSTED = (process.env.HOSTED_OWNERS || '3Ga7z16ntwpMmr7pqZhWAnHpajUbbaeJB9etYnTmKH43').split(',').filter(Boolean);
const ROWS = 'platform-tx.jsonl', OUT = 'day-cap.json', LEDGER = 'day-caps.jsonl'; // LEDGER: each past day's base cap and our result
const SHARE = +(process.env.CAP_SHARE || 0.5), RULE = process.env.CAP_RULE || 'net_from_others';
export const RULES = {
  net_from_others: (d) => d.feesAll + d.vault + d.ourNet - (d.feesHosted || 0) - (d.vaultHosted || 0), // the platform's result on every user but us and the hosted accounts
  fees_all: (d) => d.feesAll,                             // every fee, ours included
  net_incl_us: (d) => d.feesAll + d.vault,                // the platform's result, us included
  fees_others: (d) => d.feesAll - d.feesOurs,             // the fees of the other users
};

const sleep = (ms) => new Promise((s) => setTimeout(s, ms));
const r2 = (x) => Math.round(x * 100) / 100;
// A day runs from 00:01 to 00:01 UTC: the round ending at 00:00 pays out in the next seconds (+201.81 to us at 2026-11-07
// 00:00:01), so a cut at 00:00 books its stakes on one day and its payouts on the next.
const BOUNDARY_S = 60;
const dayOf = (sec) => new Date((sec - BOUNDARY_S) * 1000).toISOString().slice(0, 10);
const dayStartSec = (day) => Date.parse(day + 'T00:00:00Z') / 1000 + BOUNDARY_S;
async function rpc(method, params) {
  for (let i = 0; i < 8; i++) {
    const r = await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(20000) }).catch(() => null);
    if (!r || r.status === 429 || r.status >= 500) { await sleep(2000 * (i + 1)); continue; }
    const j = await r.json();
    if (j.error) throw new Error(`${method} ${JSON.stringify(j.error)}`);
    await sleep(250);
    return j.result;
  }
  throw new Error(`${method}: RPC failed 8 times`);
}
const getTx = (sig) => rpc('getTransaction', [sig, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1, commitment: 'confirmed' }]);
export const usdcAta = async (owner) => (await rpc('getTokenAccountsByOwner', [owner, { mint: USDC }, { encoding: 'jsonParsed' }])).value[0].pubkey;

// one row per vault transaction: USDC change of the vault, fee to the platform, ours or not
export function rowOf(sig, tx) {
  const bal = {};
  for (const [s, list] of [[-1, tx.meta.preTokenBalances], [1, tx.meta.postTokenBalances]])
    for (const b of list || []) if (b.mint === USDC) bal[b.owner] = (bal[b.owner] || 0) + s * (b.uiTokenAmount.uiAmount || 0);
  const fee = bal[FEE] || 0, claim = (tx.meta.logMessages || []).some((l) => /Instruction: .*Claim/.test(l));
  return { sig, t: tx.blockTime, kind: fee > 0 ? 'trade' : claim ? 'claim' : 'other', mm: r2(bal[MM] || 0), fee: +fee.toFixed(6), ours: US in bal, hosted: HOSTED.some((h) => h in bal) };
}

// a UTC day's sums from the rows
export function summarize(rows, day) {
  const d = { day, feesAll: 0, feesOurs: 0, vault: 0, vaultOther: 0, feesHosted: 0, vaultHosted: 0, trades: 0, ourTrades: 0, hostedTrades: 0, claims: 0, other: 0 };
  for (const x of rows) {
    if (x.kind === 'start' || dayOf(x.t) !== day) continue;
    if (x.kind === 'trade') { d.trades++; d.feesAll += x.fee; d.vault += x.mm; if (x.ours) { d.ourTrades++; d.feesOurs += x.fee; } }
    else if (x.kind === 'claim') { d.claims++; d.vault += x.mm; }
    else { d.other++; d.vaultOther += x.mm; }
    if (x.hosted && x.kind !== 'other') { d.vaultHosted += x.mm; d.feesHosted += x.fee; if (x.kind === 'trade') d.hostedTrades++; }
  }
  for (const k of ['feesAll', 'feesOurs', 'vault', 'vaultOther', 'feesHosted', 'vaultHosted']) d[k] = r2(d[k]);
  return d;
}

// USDC balance of a token account at time t (seconds): the post balance of its last transaction before t
async function balanceAt(ata, t) {
  let before;
  for (;;) {
    const s = await rpc('getSignaturesForAddress', [ata, { limit: 1000, before }]);
    const hit = s.find((x) => x.blockTime < t && !x.err);
    if (hit) {
      const tx = await getTx(hit.signature), i = tx.transaction.message.accountKeys.findIndex((k) => k.pubkey === ata);
      return tx.meta.postTokenBalances.find((b) => b.accountIndex === i)?.uiTokenAmount.uiAmount ?? 0;
    }
    if (s.length < 1000) return 0;
    before = s.at(-1).signature;
  }
}

const readJsonl = (f) => (fs.existsSync(f) ? fs.readFileSync(f, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)) : []);
const readRows = () => readJsonl(ROWS), readLedger = () => readJsonl(LEDGER);
// today's cap: its base plus what the past days left of theirs (negative when they made more than their cap)
const carryOf = (ledger) => r2(ledger.reduce((s, x) => s + x.base - x.ourNet, 0));
export const capWithCarry = (base, ledger) => r2(Math.max(0, base + carryOf(ledger)));

// appends the vault's transactions newer than the last row; a fresh file starts at yesterday 00:00 UTC
async function catchUp(vaultAta) {
  const rows = readRows(), last = rows.filter((x) => x.sig).at(-1);
  const from = rows.length ? 0 : dayStartSec(dayOf(Date.now() / 1000 - 86400));
  if (!rows.length) fs.appendFileSync(ROWS, JSON.stringify({ kind: 'start', t: from }) + '\n');
  const sigs = [];
  let before;
  for (;;) {
    const s = await rpc('getSignaturesForAddress', [vaultAta, { limit: 1000, before, until: last?.sig }]);
    sigs.push(...s.filter((x) => !x.err && x.blockTime >= from));
    if (s.length < 1000 || s.at(-1).blockTime < from) break;
    before = s.at(-1).signature;
  }
  for (const x of sigs.reverse()) fs.appendFileSync(ROWS, JSON.stringify(rowOf(x.signature, await getTx(x.signature))) + '\n');
  return sigs.length;
}

// writes today's cap from yesterday once the rows cover all of yesterday
export async function writeCap(ourAta) {
  const today = dayOf(Date.now() / 1000), y = dayOf(Date.now() / 1000 - 86400);
  if (Date.now() / 1000 < dayStartSec(today) + 30) return null; // yesterday's last payouts may not be read yet
  try { if (JSON.parse(fs.readFileSync(OUT, 'utf8')).day === today) return null; } catch {}
  const rows = readRows(), start = rows.find((x) => x.kind === 'start');
  if (!start || start.t > dayStartSec(y)) return null; // rows begin after yesterday 00:00: no full day yet
  const d = summarize(rows, y);
  d.ourNet = r2((await balanceAt(ourAta, dayStartSec(today))) - (await balanceAt(ourAta, dayStartSec(y))));
  const gains = Object.fromEntries(Object.entries(RULES).map(([k, f]) => [k, r2(f(d))]));
  // carry (owner 2026-11-07): what a day leaves of its cap moves to the next days, what it makes beyond is taken off.
  // ponytail: no ceiling on the carry; a day the job missed is left out of it.
  let prev = null;
  try { prev = JSON.parse(fs.readFileSync(OUT, 'utf8')); } catch {}
  if (prev?.day === y && !readLedger().some((x) => x.day === y)) fs.appendFileSync(LEDGER, JSON.stringify({ day: y, base: prev.base ?? prev.cap, ourNet: d.ourNet }) + '\n');
  const base = r2(Math.max(0, SHARE * gains[RULE])), ledger = readLedger();
  const out = { day: today, basedOn: y, rule: RULE, share: SHARE, base, carry: carryOf(ledger), cap: capWithCarry(base, ledger), gains, parts: d, at: new Date().toISOString() };
  fs.writeFileSync(OUT + '.tmp', JSON.stringify(out, null, 1));
  fs.renameSync(OUT + '.tmp', OUT);
  return out;
}

function selftest() {
  const t = dayStartSec('2026-11-06') + 3600, row = (kind, mm, fee, ours) => ({ kind, t, mm, fee, ours });
  const d = summarize([{ kind: 'start', t: 0 }, row('trade', 3, 0.1, true), row('trade', -1, 0.2, false), row('claim', 5, 0, false), row('other', 1000, 0, false),
    { ...row('trade', 9, 9, false), t: t + 86400 }], '2026-11-06');
  console.assert(d.trades === 2 && d.feesAll === 0.3 && d.feesOurs === 0.1 && d.vault === 7 && d.vaultOther === 1000 && d.other === 1, 'summarize', d);
  console.assert(summarize([{ ...row('claim', 5, 0, false), t: Date.parse('2026-11-07T00:00:01Z') / 1000 }], '2026-11-06').vault === 5, 'a payout at 00:00:01 belongs to the day before');
  const g = (k) => r2(RULES[k]({ ...d, ourNet: 10 }));
  console.assert(g('net_from_others') === 17.3 && g('fees_all') === 0.3 && g('net_incl_us') === 7.3 && g('fees_others') === 0.2, 'rules');
  // the vault's 24 h to 2026-11-07 00:18 UTC: fees 469.51, vault -847.16, us +900.12 -> 522.47 from the others
  console.assert(r2(RULES.net_from_others({ feesAll: 469.51, vault: -847.16, ourNet: 900.12 })) === 522.47, 'net_from_others on the 11-07 read');
  // a hosted account's trade (vault -4, fee 0.3) and payout (vault -2) leave the others' result, its 'other' rows never count
  const h = summarize([row('trade', -1, 0.2, false), { ...row('trade', -4, 0.3, false), hosted: true }, { ...row('claim', -2, 0, false), hosted: true }, { ...row('other', 50, 0, false), hosted: true }], '2026-11-06');
  console.assert(h.feesHosted === 0.3 && h.vaultHosted === -6 && h.hostedTrades === 1 && r2(RULES.net_from_others({ ...h, ourNet: 0 })) === -0.8, 'hosted', h);
  console.assert(rowOf('s', { blockTime: t, meta: { logMessages: [], preTokenBalances: [], postTokenBalances: [{ mint: USDC, owner: HOSTED[0], uiTokenAmount: { uiAmount: 1 } }, { mint: USDC, owner: FEE, uiTokenAmount: { uiAmount: 0.1 } }] } }).hosted === true, 'rowOf hosted');
  // the owner's example: caps of 250, day 1 makes 100, day 2 makes 150, day 3 may make 750 - 100 - 150 = 500
  console.assert(capWithCarry(250, [{ base: 250, ourNet: 100 }, { base: 250, ourNet: 150 }]) === 500, 'carry');
  console.assert(capWithCarry(250, [{ base: 250, ourNet: 900 }]) === 0, 'a day far over its cap leaves 0, not a negative cap');
  console.log('selftest done (an assertion failure prints above)');
}

if (process.argv.includes('--selftest')) selftest();
else if (import.meta.url === `file://${process.argv[1]}`) {
  if (!RULES[RULE]) throw new Error(`CAP_RULE ${RULE} unknown: ${Object.keys(RULES)}`);
  const [vaultAta, ourAta] = [await usdcAta(MM), await usdcAta(US)];
  const once = process.argv.includes('--once');
  for (;;) {
    try {
      const n = await catchUp(vaultAta), out = await writeCap(ourAta);
      console.log(new Date().toISOString(), `[PLATFORM] +${n} vault tx${out ? ` | cap ${out.day} = ${out.share} x ${out.rule} ${out.gains[out.rule]} = ${out.cap} USD | ${JSON.stringify(out.gains)}` : ''}`);
    } catch (e) { console.error(new Date().toISOString(), `[PLATFORM ERROR] ${e.message}`); }
    if (once) break;
    await sleep(120000);
  }
}
