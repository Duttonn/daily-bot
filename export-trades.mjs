// Dashboard export (run on the VPS by dashboard/updown-server.mjs every 3 s): one JSON line on stdout.
// trades: one row per position = market side x strategy (bot or LightGBM leg), built from the venue's fills by requestId
// (buys and sells, a stop sale is a sell) and the venue's verdict; the leg's orders are told apart by its lgb_order_sent records.
// Also: the venue's own realized PnL per market side (cross-check), the wallet ledger, the last heartbeat, the bot's CONFIG as
// deployed, the leg's ledger and the paper trader's results.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TRADES_FILE = process.env.TRADES_LOG || path.join(__dirname, 'trades.jsonl');
const BOT_LOG_FILE = process.env.BOT_LOG || path.join(__dirname, 'bot.log');
const BOT_FILE = path.join(__dirname, 'daily-s6-bot.mjs');
const PAPER_LOG = process.env.PAPER_LOG || path.join(os.homedir(), 'lgbm-paper', 'paper.jsonl');
const LEG_LOG = process.env.LEG_LOG || path.join(os.homedir(), 'lgbm-leg', 'paper.jsonl');

const readJsonl = (f) => (fs.existsSync(f) ? fs.readFileSync(f, 'utf8').split('\n') : []).flatMap((l) => { try { return l ? [JSON.parse(l)] : []; } catch { return []; } });
const readJson = (f) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return null; } };
const tail = (f, n) => { try { const fd = fs.openSync(f, 'r'), size = fs.fstatSync(fd).size, b = Buffer.alloc(Math.min(n, size)); fs.readSync(fd, b, 0, b.length, size - b.length); fs.closeSync(fd); return b.toString('utf8'); } catch { return ''; } };
const r2 = (x) => (x == null ? null : Math.round(x * 100) / 100);
const startOf = (slug) => +String(slug || '').split('-').pop() || null;

const rows = readJsonl(TRADES_FILE);
const legReq = new Set(), slugOf = {}, intentOf = {}, failed = new Set(), wallet = [], claimed = new Set();
const fills = [], venue = {}, verdict = {};
for (const r of rows) {
  if (r.event === 'lgb_order_sent') { legReq.add(r.requestId); slugOf[r.marketPda] = r.slug; }
  if (r.status === 'intent_sent') { intentOf[r.requestId] = r; slugOf[r.marketId] = r.roundId; }
  if (r.event === 'daily_error' && r.details?.requestId) failed.add(r.details.requestId);
  if (r.type === 'cash_account_changed.v1') { const a = Number(r.details?.availableAtomic) / 1e6; if (a > 0) wallet.push([r.t, r2(a)]); }
  const d = r.details;
  if (r.event === 'trade_executed' && d?.type === 'daily_markets_trade_executed') {
    const e = d.execution || {}, sh = Number(e.acceptedSize) / 1e6;
    if (sh > 0) fills.push({ req: d.requestId, t: r.t, pda: e.market, outcome: e.outcomeIndex, sell: e.side === 'sell', sh,
      notional: Number(e.acceptedNotional) / 1e6, fee: Number(e.acceptedFee) / 1e6, sig: e.acceptedSignature });
  }
  for (const p of (r.type === 'updown_update' && d?.nativePositions) || []) {
    const m = p.market || {};
    if (m.slug) slugOf[m.marketPda] = m.slug;
    if (m.payoutVector) verdict[m.marketPda] = m.payoutVector.map((x) => Number(x) / 1e6);
    // the venue books a held position's PnL when it is claimed: until then realizedPnlMicros is 0, so leave the side out (last record wins)
    const vk = `${m.marketPda}:${p.outcomeIndex}`;
    if (p.claimedAt || Number(p.quantity) === 0) venue[vk] = Number(p.realizedPnlMicros || 0) / 1e6; else delete venue[vk];
    if (p.claimedAt) claimed.add(vk);
  }
}

const pos = new Map();
for (const f of fills) {
  if (failed.has(f.req)) continue; // accepted, then refused by the venue
  const owner = legReq.has(f.req) ? 'lgb' : 'bot', key = `${f.pda}:${f.outcome}:${owner}`;
  let p = pos.get(key);
  if (!p) {
    const it = intentOf[f.req];
    p = { key, pda: f.pda, outcome: f.outcome, owner, slug: slugOf[f.pda] || f.pda, side: f.outcome === 0 ? 'UP' : 'DOWN', t: f.t,
      sigma: it ? r2(it.sigma_m) : null, q: it ? r2(it.q) : null, edge: it ? r2(it.edge) : null,
      buys: 0, sells: 0, sharesIn: 0, cost: 0, sharesOut: 0, proceeds: 0, fees: 0, exitT: null, sig: f.sig };
    pos.set(key, p);
  }
  if (f.sell) { p.sells++; p.sharesOut += f.sh; p.proceeds += f.notional - f.fee; p.exitT = f.t; }
  else { p.buys++; p.sharesIn += f.sh; p.cost += f.notional + f.fee; }
  p.fees += f.fee;
}

const ours = {}; // our PnL per settled market side, both strategies merged, for the cross-check with the venue's
const trades = [...pos.values()].sort((a, b) => a.t - b.t).map((p, i) => {
  const start = startOf(p.slug), left = p.sharesIn - p.sharesOut, pv = verdict[p.pda];
  const pay = pv ? pv[p.outcome] : null;
  const settled = left < 1e-6 || pay != null;
  const payout = left > 1e-6 && pay != null ? left * pay : 0;
  const pnl = settled ? p.proceeds + payout - p.cost : null;
  if (pnl != null) ours[`${p.pda}:${p.outcome}`] = (ours[`${p.pda}:${p.outcome}`] || 0) + pnl;
  return {
    index: i + 1, owner: p.owner, slug: p.slug, start, time: new Date(p.t).toISOString(), side: p.side,
    rem: start ? Math.round(start + 300 - p.t / 1000) : null, exitRem: p.exitT && start ? Math.round(start + 300 - p.exitT / 1000) : null,
    sigma: p.sigma, q: p.q, edge: p.edge, buys: p.buys, sells: p.sells,
    sharesIn: r2(p.sharesIn), avgIn: p.sharesIn ? Math.round((p.cost / p.sharesIn) * 1000) / 1000 : null, cost: r2(p.cost),
    sharesOut: r2(p.sharesOut), avgOut: p.sharesOut ? Math.round((p.proceeds / p.sharesOut) * 1000) / 1000 : null, proceeds: r2(p.proceeds),
    fees: Math.round(p.fees * 1000) / 1000, resolution: pv ? (pv[0] > 0.5 ? 'UP' : 'DOWN') : null, payout: r2(payout),
    exit: p.sharesOut > 1e-6 ? (left > 1e-6 ? 'partielle' : p.owner === 'bot' ? 'stop' : 'vente') : settled ? 'règlement' : 'ouverte',
    pnl: r2(pnl), sig: p.sig, toClaim: payout > 0 && !claimed.has(`${p.pda}:${p.outcome}`) ? r2(payout) : 0
  };
});

const keys = Object.keys(ours).filter((k) => k in venue);
const venueCheck = { sides: keys.length, ours: r2(keys.reduce((a, k) => a + ours[k], 0)), venue: r2(keys.reduce((a, k) => a + venue[k], 0)) };

// Last heartbeat (the log's tail only)
const hbLine = (tail(BOT_LOG_FILE, 262144).match(/^.*\[HEARTBEAT\].*$/gm) || []).pop() || '';
const num = (re) => { const m = hbLine.match(re); return m ? parseFloat(m[1]) : null; };
const hbTime = Date.parse(hbLine.slice(0, 24));
const heartbeat = {
  time: Number.isFinite(hbTime) ? new Date(hbTime).toISOString() : null, ageS: Number.isFinite(hbTime) ? Math.round((Date.now() - hbTime) / 1000) : null,
  bf: num(/bf=([\d.]+)/), sigmaM: num(/sigmaM=([\d.]+)/), usdc: num(/usdc=([\d.]+)\$/), floor: num(/floor=([\d.]+)\$/),
  bot: num(/ bot=([\d.]+)\$/), lgbCash: num(/ lgb=([\d.]+)\$/), lgbPos: num(/ lgb=[\d.]+\$\/(\d+)pos/), halted: hbLine.includes('/HALTED'),
  authed: /authed=true/.test(hbLine)
};

// The bot's CONFIG as deployed
const src = fs.existsSync(BOT_FILE) ? fs.readFileSync(BOT_FILE, 'utf8') : '';
const cfgNum = (k, re = new RegExp(`^\\s*${k}:\\s*([-\\d.]+)`, 'm')) => { const m = src.match(re); return m ? parseFloat(m[1]) : null; };
const config = Object.fromEntries(['windowRemainingMin', 'windowRemainingMax', 'earlyEntryMaxSigma', 'lateEntryTau', 'minZScore', 'calmVolThreshold',
  'minSigmaFloor', 'minEdgeMargin', 'maxEntryAsk', 'stopLossMaxQ', 'stopLossMaxAdverseUsd', 'stopLossMinTau', 'stopLossMaxTau', 'trailingFloorDrawdown'].map((k) => [k, cfgNum(k)]));
config.sizingFraction = cfgNum('sizingFraction', /sizingFraction:\s*Math\.min\(([\d.]+)/);
config.legCapFrac = cfgNum('capFrac', /capFrac:\s*([\d.]+)/);
config.legHaltBelow = cfgNum('haltBelow', /haltBelow:\s*([\d.]+)/);

// LightGBM leg: ledger, trader journal, paper trader
const state = readJson(path.join(__dirname, 'bot-state.json')) || {};
const lgb = readJson(path.join(__dirname, 'lgb-state.json'));
const legEv = readJsonl(LEG_LOG);
const li = legEv.map((e) => e.ev).lastIndexOf('start'), legStart = legEv[li]; // the trader's last start, and its preload if any
const legPre = li >= 0 ? legEv.slice(li + 1).find((e) => e.ev === 'preload') : null;
const st = {}, sentOf = {}, misses = [];
for (const e of legEv) if (e.ev === 'order' && e.sent) sentOf[e.k] = e;
const fromKey = (k) => { const [rs, t, act, side] = String(k).split('|'); return { slug: `btc-updown-5m-${rs}`, t: +t, act, side }; };
for (const e of legEv) {
  if (e.ev === 'venue') st[e.st] = (st[e.st] || 0) + 1;
  // an order the model wanted and did not get: never sent, or sent and answered without a fill
  if (e.ev === 'order' && !e.sent) misses.push({ ...fromKey(e.k), where: 'trader', why: e.why });
  if (e.ev === 'venue' && e.st !== 'filled') { const o = sentOf[e.k] || fromKey(e.k); misses.push({ slug: o.slug, t: o.t, act: o.act, side: o.side, limit: o.limit, target: o.target ?? o.shares, where: e.st, why: e.why || e.code || e.st }); }
}
const legSlugs = new Set(trades.filter((t) => t.owner === 'lgb').map((t) => t.slug));
const leg = lgb && {
  cash: r2(lgb.cash), halted: !!lgb.halted, createdAt: lgb.createdAt, open: Object.values(lgb.pos || {}).map((p) => ({ slug: p.slug, outcome: p.outcome, shares: p.shares })),
  trader: { start: legStart?.wall || null, warmupS: legStart?.warmup_s ?? null,
    warmFrom: legPre ? legPre.warm_from : legStart ? Date.parse(legStart.wall) / 1000 + (legStart.warmup_s || 0) : null, preloadGapS: legPre?.gap_s ?? null, sent: Object.keys(sentOf).length,
    notSent: legEv.filter((e) => e.ev === 'order' && !e.sent).length, answers: st },
  misses: misses.slice(-100)
};
const pap = readJsonl(PAPER_LOG), papSettle = pap.filter((e) => e.ev === 'settle' && e.spent > 0);
const paper = pap.length ? {
  start: pap.find((e) => e.ev === 'start')?.wall || null, pool0: pap.find((e) => e.ev === 'start')?.pool0 ?? null,
  pool: r2(pap.filter((e) => e.ev === 'settle').pop()?.pool_settled ?? null), rounds: papSettle.length, won: papSettle.filter((e) => e.pnl > 0).length,
  bySlug: Object.fromEntries(papSettle.map((e) => [e.slug, { pnl: r2(e.pnl), spent: r2(e.spent) }]))
} : null;
// rounds the paper trader traded after the leg went live and the leg did not (its simulated fills against the leg's real ones)
if (leg && paper) {
  // a round already running when the trader starts is never its to trade
  const from = Math.max(leg.trader.warmFrom ?? 0, leg.trader.start ? Math.ceil(Date.parse(leg.trader.start) / 300000) * 300 : 0);
  leg.paperOnly = papSettle.filter((e) => startOf(e.slug) >= from && !legSlugs.has(e.slug)).map((e) => ({ slug: e.slug, pnl: r2(e.pnl), spent: r2(e.spent) }));
}

// Orders and the venue's answer: the bot's buys and the leg's orders (a refusal is a daily_error on that requestId)
const errCode = {}, executed = new Set(fills.map((f) => f.req));
for (const r of rows) if (r.event === 'daily_error' && r.details?.requestId) errCode[r.details.requestId] = r.details.code || 'error';
const orders = rows.filter((r) => r.status === 'intent_sent' || r.event === 'lgb_order_sent').map((r) => ({
  t: r.t, owner: r.event === 'lgb_order_sent' ? 'lgb' : 'bot', act: r.act || 'entry',
  st: errCode[r.requestId] ? 'refused' : executed.has(r.requestId) ? 'filled' : 'none', code: errCode[r.requestId] || null }));

// Runs: the bot's restarts (its state restore line) and the leg trader's starts
let runs = [];
try {
  for (const m of fs.readFileSync(BOT_LOG_FILE, 'utf8').matchAll(/^(\S+) \[STATE RESTORED\] Loaded (\d+) traded rounds, (\d+) active positions/gm))
    runs.push({ t: Date.parse(m[1]), what: 'bot', note: `${m[3]} position(s) reprise(s)` });
} catch {}
for (const e of legEv) if (e.ev === 'start') runs.push({ t: Date.parse(e.wall), what: 'lgb', note: `trader ${String(e.sha256?.['trader.py'] || '').slice(0, 8)}` });
for (const e of legEv) if (e.ev === 'preload') runs.push({ t: e.until * 1000, what: 'lgb', note: `préchargement, trade dès ${new Date(e.warm_from * 1000).toISOString().slice(11, 16)} UTC` });
runs = runs.filter((x) => Number.isFinite(x.t)).sort((a, b) => a.t - b.t).slice(-30);

// Journal: the log's last lines, without the raw venue records and the heartbeats
const logTail = tail(BOT_LOG_FILE, 262144).split('\n').slice(1).filter((l) => l && !l.includes('TRADE LOGGED') && !l.includes('[HEARTBEAT]')).slice(-80).join('\n');

const toClaim = r2(trades.reduce((a, t) => a + t.toClaim, 0)); // won and settled, not yet claimed: not in the wallet yet
console.log(JSON.stringify({ trades, venueCheck, wallet, toClaim, heartbeat, peak: r2(state.peakBalance), balance: heartbeat.usdc, config, leg, paper, orders, runs, logTail, updatedAt: new Date().toISOString() }));
