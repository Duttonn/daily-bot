// Minimal zero-dependency local server for Daily Up/Down dashboard with in-memory caching
import { createServer } from 'node:http';
import { readFileSync, existsSync, appendFileSync } from 'node:fs';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const execAsync = promisify(exec);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 8789;
const HTML_PATH = path.join(__dirname, 'updown.html');
const SSH_KEY = process.env.SSH_KEY || '/Users/ndn18/Downloads/canada-key.pem';
const VPS_HOST = process.env.VPS_HOST || 'ubuntu@15.156.83.71';

let cachedPayload = null;
let isFetching = false;
let lastFetchTime = 0;

async function pollVps() {
  if (isFetching) return;
  isFetching = true;
  try {
    let cmd;
    if (process.env.LOCAL_MODE === 'true' || (!process.env.VPS_HOST && !existsSync(SSH_KEY))) {
      // Direct local execution (e.g. running directly on VPS or locally)
      cmd = `node "${path.join(__dirname, '..', 'export-trades.mjs')}"`;
    } else {
      cmd = `ssh -o StrictHostKeyChecking=no -i ${SSH_KEY} ${VPS_HOST} "node ~/daily-bot/export-trades.mjs"`;
    }
    const { stdout } = await execAsync(cmd, { timeout: 8000 });
    const parsed = JSON.parse(stdout.trim());
    if (parsed && Array.isArray(parsed.trades)) {
      cachedPayload = parsed;
      lastFetchTime = Date.now();
    }
  } catch (err) {
    // Keep previous cache if transient network drop
    console.error('[SYNC ERROR]', err.message);
  } finally {
    isFetching = false;
  }
}

// Initial poll + background loop every 3 seconds
pollVps();
setInterval(pollVps, 3000);

// The market maker in front of all our fills is one vault (owner F9sT6b..., read on chain 2026-11-07). Its USDC is what
// it can put against our bets; it fell 2774 -> 1927 in the 24 h to 2026-11-07 00:18 UTC. Read once a minute from the
// public Solana RPC; samples kept in reserve-history.jsonl next to this file so the curve survives a restart.
const SOLANA_RPC = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const RESERVE_OWNER = process.env.RESERVE_OWNER || 'F9sT6bbtmT1bo7VA5eCbGe5QTiZ3AMW36ERH9DohGEj5';
const RESERVE_LOW = +(process.env.RESERVE_LOW || 800);
const RESERVE_FILE = process.env.RESERVE_FILE || path.join(__dirname, 'reserve-history.jsonl');
const reserve = existsSync(RESERVE_FILE) ? readFileSync(RESERVE_FILE, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)).sort((a, b) => a.t - b.t) : [];
let reserveError = null;
async function pollReserve() {
  try {
    const r = await fetch(SOLANA_RPC, { method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(10000),
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getTokenAccountsByOwner', params: [RESERVE_OWNER, { mint: USDC_MINT }, { encoding: 'jsonParsed' }] }) });
    const j = await r.json(); if (j.error) throw new Error(j.error.message);
    const s = { t: Date.now(), usdc: +j.result.value.reduce((a, v) => a + (v.account.data.parsed.info.tokenAmount.uiAmount || 0), 0).toFixed(2) };
    reserve.push(s); appendFileSync(RESERVE_FILE, JSON.stringify(s) + '\n'); reserveError = null;
  } catch (err) { reserveError = err.message; console.error('[RESERVE ERROR]', err.message); }
}
pollReserve();
setInterval(pollReserve, 60e3);

// The reserve over the same buckets as the accounts (last sample at or before each bucket's end), its change over
// 1 h / 6 h / 24 h / the window, and our refusals that name low liquidity or capacity rather than a moved price.
function reserveView(d, from, now, n = 144) {
  const ms = (now - from) / n, timeline = [];
  let i = 0, v = null;
  for (let k = 0; k < n; k++) { const end = from + (k + 1) * ms; while (i < reserve.length && reserve[i].t <= end) v = reserve[i++].usdc; timeline.push({ t: Math.round(from + k * ms), usdc: v }); }
  const last = reserve.at(-1), before = (t) => { let x = null; for (const s of reserve) { if (s.t > t) break; x = s.usdc; } return x; };
  const change = (t) => { const b = before(t); return last && b != null ? +(last.usdc - b).toFixed(2) : null; };
  const low = (d.orders || []).filter((o) => o.t > now - 3600e3 && o.st === 'refused' && /USDC_LOW|VAULT|CAPACITY/i.test(o.code || ''));
  const out = { now: last?.usdc ?? null, at: last?.t ?? null, low: RESERVE_LOW, owner: RESERVE_OWNER, from, to: now, timeline,
    change: { h1: change(now - 3600e3), h6: change(now - 6 * 3600e3), h24: change(now - 24 * 3600e3), all: change(from) },
    lowRefusals1h: low.length, alerts: [] };
  const add = (code, level, text) => out.alerts.push({ key: `Réserve|${code}`, level, text: `Réserve en face : ${text}` });
  if (last && last.usdc < RESERVE_LOW) add('low', 'warn', `${last.usdc} USDC, sous ${RESERVE_LOW}`);
  if (!last || now - last.t > 5 * 60e3) add('stale', 'warn', `pas lue depuis ${last ? Math.round((now - last.t) / 60e3) + ' min' : 'le démarrage'}${reserveError ? ` (${reserveError})` : ''}`);
  if (low.length) add('refusals', 'warn', `${low.length} refus pour liquidité ou capacité sur la dernière heure`);
  return out;
}

// The page is euphoria-bot's dashboard/fleet.html (one panel per account, 6 h / 24 h / 72 h, click + drag for a period).
// Here the bot and the LightGBM leg are its two accounts, in one wallet. Same fields as euphoria-bot lib/fleet.mjs
// botView, built from the VPS export: buckets by entry time, P&L and stake of settled positions, orders and refusals.
const r2 = (x) => +Number(x).toFixed(2);
const sum = (a, f) => a.reduce((s, x) => s + (f(x) || 0), 0);
function fleetView(d, hours, now = Date.now(), n = 144) {
  const from = now - hours * 3600e3, ms = (now - from) / n, hb = d.heartbeat || {}, cfg = d.config || {}, leg = d.leg;
  const view = (owner, name, extra) => {
    const pos = d.trades.filter((t) => t.owner === owner).map((t) => ({ ...t, at: Date.parse(t.time) }));
    const ord = (d.orders || []).filter((o) => o.owner === owner);
    const B = Array.from({ length: n }, (_, i) => ({ t: Math.round(from + i * ms), pnl: 0, staked: 0, n: 0, rej: 0, asked: 0, k: 0 }));
    for (const o of ord) { const b = B[Math.floor((o.t - from) / ms)]; if (b) { b.n++; if (o.st === 'refused') b.rej++; } }
    for (const p of pos) { const b = B[Math.floor((p.at - from) / ms)]; if (!b) continue; b.asked += p.cost; b.k++; if (p.pnl != null) { b.staked += p.cost; b.pnl += p.pnl; } }
    let cum = 0;
    const timeline = B.map((b) => ({ t: b.t, pnl: r2(b.pnl), staked: r2(b.staked), n: b.n, rej: b.rej, bet: b.k ? r2(b.asked / b.k) : null, cum: r2(cum += b.pnl) }));
    const win = (h) => {
      const a = now - h * 3600e3, p = pos.filter((x) => x.at >= a && x.pnl != null), o = ord.filter((x) => x.t >= a), pnl = sum(p, (x) => x.pnl), st = sum(p, (x) => x.cost);
      return { pnl: r2(pnl), staked: r2(st), pct: st ? +(100 * pnl / st).toFixed(1) : null, orders: o.length, refusedPct: o.length ? +(100 * o.filter((x) => x.st === 'refused').length / o.length).toFixed(1) : 0 };
    };
    const v = { name, from, to: now, timeline, windows: { m15: win(0.25), h1: win(1), h6: win(Math.min(6, hours)), all: win(hours) },
      changes: (d.runs || []).filter((r) => r.what === owner && r.t >= from).map((r) => ({ t: r.t, text: `redémarrage : ${r.note}` })), jumps: [],
      runStartedAt: (d.runs || []).filter((r) => r.what === owner && !r.note.startsWith('préchargement')).map((r) => new Date(r.t).toISOString()).at(-1), ...extra };
    const out = [], add = (code, level, text) => out.push({ key: `${name}|${code}`, level, text: `${name}: ${text}` });
    for (const a of v.alertList || []) add(...a);
    const claim = sum(pos, (x) => x.toClaim);
    if (claim > 0) add('claim', 'warn', `${claim.toFixed(2)} $ gagnés pas encore versés au wallet`);
    if (v.state !== 'stopped' && v.windows.h1.pnl < -30) add('loss', 'warn', `${v.windows.h1.pnl.toFixed(0)} $ sur la dernière heure${v.windows.h1.pct != null ? ` (${v.windows.h1.pct} %/$)` : ''}`);
    delete v.alertList;
    return { ...v, alerts: out };
  };
  const silent = hb.ageS == null || hb.ageS > 30, s = hb.sigmaM, vc = d.venueCheck || {}, F = hb.floor == null ? null : r2(hb.floor);
  const bot = view('bot', 'Bot', {
    state: silent ? 'stopped' : !hb.authed ? 'paused' : s == null ? 'warming-up' : s >= cfg.calmVolThreshold ? 'paused' : 'running',
    // One pool since 047e5b8: both stake a fraction of the whole wallet and share the wallet's trailing floor.
    balance: hb.bot ?? hb.usdc, floor: F, sizing: { clicks: 1, stake: r2(hb.usdc * cfg.sizingFraction) },
    flag: hb.authed ? 'ok' : 'blocked', flagWhy: hb.authed ? 'authentifié sur la plateforme' : 'non authentifié',
    note: `VPS ~/daily-bot · σ ${s == null ? '-' : s.toFixed(2)} (pas d'entrée si ≥ ${cfg.calmVolThreshold}) · pic ${d.peak} $ · plateforme ${vc.sides && Math.abs(vc.ours - vc.venue) < 0.05 ? 'concorde' : `écart ${r2((vc.ours ?? 0) - (vc.venue ?? 0))} $`}`,
    alertList: [
      silent && ['down', 'bad', `plus de heartbeat depuis ${hb.ageS ?? '?'} s`],
      !silent && !hb.authed && ['auth', 'bad', 'non authentifié sur la plateforme'],
      F != null && hb.usdc - F < 0.1 * F && ['floor', 'warn', `wallet ${r2(hb.usdc)} $ proche du plancher ${F} $`],
      vc.sides && Math.abs(vc.ours - vc.venue) >= 0.05 && ['venue', 'warn', `P&L calculé ${vc.ours} $, plateforme ${vc.venue} $`],
    ].filter(Boolean),
  });
  if (!leg) return { at: now, hours, bots: [bot, { name: 'LightGBM', error: 'jambe off' }], positions: [] };
  const lo = (d.orders || []).filter((o) => o.owner === 'lgb' && o.t > now - 3600e3), lr = lo.filter((o) => o.st === 'refused').length;
  const lgb = view('lgb', 'LightGBM', {
    state: leg.halted ? 'stopped' : now < (leg.trader?.warmFrom ?? 0) * 1000 ? 'warming-up' : 'running',
    balance: leg.cash, floor: F, sizing: { clicks: 1, stake: r2(hb.usdc * cfg.legCapFrac) },
    flag: leg.halted ? 'blocked' : 'ok', flagWhy: leg.halted ? `achats arrêtés : wallet sous le plancher ${F} $` : 'les ordres partent',
    note: `même wallet, P&L compté à part · ${Math.round(cfg.legCapFrac * 100)} % du wallet par round · ${(leg.open || []).length} position(s) ouverte(s) · trader : ${leg.trader?.sent ?? 0} ordres envoyés`,
    alertList: [
      leg.halted && ['halted', 'bad', `achats arrêtés, wallet sous le plancher ${F} $`],
      lo.length >= 4 && lr / lo.length > 0.5 && ['refused', 'warn', `${lr} de ses ${lo.length} ordres de la dernière heure refusés par la plateforme`],
    ].filter(Boolean),
  });
  return { at: now, hours, bots: [bot, lgb], positions: d.trades.filter((t) => Date.parse(t.time) >= from) };
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  
  if (url.pathname === '/' || url.pathname === '/updown') {
    const html = readFileSync(HTML_PATH, 'utf8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
  }

  if (url.pathname === '/api/fleet') {
    if (!cachedPayload || Date.now() - lastFetchTime > 5000) await pollVps();
    const hours = Math.min(168, Math.max(1, +url.searchParams.get('hours') || 24));
    res.writeHead(cachedPayload ? 200 : 503, { 'Content-Type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    const v = cachedPayload && fleetView(cachedPayload, hours);
    if (v) v.reserve = reserveView(cachedPayload, v.at - hours * 3600e3, v.at);
    return res.end(JSON.stringify(v || { error: 'pas encore de données du VPS' }));
  }

  if (url.pathname === '/api/trades') {
    // If cache is older than 5s or empty, trigger quick fetch
    if (!cachedPayload || Date.now() - lastFetchTime > 5000) {
      await pollVps();
    }
    const origin = req.headers.origin;
    const allowedOrigin = origin && (origin.startsWith('http://localhost') || origin.startsWith('http://127.0.0.1')) ? origin : 'http://localhost:8789';
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': allowedOrigin
    });
    return res.end(JSON.stringify(cachedPayload || { trades: [], balance: 49.22 }));
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[DASHBOARD READY] http://localhost:${PORT}`);
  console.log(`[STANDALONE FILE] file://${HTML_PATH}`);
});
