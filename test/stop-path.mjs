#!/usr/bin/env node
// Offline check of the buy and stop path. Runs the real bot file with fake sockets and fake HTTP: no network, no keys.
//   node test/stop-path.mjs [botFile]                 run the scenarios, exit 1 on any failure
//   node test/stop-path.mjs --build-probe <out> [bot] write the one-shot live probe build
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MAX_U64 = '18446744073709551615';

// Adds one forced buy, then forces the stop on that round. Normal code paths do the rest.
function withProbeHooks(src) {
  const swaps = [
    [
      'function evaluateStrategy() {',
      `const PROBE_SHARES = Number(process.env.STOP_PROBE_SHARES);
let probeSlug = null;
function stopProbeTick(now, sigmaM) {
  if (probeSlug) return;
  for (const [slug, m] of markets.entries()) {
    if (m.symbol !== 'BTC' || !m.startMs || !m.endMs || tradedRounds.has(slug)) continue;
    const rem = (m.endMs - now) / 1000;
    if (rem < 92 || rem > 104) continue;
    const f0 = getPriceAt(priceHistory, m.startMs);
    if (!f0) continue;
    const d = currentBfPrice - f0;
    const targetOutcome = d > 0 ? 0 : 1;
    const book = books.get(\`\${m.marketPda}:\${targetOutcome}\`);
    const oppBook = books.get(\`\${m.marketPda}:\${1 - targetOutcome}\`);
    if (!book?.asks?.[0] || !book?.bids?.[0] || !oppBook || book.stale) continue;
    if (now - book.receivedAtMs > 3000) continue;
    const ask = book.asks[0].price, bid = book.bids[0].price;
    if (ask < 0.25 || ask > 0.8 || bid < 0.2) continue;
    probeSlug = slug;
    tradedRounds.add(slug);
    saveBotState();
    console.log(new Date().toISOString(), \`[STOP PROBE] buying \${PROBE_SHARES} on \${slug} ask=\${ask} bid=\${bid}\`);
    executeTrade({ slug, market: m, remainingSeconds: rem, tau: rem, f0, f_now: currentBfPrice, d, sigmaM: sigmaM || 0, z: 0, q: 0.99,
      targetOutcome, targetSideName: targetOutcome === 0 ? 'up' : 'down', book, oppBook, level1Ask: ask, level1Sz: book.asks[0].sizeShares, edge: 0 });
    return;
  }
}

function evaluateStrategy() {`,
    ],
    ['if (qSurv <= CONFIG.stopLossMaxQ || relDelta <= -CONFIG.stopLossMaxAdverseUsd) {', 'if (qSurv <= CONFIG.stopLossMaxQ || relDelta <= -CONFIG.stopLossMaxAdverseUsd || slug === probeSlug) {'],
    ['  // 2. GATES FOR NEW ENTRIES ONLY', '  stopProbeTick(now, sigmaM);\n  // 2. GATES FOR NEW ENTRIES ONLY'],
    ['let sharesToBuy = Math.floor(Math.min(sharesTarget, maxFillableShares));', 'let sharesToBuy = PROBE_SHARES;'],
  ];
  for (const [from, to] of swaps) {
    if (src.split(from).length !== 2) throw new Error(`probe hook does not match exactly once: ${from}`);
    src = src.replace(from, () => to);
  }
  return src;
}

const FAKE_WS = String.raw`
const { EventEmitter } = require('node:events');
const scenario = process.env.SCENARIO;
const PDA = 'So11111111111111111111111111111111111111112';
const startSec = Math.ceil(Date.now() / 1000) + 2;
const endSec = startSec + 96;
const slug = 'btc-updown-5m-' + startSec;
const level = (price) => [{ price, sizeShares: 100 }];
const book = (tokenId) => ({ type: 'updown_book', tokenId, bids: level(0.5), asks: level(0.52), stale: false });
let sells = 0, buys = 0;
function note(frame) {
  const i = frame.intent;
  console.log('HARNESS ' + JSON.stringify({ t: Date.now(), side: i.side, maxShares: i.maxShares, maxNotional: i.maxNotional, minFill: i.minFillShares, limit: i.limitPriceMicros, hasCash: 'cashAuthorization' in frame }));
}
class WebSocket extends EventEmitter {
  static OPEN = 1;
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 1;
    setTimeout(() => this.emit('open'), 10);
    if (url.includes('fstream')) {
      let p = 80000;
      setInterval(() => { p += 0.5; this.emit('message', JSON.stringify({ b: String(p), a: String(p + 0.1) })); }, 100);
    }
  }
  reply(o, delay = 60) { setTimeout(() => this.emit('message', JSON.stringify(o)), delay); }
  send(raw) {
    const m = JSON.parse(raw);
    if (m.type === 'auth') this.reply({ type: 'authed', wallet: 'x' });
    if (m.type === 'updown_subscribe') {
      this.reply({ type: 'updown_markets', markets: [{ slug, symbol: 'BTC', marketPda: PDA, feeRateBps: 700, roundId: 'updown:BTC:' + startSec + ':' + endSec,
        outcomes: [{ tokenId: PDA + ':0', book: { bids: level(0.5), asks: level(0.52), stale: false } }, { tokenId: PDA + ':1', book: { bids: level(0.46), asks: level(0.48), stale: false } }] }] });
      setInterval(() => { this.emit('message', JSON.stringify(book(PDA + ':0'))); this.emit('message', JSON.stringify(book(PDA + ':1'))); }, 500);
    }
    if (m.type === 'daily_markets_trade') { note(m); this.onTrade(m); }
  }
  accept(m, size, withProcessed = true) {
    this.reply({ type: 'daily_markets_trade_executed', requestId: m.requestId, execution: { side: m.intent.side, state: 'prepared', filledSize: '0', acceptedSize: String(size), market: m.intent.market } });
    if (withProcessed) this.reply({ type: 'daily_markets_trade_processed', requestId: m.requestId, status: 'processed' }, 300);
  }
  reject(m) { this.reply({ type: 'daily_markets_error', requestId: m.requestId, stage: 'execute', code: 'MATCHER_LIQUIDITY_CHANGED', message: 'x' }); }
  onTrade(m) {
    if (m.intent.side === 'buy') {
      buys++;
      if (scenario === 'buy_rejected') return this.reject(m);
      if (scenario === 'buy_silent') return; // the venue never answers this buy
      return this.accept(m, m.intent.maxShares);
    }
    sells++;
    if (scenario === 'sell_lost' && sells === 1) { // accepted, confirmation lost, the venue position list shows 0
      this.accept(m, m.intent.maxShares, false);
      return this.reply({ type: 'updown_update', nativePositions: [{ market: { marketPda: PDA }, outcomeIndex: m.intent.outcomeIndex, quantity: '0' }] }, 500);
    }
    if (scenario === 'partial') return this.accept(m, sells === 1 ? 4000000 : m.intent.maxShares);
    if (scenario === 'sell_rejected' && sells === 1) return this.reject(m);
    if (scenario === 'no_processed' && sells === 1) return this.accept(m, m.intent.maxShares, false);
    return this.accept(m, m.intent.maxShares);
  }
  close() { this.readyState = 3; }
  terminate() { this.readyState = 3; }
  ping() {}
}
module.exports = WebSocket;
`;

const FAKE_FETCH = String.raw`
const key = 'So11111111111111111111111111111111111111112';
globalThis.fetch = async (url, opts) => {
  const body = opts?.body ? JSON.parse(opts.body) : {};
  if (String(url).includes('trading-session')) { // session and prewarm: 300 ms each, like the real API
    console.log('HARNESS_FETCH ' + JSON.stringify({ t: Date.now(), u: String(url).includes('prewarm') ? 'prewarm' : 'session' }));
    await new Promise((r) => setTimeout(r, 300));
  }
  let json = {};
  if (String(url).includes('trading-session?')) {
    json = { tradingSession: { vault: key, session: key, cashSession: { enabled: true, nextNonce: '5', programId: key, deploymentDomain: '00'.repeat(32),
      vault: key, session: key, sessionGeneration: '1', adapterProgram: key, destination: key } } };
  } else if (body.method === 'getTokenAccountBalance') {
    json = { result: { value: { uiAmount: 400 } } };
  }
  return { ok: true, status: 200, json: async () => json };
};
`;

function runScenario(botSource, scenario, ms, seedState, extraEnv = {}, seedFiles = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'stop-path-'));
  if (seedState) fs.writeFileSync(path.join(dir, 'bot-state.json'), JSON.stringify(seedState));
  for (const [name, body] of Object.entries(seedFiles)) fs.writeFileSync(path.join(dir, name), body);
  const nm = path.join(dir, 'node_modules');
  fs.mkdirSync(path.join(nm, 'ws'), { recursive: true });
  fs.writeFileSync(path.join(nm, 'ws', 'index.js'), FAKE_WS);
  for (const dep of ['@solana', 'tweetnacl']) fs.symlinkSync(path.join(process.env.REAL_NODE_MODULES || path.join(ROOT, 'node_modules'), dep), path.join(nm, dep));
  fs.writeFileSync(path.join(dir, 'fetch-stub.mjs'), FAKE_FETCH);
  fs.writeFileSync(path.join(dir, 'package.json'), '{"type":"module"}');
  fs.writeFileSync(path.join(dir, 'bot.mjs'), botSource);
  const env = { PATH: process.env.PATH, SCENARIO: scenario, STOP_PROBE_SHARES: '6', OWNER_ADDRESS: '11111111111111111111111111111111',
    SESSION_SECRET_KEY: JSON.stringify(Array(64).fill(7)), PRIVY_TOKEN: 'x', PRIVY_REFRESH_TOKEN: 'x', ENV_FILE: path.join(dir, 'none'), AUTH_FILE: path.join(dir, 'none'), ...extraEnv };
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ['--import', path.join(dir, 'fetch-stub.mjs'), 'bot.mjs'], { cwd: dir, env });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    child.stderr.on('data', (d) => (out += d));
    setTimeout(() => child.kill('SIGKILL'), ms);
    child.on('close', () => {
      const frames = out.split('\n').filter((l) => l.startsWith('HARNESS ')).map((l) => JSON.parse(l.slice(8)));
      const statePath = path.join(dir, 'bot-state.json');
      const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, 'utf8')) : null;
      const histPath = path.join(dir, 'price-history.json');
      const saved = fs.existsSync(histPath) ? JSON.parse(fs.readFileSync(histPath, 'utf8')) : null;
      fs.rmSync(dir, { recursive: true, force: true });
      resolve({ frames, state, out, saved });
    });
  });
}

let failures = 0;
function check(name, ok) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
  if (!ok) failures++;
}

const argv = process.argv.slice(2);
if (argv[0] === '--build-probe') {
  fs.writeFileSync(argv[1], withProbeHooks(fs.readFileSync(argv[2] || path.join(ROOT, 'daily-s6-bot.mjs'), 'utf8')));
  process.exit(0);
}

const bot = withProbeHooks(fs.readFileSync(argv[0] || path.join(ROOT, 'daily-s6-bot.mjs'), 'utf8'));
// 10 min of saved ticks ending `ageMs` ago, as a previous run would leave them
const savedTicks = (ageMs) => { const end = Date.now() - ageMs, out = []; let p = 80000; for (let t = end - 600000; t <= end; t += 100) { p += Math.sin(t / 7000) * 0.8; out.push([t, p]); } return JSON.stringify(out); };
const [partial, rejected, sellRej, noProc, floored, sellLost, buySilent, reset, histFresh, histStale] = await Promise.all([
  runScenario(bot, 'partial', 13000),
  runScenario(bot, 'buy_rejected', 11000),
  runScenario(bot, 'sell_rejected', 13000),
  runScenario(bot, 'no_processed', 18000),
  runScenario(bot, 'floor', 6000, { tradedRounds: [], activePositions: [], peakBalance: 700 }), // balance 400 < 0.65 x 700
  runScenario(bot, 'sell_lost', 10000),
  runScenario(bot, 'buy_silent', 21000),
  runScenario(bot, 'floor', 6000, { tradedRounds: [], activePositions: [], peakBalance: 700 }, { RESET_PEAK: '1', SIZING_FRACTION: '0.2' }),
  runScenario(bot, 'history', 16000, null, {}, { 'price-history.json': savedTicks(2000) }),
  runScenario(bot, 'history', 3000, null, {}, { 'price-history.json': savedTicks(120000) }),
]);

const sells = (r) => r.frames.filter((f) => f.side === 'sell');
const openCount = (r) => (r.state?.activePositions || []).length;

let s = sells(partial);
check('partial: buy sent with its cash authorization', partial.frames[0]?.side === 'buy' && partial.frames[0].hasCash);
check('partial: sell has no cashAuthorization', s.length > 0 && !s[0].hasCash);
check('partial: sell maxNotional is the u64 maximum', s[0]?.maxNotional === MAX_U64);
check('partial: sell minFillShares is 1', s[0]?.minFill === '1');
check('partial: first sell asks for all 6 shares', s[0]?.maxShares === '6000000');
check('partial: remainder of 2 shares is sold next', s[1]?.maxShares === '2000000');
check('partial: exactly 2 sells', s.length === 2);
check('partial: position closed after the last confirmation', partial.out.includes('[CASHOUT FILLED]') && openCount(partial) === 0);

check('buy_rejected: no sell sent', sells(rejected).length === 0);
check('buy_rejected: no position kept', rejected.out.includes('[BUY REJECTED]') && openCount(rejected) === 0);

s = sells(sellRej);
check('sell_rejected: retried after the rejection', s.length === 2);
check(`sell_rejected: retry ${s[1]?.t - s[0]?.t} ms after the first sale, 0.25-0.8 s`, s.length === 2 && s[1].t - s[0].t >= 250 && s[1].t - s[0].t <= 800);
check(`sell_rejected: first limit ${s[0]?.limit} is the bid minus 5 c`, s.length === 2 && Number(s[0].limit) === 450000);
check('sell_rejected: position closed after the retry', openCount(sellRej) === 0 && sellRej.out.includes('[CASHOUT FILLED]'));

s = sells(noProc);
check('no_processed: no second sell before 6 s', s.length >= 2 && s[1].t - s[0].t >= 5900);
check('no_processed: prepared alone does not close the position', !noProc.out.split('\n').some((l, i, a) => l.includes('[CASHOUT FILLED]') && s.length < 2));

const logMs = (out, tag) => { const l = out.split('\n').find((x) => x.includes(tag)); return l ? Date.parse(l.slice(0, 24)) : NaN; };
const buyLag = partial.frames[0]?.t - logMs(partial.out, '[STOP PROBE]');
const sellLag = sells(partial)[0]?.t - logMs(partial.out, '[STOP LOSS / CASH OUT TRIGGERED]');
check(`latency: buy sent ${buyLag} ms after the signal, under 150 ms with 300 ms HTTP`, buyLag < 150);
check(`latency: sell sent ${sellLag} ms after the trigger, under 150 ms`, sellLag < 150);
check('latency: the market was prewarmed before the buy', partial.out.split('\n').some((l) => l.startsWith('HARNESS_FETCH') && l.includes('prewarm') && JSON.parse(l.slice(14)).t < partial.frames[0]?.t));

check('floor: peak restored and kept at 700', floored.out.includes('peakBalance=700$') && floored.state?.peakBalance === 700);
check('floor: entries paused at 455', floored.out.includes('<= floor 455.00$'));
check('floor: first balance becomes the peak, no pause', partial.state?.peakBalance === 400 && !partial.out.includes('[FLOOR]'));

s = sells(sellRej);
check('sell_rejected: the retry limit is 2 c lower', s.length === 2 && BigInt(s[0].limit) - BigInt(s[1].limit) === 20000n);
s = sells(sellLost);
check('sell_lost: venue position at 0 closes the position', sellLost.out.includes('[VENUE SYNC]') && openCount(sellLost) === 0);
check('sell_lost: no second sell after the lost confirmation', s.length === 1);
check('buy_silent: nothing sold before the buy is confirmed', sells(buySilent).length === 0);
check('buy_silent: unconfirmed buy dropped after 15 s', buySilent.out.includes('[BUY UNCONFIRMED]') && openCount(buySilent) === 0);
check('reset: RESET_PEAK=1 drops the stored peak', reset.out.includes('[RESET_PEAK]') && reset.state?.peakBalance === 400 && !reset.out.includes('[FLOOR]'));
check('reset: SIZING_FRACTION above 5 % is ignored with a warning', reset.out.includes('SIZING_FRACTION=0.2 ignored'));

const firstHb = (r) => (r.out.split('\n').find((l) => l.includes('[HEARTBEAT]')) || '');
check('history: a fresh save is restored and the first heartbeat has a sigma', histFresh.out.includes('[HISTORY RESTORED]') && /sigmaM=\d/.test(firstHb(histFresh)));
check('history: the bot rewrites the file with its live ticks after the restored ones', histFresh.saved?.length > 6001 && histFresh.saved.at(-1)[0] - histFresh.saved[6000][0] > 5000);
check('history: a save older than 60 s is skipped and the bot warms up', histStale.out.includes('[HISTORY SKIPPED]') && firstHb(histStale).includes('sigmaM=warmup'));
check('history: without a save the bot warms up as before', firstHb(partial).includes('sigmaM=warmup'));
console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
process.exit(failures ? 1 : 0);
