#!/usr/bin/env node
// Offline check of the buy and stop path. Runs the real bot file with fake sockets and fake HTTP: no network, no keys.
//   node test/stop-path.mjs [botFile]                 run the scenarios, exit 1 on any failure
//   node test/stop-path.mjs --build-probe <out> [bot] write the one-shot live probe build
import { spawn } from 'node:child_process';
import dgram from 'node:dgram';
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
const endSec = startSec + (scenario === 'lgb_settle' ? 14 : 96);
const slug = 'btc-updown-5m-' + startSec;
console.log('HARNESS_SLUG ' + slug);
const level = (price) => [{ price, sizeShares: 100 }];
const book = (tokenId) => ({ type: 'updown_book', tokenId, bids: level(0.5), asks: level(0.52), stale: false });
let sells = 0, buys = 0;
function note(frame) {
  const i = frame.intent;
  console.log('HARNESS ' + JSON.stringify({ t: Date.now(), side: i.side, maxShares: i.maxShares, maxNotional: i.maxNotional, minFill: i.minFillShares, limit: i.limitPriceMicros, hasCash: 'cashAuthorization' in frame, nonce: frame.cashAuthorization?.nonce, outcome: i.outcomeIndex }));
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
    if (m.type === 'auth' && scenario === 'privy_rotate' && m.token !== 'eyJ.rotated.t') return this.reply({ type: 'error', code: 'unauthorized', message: 'bad token' });
    if (m.type === 'auth') this.reply({ type: 'authed', wallet: 'x' });
    if (m.type === 'updown_subscribe') {
      if (scenario === 'lgb_shared') this.reply({ type: 'updown_update', nativePositions: [{ market: { marketPda: PDA }, outcomeIndex: 1, quantity: '11000000' }] }, 200);
      if (scenario === 'lgb_stale') // bot 6 + leg 8 up shares, and a venue list written before the bot's fill that shows only the leg's 8
        this.reply({ type: 'updown_update', nativePositions: [{ market: { marketPda: PDA }, outcomeIndex: 0, quantity: '8000000', lastTradeAt: new Date(Date.now() - 140000).toISOString() }] }, 200);
      this.reply({ type: 'updown_markets', markets: [{ slug, symbol: 'BTC', marketPda: PDA, marketId: 'MID1', feeRateBps: 700, roundId: 'updown:BTC:' + startSec + ':' + endSec,
        outcomes: [{ tokenId: PDA + ':0', book: { bids: level(0.5), asks: level(0.52), stale: false } }, { tokenId: PDA + ':1', book: { bids: level(0.46), asks: level(0.48), stale: false } }] }] });
      setInterval(() => { this.emit('message', JSON.stringify(book(PDA + ':0'))); this.emit('message', JSON.stringify(book(PDA + ':1'))); }, 500);
    }
    if (m.type === 'daily_markets_trade') { note(m); this.onTrade(m); }
  }
  accept(m, size, withProcessed = true) { // filled at the limit, venue fee 7 % x p x (1 - p) per share
    const sz = BigInt(size), lim = BigInt(m.intent.limitPriceMicros);
    const notional = (sz * lim) / 1000000n, fee = (sz * 700n * lim * (1000000n - lim)) / (10000n * 1000000n * 1000000n);
    this.reply({ type: 'daily_markets_trade_executed', requestId: m.requestId, execution: { side: m.intent.side, state: 'prepared', filledSize: '0', acceptedSize: String(size),
      acceptedNotional: String(notional), acceptedFee: String(fee), market: m.intent.market } });
    if (withProcessed) this.reply({ type: 'daily_markets_trade_processed', requestId: m.requestId, status: 'processed' }, 300);
  }
  reject(m) { this.reply({ type: 'daily_markets_error', requestId: m.requestId, stage: 'execute', code: 'MATCHER_LIQUIDITY_CHANGED', message: 'x' }); }
  onTrade(m) {
    if (m.intent.side === 'buy') {
      buys++;
      if (scenario === 'buy_rejected' || scenario === 'lgb_reject') return this.reject(m);
      if (scenario === 'add_reject' && buys === 2) return this.reject(m); // D1: the add is refused, the entry stays
      if (scenario === 'lgb_collide') { // the leg holds 8 up shares; after the bot's buy, a venue list from before it, then the right one
        this.accept(m, m.intent.maxShares);
        const pos = (qty, at) => ({ type: 'updown_update', nativePositions: [{ market: { marketPda: PDA }, outcomeIndex: m.intent.outcomeIndex, quantity: qty, lastTradeAt: at }] });
        this.reply(pos('8000000', new Date(Date.now() - 140000).toISOString()), 600);
        return this.reply(pos(String(8000000n + BigInt(m.intent.maxShares)), new Date(Date.now() + 400).toISOString()), 650);
      }
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
  if (String(url).includes('auth.privy.io')) json = { token: 'eyJ.rotated.t', refresh_token: 'rotated-refresh' };
  else if (String(url).includes('trading-session?')) {
    json = { tradingSession: { vault: key, session: key, cashSession: { enabled: true, nextNonce: '5', programId: key, deploymentDomain: '00'.repeat(32),
      vault: key, session: key, sessionGeneration: '1', adapterProgram: key, destination: key } } };
  } else if (body.method === 'getTokenAccountBalance') {
    json = { result: { value: { uiAmount: +(process.env.FAKE_USDC || 400) } } };
  } else if (String(url).includes('/daily-markets/MID1')) { // the venue's verdict: Up won
    json = { market: { state: 'resolved', payoutVector: ['1000000', '0'] } };
  }
  return { ok: true, status: 200, json: async () => json };
};
`;

// lgb = { port, replyPort, intents: [{ at, o }] }: runs the LightGBM leg; each intent goes to the bot `at` ms after the fake market
// is announced, with that market's slug; the bot's answers are collected from replyPort.
function runScenario(botSource, scenario, ms, seedState, extraEnv = {}, seedFiles = {}, lgb = null) {
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
    SESSION_SECRET_KEY: JSON.stringify(Array(64).fill(7)), PRIVY_TOKEN: 'x', PRIVY_REFRESH_TOKEN: 'x', ENV_FILE: path.join(dir, 'none'), AUTH_FILE: path.join(dir, 'none'),
    ...(lgb ? { LGB_PORT: String(lgb.port), LGB_REPLY_PORT: String(lgb.replyPort), LGB_MIN_SIGMA: '0' } : {}), ...extraEnv }; // the O2 gate is off unless a scenario sets it
  const replies = [];
  const rsock = lgb ? dgram.createSocket('udp4') : null;
  if (rsock) { rsock.on('message', (b) => replies.push(JSON.parse(b))); rsock.bind(lgb.replyPort, '127.0.0.1'); }
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ['--import', path.join(dir, 'fetch-stub.mjs'), 'bot.mjs'], { cwd: dir, env });
    let out = '', slugSeen = false;
    child.stdout.on('data', (d) => {
      out += d;
      const mm = lgb && !slugSeen && out.match(/HARNESS_SLUG (\S+)/);
      if (!mm) return;
      slugSeen = true;
      for (const { at, o } of lgb.intents) {
        setTimeout(() => { const s = dgram.createSocket('udp4'); s.send(JSON.stringify({ slug: mm[1], ...o }), lgb.port, '127.0.0.1', () => s.close()); }, at);
      }
    });
    child.stderr.on('data', (d) => (out += d));
    setTimeout(() => child.kill('SIGKILL'), ms);
    child.on('close', () => {
      const frames = out.split('\n').filter((l) => l.startsWith('HARNESS ')).map((l) => JSON.parse(l.slice(8)));
      const statePath = path.join(dir, 'bot-state.json');
      const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, 'utf8')) : null;
      const histPath = path.join(dir, 'price-history.json');
      const saved = fs.existsSync(histPath) ? JSON.parse(fs.readFileSync(histPath, 'utf8')) : null;
      const lgbPath = path.join(dir, 'lgb-state.json');
      const lgbState = fs.existsSync(lgbPath) ? JSON.parse(fs.readFileSync(lgbPath, 'utf8')) : null;
      const authPath = path.join(dir, 'none'), auth = fs.existsSync(authPath) ? { ...JSON.parse(fs.readFileSync(authPath, 'utf8')), mode: fs.statSync(authPath).mode & 0o777 } : null;
      fs.rmSync(dir, { recursive: true, force: true });
      if (rsock) rsock.close();
      resolve({ frames, state, out, saved, replies, lgbState, auth });
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
// LightGBM leg: the bot file as deployed (no probe hooks); in 25 s it warms up and makes no trade of its own
const rawBot = fs.readFileSync(argv[0] || path.join(ROOT, 'daily-s6-bot.mjs'), 'utf8');
const FAKE_PDA = 'So11111111111111111111111111111111111111112';
const legOrders = [
  { at: 800, o: { k: 'k1', act: 'entry', side: 'up', limit: 0.53, target: 200, p: 0.7 } },
  { at: 900, o: { k: 'k2', act: 'entry', side: 'up', limit: 0.53, target: 200 } },
  { at: 2500, o: { k: 'k3', act: 'add', side: 'up', limit: 0.53, target: 200 } },
  { at: 4000, o: { k: 'k4', act: 'sell', side: 'up', limit: 0.48, shares: 36 } },
  { at: 5500, o: { k: 'k5', act: 'sell', side: 'up', limit: 0.48, shares: 9 } },
];
const seedLeg = (cash, pos = {}) => JSON.stringify({ cash, halted: false, pos, spent: {}, cap: {}, orders: {}, touched: {}, changedAt: 0 });
const peak400 = { tradedRounds: [], activePositions: [], peakBalance: 400 };
const sharedEnd = Date.now() + 90000; // a bot position of 6 down shares and a leg position of 5 on the same side: the venue shows 11
const sharedBot = { tradedRounds: [], peakBalance: 400, activePositions: [['btc-updown-5m-seed', { targetOutcome: 1, targetSideName: 'down', sharesBought: 6, f0: 80000,
  entryPrice: 0.48, entryTau: 200, cashoutAttempted: false, marketPda: FAKE_PDA, marketEndMs: sharedEnd, marketFeeRateBps: 700, buyAccepted: true, buyProcessed: true }]] };
const sharedLeg = seedLeg(95, { [FAKE_PDA + ':1']: { slug: 'btc-updown-5m-seed', marketId: 'MID1', marketPda: FAKE_PDA, outcome: 1, endMs: sharedEnd, shares: 5 } });
const collideLeg = seedLeg(95, { [FAKE_PDA + ':0']: { slug: 'btc-updown-5m-seed', marketId: 'MID1', marketPda: FAKE_PDA, outcome: 0, endMs: sharedEnd, shares: 8 } });
const staleBot = { tradedRounds: [], peakBalance: 400, activePositions: [['btc-updown-5m-seed', { targetOutcome: 0, targetSideName: 'up', sharesBought: 6, f0: 79000,
  entryPrice: 0.48, entryTau: 200, cashoutAttempted: false, marketPda: FAKE_PDA, marketEndMs: sharedEnd, marketFeeRateBps: 700, buyAccepted: true, buyProcessed: true, lastFillAt: Date.now() }]] };
const [legBuy, legSettle, legShared, legHalt, legReject, legCollide, legOrphan, legStale] = await Promise.all([
  runScenario(rawBot, 'lgb_buy', 12500, peak400, {}, {}, { port: 47301, replyPort: 47302, intents: legOrders }),
  runScenario(rawBot, 'lgb_settle', 24000, peak400, {}, {}, { port: 47303, replyPort: 47304, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_shared', 6000, sharedBot, {}, { 'lgb-state.json': sharedLeg }, { port: 47305, replyPort: 47306, intents: [] }),
  runScenario(rawBot, 'lgb_low_cash', 5000, peak400, {}, { 'lgb-state.json': seedLeg(49) }, { port: 47307, replyPort: 47308, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_reject', 6000, peak400, {}, {}, { port: 47309, replyPort: 47310, intents: [legOrders[0]] }),
  runScenario(bot, 'lgb_collide', 13000, peak400, {}, { 'lgb-state.json': collideLeg }, { port: 47311, replyPort: 47312, intents: [] }),
  runScenario(rawBot, 'lgb_shared', 6000, peak400, {}, { 'lgb-state.json': sharedLeg }, { port: 47313, replyPort: 47314, intents: [] }),
  runScenario(rawBot, 'lgb_stale', 6000, staleBot, {}, { 'lgb-state.json': collideLeg }, { port: 47315, replyPort: 47316, intents: [] }),
]);
// one pool: the wallet (400) at or under the floor (0.65 x 700 = 455) blocks the leg's buys as well as the bot's
const legBig = await runScenario(rawBot, 'lgb_buy', 5000, peak400, { FAKE_USDC: '2000' }, {}, { port: 47321, replyPort: 47322, intents: [legOrders[0]] });
const legFloor = await runScenario(rawBot, 'lgb_floor', 5000, { tradedRounds: [], activePositions: [], peakBalance: 700 }, {}, {}, { port: 47317, replyPort: 47318, intents: [legOrders[0]] });
// the session stub always shows nextNonce 5, as the real API did right after a processed buy: the add must sign 6, not 5 again
const legNonce = await runScenario(rawBot, 'lgb_nonce', 6000, peak400, {}, {}, { port: 47319, replyPort: 47320, intents: [
  { at: 800, o: { k: 'k1', act: 'entry', side: 'up', limit: 0.53, target: 10, p: 0.7 } }, { at: 2500, o: { k: 'k2', act: 'add', side: 'up', limit: 0.53, target: 10 } }] });
// daily gain cap: wallet 400, the day began at 300, so +100; a cap of 50 stops new entries, a cap of 150 does not
const today = new Date().toISOString().slice(0, 10);
const dayFrom300 = { ...peak400, dayStart: { day: today, usdc: 300, hit: false } };
const capFile = (cap) => ({ 'day-cap.json': JSON.stringify({ day: today, cap }) });
const [legDayCap, legDayUnder] = await Promise.all([
  runScenario(rawBot, 'lgb_buy', 5000, dayFrom300, {}, capFile(50), { port: 47323, replyPort: 47324, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_buy', 5000, dayFrom300, {}, capFile(150), { port: 47325, replyPort: 47326, intents: [legOrders[0]] }),
]);
// G4 volatility caps: the leg's round cap is 60 USD while sigmaM >= LGB_MIN_SIGMA and 30 under it; a restored history gives a
// numeric sigmaM, none leaves it in warmup. Wallet 2000 so 5 % (100 USD) does not bind.
const hist = { 'price-history.json': savedTicks(2000) };
const [legCalm, legMoving, legWarm, legSellCalm] = await Promise.all([
  runScenario(rawBot, 'lgb_buy', 5000, peak400, { LGB_MIN_SIGMA: '1e9', FAKE_USDC: '2000' }, hist, { port: 47327, replyPort: 47328, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_buy', 5000, peak400, { LGB_MIN_SIGMA: '0.0001', FAKE_USDC: '2000' }, hist, { port: 47329, replyPort: 47330, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_buy', 5000, peak400, { LGB_MIN_SIGMA: '2' }, {}, { port: 47331, replyPort: 47332, intents: [legOrders[0]] }),
  runScenario(rawBot, 'lgb_buy', 5000, peak400, { LGB_MIN_SIGMA: '1e9' }, { 'lgb-state.json': sharedLeg },
    { port: 47333, replyPort: 47334, intents: [{ at: 800, o: { k: 'k1', act: 'sell', side: 'down', limit: 0.48, shares: 5 } }] }),
]);
// H2c: with a window of 2 and 2 refusals to pause, two venue refusals pause the leg's buys; the third buy is dropped as paused
const legPause = await runScenario(rawBot, 'lgb_reject', 6000, peak400, { LGB_PAUSE_WIN: '2', LGB_PAUSE_REFUSED: '2' }, {}, { port: 47335, replyPort: 47336, intents: [
  { at: 800, o: { k: 'k1', act: 'entry', side: 'up', limit: 0.53, target: 200 } },
  { at: 2000, o: { k: 'k2', act: 'entry', side: 'up', limit: 0.53, target: 200 } },
  { at: 3200, o: { k: 'k3', act: 'entry', side: 'up', limit: 0.53, target: 200 } }] });
// D1: the raw bot with a restored history enters on its own (price rising 5 USD/s, up ask 0.52); 10 s later the gate passes again on
// the same side with a larger edge and it adds once. Refused, the add leaves the entry and is retried 10 s later. BOT_ADD_MAX 0: no add.
const [addOn, addRej, addOff] = await Promise.all([
  runScenario(rawBot, 'add', 20000, peak400, { FAKE_USDC: '2000' }, hist),
  runScenario(rawBot, 'add_reject', 32000, peak400, { FAKE_USDC: '2000' }, hist),
  runScenario(rawBot, 'add', 20000, peak400, { FAKE_USDC: '2000', BOT_ADD_MAX: '0' }, hist),
]);
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
check(`sell_rejected: first limit ${s[0]?.limit} is the bid minus 15 c`, s.length === 2 && Number(s[0].limit) === 350000);
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

const rep = (r, k) => r.replies.find((x) => x.k === k) || {};
const near = (a, b) => Math.abs(a - b) < 1e-6;
const legPos = (r) => Object.keys(r.lgbState?.pos || {}).length;
check('leg off without LGB_PORT: no leg file, no leg line', partial.lgbState === null && !partial.out.includes('[LGB'));
check('leg: a new leg ledger leaves the peak on the wallet, 400', legBuy.out.includes('new leg ledger, PnL counted from 100 USD') && legBuy.state?.peakBalance === 400);
let f = legBuy.frames;
check(`leg: entry sized to 5 % of the wallet, ${Number(f[0]?.maxShares) / 1e6} shares at 0.53 with the cash authorization`, f[0]?.side === 'buy' && f[0].maxShares === '36000000' && f[0].limit === '530000' && f[0].hasCash);
check(`leg: fill answered with the venue cost ${rep(legBuy, 'k1').cost} USD (36 x 0.53 + fee)`, rep(legBuy, 'k1').st === 'filled' && rep(legBuy, 'k1').sh === 36 && near(rep(legBuy, 'k1').cost, 19.707732));
check('leg: the trader is told the wallet, 400 USD, for its own 5 % cap', legBuy.replies.some((x) => x.wallet === 400));
check(`leg: an order while the previous one is in flight is dropped (${rep(legBuy, 'k2').why})`, rep(legBuy, 'k2').st === 'dropped' && /pending/.test(rep(legBuy, 'k2').why));
check(`leg: an add beyond the round cap is dropped (${rep(legBuy, 'k3').why})`, rep(legBuy, 'k3').st === 'dropped' && /room/.test(rep(legBuy, 'k3').why));
check('leg: the sale asks for the leg\'s 36 shares, no cash authorization, u64 maxNotional', f[1]?.side === 'sell' && f[1].maxShares === '36000000' && !f[1].hasCash && f[1].maxNotional === MAX_U64);
check(`leg: sale answered with proceeds net of fee ${rep(legBuy, 'k4').cost} USD`, rep(legBuy, 'k4').st === 'filled' && near(rep(legBuy, 'k4').cost, 16.651008));
check('leg: a sale without leg shares is dropped', rep(legBuy, 'k5').st === 'dropped' && f.length === 2);
check(`leg: ledger cash ${legBuy.lgbState?.cash} USD = 100 - 19.707732 + 16.651008, no position`, near(legBuy.lgbState?.cash, 96.943276) && legPos(legBuy) === 0);
check('leg: the heartbeat shows the wallet floor and the two shares, bot = wallet minus the leg ledger', legBuy.out.includes('usdc=400.00$ floor=260.00$ bot=303.06$ lgb=96.94$/0pos'));
check(`leg settle: the venue verdict pays the 36 up shares, cash ${legSettle.lgbState?.cash} USD`, legSettle.out.includes('[LGB SETTLED]') && near(legSettle.lgbState?.cash, 116.292268) && legPos(legSettle) === 0);
s = sells(legShared);
check(`leg shared side: the venue shows 11, the bot's stop sells its own ${Number(s[0]?.maxShares) / 1e6} shares`, s.length >= 1 && s[0].maxShares === '6000000' && !legShared.out.includes('-> 11'));
check('leg shared side: the leg keeps its 5 shares', legShared.lgbState?.pos?.[FAKE_PDA + ':1']?.shares === 5 && legShared.out.includes('[CASHOUT FILLED]'));
check('leg low cash: no 50 USD halt, a leg ledger at 49 USD still buys on the wallet', rep(legHalt, 'k1').st === 'filled' && legHalt.lgbState?.halted === false && legHalt.frames.length === 1);
check(`leg cap: wallet 2000, 5 % would be 100 USD, the entry is capped at 60 USD: ${Number(legBig.frames[0]?.maxShares) / 1e6} shares at 0.53 (109 x 0.547437 = 59.67)`, legBig.frames[0]?.side === 'buy' && legBig.frames[0].maxShares === '109000000' && legBig.out.includes('and 60 USD per round'));
check(`leg floor: wallet 400 under the floor 455, the leg buy is dropped (${rep(legFloor, 'k1').why}) and nothing is sent`, rep(legFloor, 'k1').why === 'floor' && legFloor.frames.length === 0);
check('leg reject: a venue rejection is answered and books nothing', rep(legReject, 'k1').st === 'rejected' && near(legReject.lgbState?.cash, 100) && legPos(legReject) === 0);
s = sells(legCollide);
check('leg stale list: a venue list older than the bot\'s last fill does not close its 6 shares', !legStale.out.includes('venue holds 0 shares')
  && legStale.state?.activePositions?.[0]?.[1]?.sharesBought === 6 && legStale.lgbState?.pos?.[FAKE_PDA + ':0']?.shares === 8);
check(`leg collide: the bot's stop sells its own ${Number(s[0]?.maxShares) / 1e6} shares and the leg keeps its 8`, s[0]?.maxShares === '6000000' && legCollide.out.includes('[CASHOUT FILLED]')
  && legCollide.lgbState?.pos?.[FAKE_PDA + ':0']?.shares === 8 && !legCollide.out.includes('[LGB SYNC]'));
check('leg orphan: venue shares beyond the leg\'s own fills are not booked to the leg (5 stays 5, the venue shows 11)', legOrphan.lgbState?.pos?.[FAKE_PDA + ':1']?.shares === 5 && !legOrphan.out.includes('[LGB SYNC]'));
check(`leg nonce: after a processed buy the next buy signs the next nonce (${legNonce.frames.map((x) => x.nonce)}) though the session still shows 5`,
  legNonce.frames.length === 2 && legNonce.frames[0].nonce === '5' && legNonce.frames[1].nonce === '6' && rep(legNonce, 'k2').st === 'filled');
check('nonce: the bot\'s own first buy signs the session\'s nonce, 5', partial.frames[0]?.nonce === '5');
check(`day cap: +100 since the day's start against a cap of 50, the leg buy is dropped (${rep(legDayCap, 'k1').why}) and nothing is sent`, rep(legDayCap, 'k1').why === 'day cap' && legDayCap.frames.length === 0);
check('day cap: the stop is logged, latched in the state and shown in the heartbeat', legDayCap.out.includes('[DAY CAP]') && legDayCap.state?.dayStart?.hit === true && legDayCap.out.includes('day=100.00$/50.00$/CAPPED'));
check('day cap: the same day under a cap of 150 still buys', rep(legDayUnder, 'k1').st === 'filled' && legDayUnder.state?.dayStart?.hit === false);
check('day cap: without a saved day start, the first flat moment sets it to the wallet, 400, and no file means no cap', legBuy.state?.dayStart?.day === today && legBuy.state.dayStart.usdc === 400 && legBuy.out.includes('day=0.00$/nocap'));
check(`G4: sigmaM under the threshold buys at the 30 USD calm cap (${rep(legCalm, 'k1').st}, ${Number(legCalm.frames[0]?.maxShares) / 1e6} shares at 0.53, 54 x 0.547437 = 29.56)`, rep(legCalm, 'k1').st === 'filled' && legCalm.frames[0]?.maxShares === '54000000');
check(`G4: the same history over the threshold buys at the 60 USD cap (${rep(legMoving, 'k1').st}, ${Number(legMoving.frames[0]?.maxShares) / 1e6} shares)`, rep(legMoving, 'k1').st === 'filled' && legMoving.frames[0]?.maxShares === '109000000');
check(`G4: no sigmaM yet (warmup) drops the buy at a threshold of 2 (${rep(legWarm, 'k1').why})`, rep(legWarm, 'k1').why === 'sigmaM warmup');
check(`G4: a leg sale is never gated (${rep(legSellCalm, 'k1').st} ${rep(legSellCalm, 'k1').why ?? ''})`, rep(legSellCalm, 'k1').st === 'filled');
check(`H2c: two refusals pause the leg's buys (${rep(legPause, 'k1').st} ${rep(legPause, 'k2').st} ${rep(legPause, 'k3').st} ${rep(legPause, 'k3').why ?? ''})`,
  rep(legPause, 'k1').st === 'rejected' && rep(legPause, 'k2').st === 'rejected' && rep(legPause, 'k3').st === 'dropped' && /^paused after refusals/.test(rep(legPause, 'k3').why ?? '') && legPause.out.includes('[LGB PAUSE]'));
// a refreshed Privy session is saved even though no auth.json existed (second instance, 11-09 01:32)
const rot = await runScenario(rawBot, 'privy_rotate', 5000, peak400);
check(`privy: rotated tokens are saved to a new auth.json, mode 600 (${rot.auth ? rot.auth.privyRefreshToken + ' ' + rot.auth.mode.toString(8) : 'none'})`,
  rot.auth?.privyToken === 'eyJ.rotated.t' && rot.auth.privyRefreshToken === 'rotated-refresh' && rot.auth.mode === 0o600 && rot.out.includes('PRIVY AUTO-REFRESH SUCCESS'));
const buysOf = (r) => r.frames.filter((f) => f.side === 'buy'), posOf = (r) => r.state?.activePositions?.[0]?.[1];
{ const b = buysOf(addOn), sh = b.map((f) => Number(f.maxShares) / 1e6);
  check(`D1: one add on the same side >= 10 s after the entry, the position holds both (${b.length} buys, gap ${b[1] ? b[1].t - b[0].t : '-'} ms, ${sh} -> ${posOf(addOn)?.sharesBought})`,
    b.length === 2 && b[1].t - b[0].t >= 10000 && b[1].outcome === b[0].outcome && Math.abs(posOf(addOn)?.sharesBought - sh[0] - sh[1]) < 1e-6 && addOn.out.includes('[ADD FILLED]')); }
{ const b = buysOf(addRej), sh = b.map((f) => Number(f.maxShares) / 1e6);
  check(`D1: a refused add keeps the entry and retries 10 s later (${b.length} buys, gaps ${b.slice(1).map((f, i) => f.t - b[i].t)} ms, position ${posOf(addRej)?.sharesBought})`,
    b.length === 3 && b[2].t - b[1].t >= 10000 && addRej.out.includes('[ADD REJECTED]') && Math.abs(posOf(addRej)?.sharesBought - sh[0] - sh[2]) < 1e-6); }
check(`D1: BOT_ADD_MAX 0 makes no add (${buysOf(addOff).length} buys)`, buysOf(addOff).length === 1);
check('G4: the start line states both caps', legWarm.out.includes('and 60 USD per round (30 while sigmaM < 2)') && !legBuy.out.includes('while sigmaM'));
console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
process.exit(failures ? 1 : 0);
