#!/usr/bin/env node
/**
 * Daily.fun S6 Production Bot (High-Precision Prediction Market Trading Engine)
 * 
 * Model: S6 Forward Edge Formulation (c=1.0, margin=0.05, F20 window=118-122s remaining, calm regime sigma_m < 2.30 $/sqrt(s))
 * Latency: Event-driven WebSocket push (~40-70 ms execution loop)
 * 
 * Note: Strategy parameters (S6 pricing, volatility filter, timing window) are hardcoded in the engine.
 * Credentials (Solana keys, Privy session tokens) are loaded from .env or auth.json.
 */

import WebSocket from 'ws';
import crypto from 'node:crypto';
import dgram from 'node:dgram';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import nacl from 'tweetnacl';
import { PublicKey } from '@solana/web3.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Global Error Handlers
process.on('uncaughtException', (err) => console.error(new Date().toISOString(), 'UNCAUGHT EXCEPTION:', err.stack || err));
process.on('unhandledRejection', (reason) => console.error(new Date().toISOString(), 'UNHANDLED REJECTION:', reason));

// 1. Credentials Loader (.env priority, auth.json fallback)
const ENV_FILE = path.resolve(process.env.ENV_FILE || path.join(__dirname, '.env'));
const AUTH_FILE = path.resolve(process.env.AUTH_FILE || path.join(__dirname, 'auth.json'));

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return {};
  const content = fs.readFileSync(filePath, 'utf8');
  const env = {};
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx > 0) {
      const key = trimmed.slice(0, eqIdx).trim();
      let val = trimmed.slice(eqIdx + 1).trim();
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      env[key] = val;
      if (!process.env[key]) process.env[key] = val;
    }
  }
  return env;
}

const fileEnv = loadEnvFile(ENV_FILE);

let authData = null;
if (fs.existsSync(AUTH_FILE)) {
  try {
    fs.chmodSync(AUTH_FILE, 0o600);
    authData = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8'));
  } catch (e) {
    console.error('Failed to parse auth.json:', e.message);
  }
}
if (fs.existsSync(ENV_FILE)) {
  try {
    fs.chmodSync(ENV_FILE, 0o600);
  } catch (_) {}
}

// Resolve credentials (auth.json priority for rotated session tokens)
const rawOwner = authData?.owner || process.env.OWNER_ADDRESS || fileEnv.OWNER_ADDRESS || null;
const rawSecret = authData?.secretKey || process.env.SESSION_SECRET_KEY || fileEnv.SESSION_SECRET_KEY || null;
let PRIVY_TOKEN = authData?.privyToken || process.env.PRIVY_TOKEN || fileEnv.PRIVY_TOKEN || null;
let PRIVY_REFRESH_TOKEN = authData?.privyRefreshToken || process.env.PRIVY_REFRESH_TOKEN || fileEnv.PRIVY_REFRESH_TOKEN || null;

const isPaperMode = process.argv.includes('--paper') || process.env.PAPER_MODE === 'true';

if (!isPaperMode && (!rawOwner || !rawSecret)) {
  console.error(new Date().toISOString(), 'FATAL: No OWNER_ADDRESS or SESSION_SECRET_KEY found in .env or auth.json.');
  console.error('To run in live mode, provide credentials in .env. To run in simulation mode, add --paper.');
  process.exit(1);
}

function parseSecretKey(val) {
  if (!val) return new Uint8Array(64);
  if (Array.isArray(val)) return Uint8Array.from(val);
  if (typeof val === 'string') {
    val = val.trim();
    if (val.startsWith('[') && val.endsWith(']')) {
      return Uint8Array.from(JSON.parse(val));
    }
    // Check if comma-separated numbers
    if (val.includes(',')) {
      return Uint8Array.from(val.split(',').map(s => parseInt(s.trim(), 10)));
    }
    // Base64
    try {
      const buf = Buffer.from(val, 'base64');
      if (buf.length === 64) return new Uint8Array(buf);
    } catch(e) {}
  }
  return new Uint8Array(64);
}

const OWNER = rawOwner ? new PublicKey(rawOwner) : new PublicKey('11111111111111111111111111111111');
const SECRET_KEY = parseSecretKey(rawSecret);

// 2. Production Configuration & Hardcoded S6 Strategy
const CONFIG = {
  binanceFuturesWs: 'wss://fstream.binance.com/ws/btcusdt@bookTicker',
  dailyWs: 'wss://ws.daily.fun/ws',
  dailyOrigin: 'https://daily.fun',
  dailyUserAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  privyAppId: 'cmnl4x5rw00480cjrnx63r79u',
  tradesLog: path.resolve(process.env.TRADES_LOG || path.join(__dirname, 'trades.jsonl')),
  programId: new PublicKey('GeZwYHaw8x1HhvFojhCYwunK9fqRGZL2zwgJecnFZDKx'),
  usdcMint: new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
  solanaRpcUrl: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
  dailyUsdcAta: process.env.DAILY_USDC_ATA || 'DXpsZgXTarjRiSdMj3jeHodP2Rzv6x86SFohSiEv2Que', // Configurable in .env or auto-updated via cash_account_changed.v1
  paperMode: isPaperMode,

  // R1: best train t-stat of a 324-point grid, chronological 60/40 split
  windowRemainingMin: 50,  // Entry window: from the round open (300 s) down to 50 s remaining
  windowRemainingMax: 300,
  earlyEntryMaxSigma: 3.0, // entries in the round's first 80 s only in a calm market (sigmaM below this)
  lateEntryTau: 220,       // otherwise enter from 220 s remaining
  minZScore: 0.40,         // Minimum statistical conviction Z >= 0.40
  calmVolThreshold: 12.0,  // Allow trading unless extreme volatility (>= 12 $/sqrt(s) over 10 min)
  minSigmaFloor: 2.20,     // Volatility floor to prevent underestimating variance on 5m BTC
  cModel: 1.0,             // S6 forward model scaling factor
  minEdgeMargin: 0.04,     // Edge floor: q - (ask + fee) >= 0.04
  maxEntryAsk: 0.72,       // Maximum allowed entry ask (buy underpriced contracts only)
  // owner's D1 (2026-11-09): one more buy on the same side at the same stake when the entry gate passes again at least 10 s after
  // the last buy, with an edge at least the entry's, before any sale (scratchpad research3/bot-params F4, "add d=0 gap 10": LODO
  // +59 USD/day after the 0.4 haircut on the VPS days, worst day and drawdown unchanged). A refused add is retried 10 s later.
  addMax: Number(process.env.BOT_ADD_MAX ?? 1), addGapMs: Number(process.env.BOT_ADD_GAP_MS ?? 10000), addEdgeDelta: 0,
  // Stop: best train units of a 61-point grid (train 50.6 vs 42.2 for the old 30-90 s / -5 $ rule)
  stopLossMaxQ: 0.45,          // sell when the survival probability falls to 45 %
  stopLossMaxAdverseUsd: 3.0,  // or when the price is 3 $ against us since the round open
  stopLossMinTau: 10,          // active from 300 s (the round open) down to 10 s remaining
  stopLossMaxTau: 300,

  // Capital Preservation Sizing
  sizingFraction: Math.min(0.05, parseFloat(process.env.SIZING_FRACTION) > 0 ? parseFloat(process.env.SIZING_FRACTION) : 0.05), // 5 % of the balance, capped: a higher stake needs a code change
  maxStakeUsd: 30,             // at most 30 USD per position, fees included, for the bot (owner 2026-11-07: the stake stops growing with the wallet)
  lgbMaxStakeUsd: 60,          // the leg's round cap while sigmaM >= lgbMinSigma (owner's O8, 2026-11-08), where its edge was
  lgbCalmStakeUsd: 30,         // the leg's round cap while sigmaM < lgbMinSigma (owner's G4, 2026-11-09): the trader prices calm seconds at a
                               // sigma floor of 1.0 (policy sigma_floor_calm); held out on 11-05..11-08 that added +221 USD/day for bot + leg at
                               // 60 USD after the 0.4 haircut, 3/4 of it on 11-07. 30 halves the extra flow into the vault (not simulated at 30).
  // sigmaM threshold between the two caps (owner's O2 then G4). No sigmaM yet (warmup) drops the buy. LGB_MIN_SIGMA overrides it
  // (the harness sets 0 = always the 60 USD cap, no warmup drop); sells are never gated.
  lgbMinSigma: Number(process.env.LGB_MIN_SIGMA ?? 2),
  maxSharesSafetyCap: 1000,    // Safety ceiling to absorb Level 1 + Level 2 liquidity depth
  trailingFloorDrawdown: 0.35, // New entries stop once the free balance is 35% below its peak (7-day p95 drawdown 24-32%)
  minBalanceFloorMinUsd: 5.0   // Absolute minimum floor to allow small wallets to trade
};

// Solana PDAs Derivation
const AUTHORITY = new PublicKey(nacl.sign.keyPair.fromSecretKey(SECRET_KEY).publicKey);
const [VAULT] = PublicKey.findProgramAddressSync([Buffer.from('vault_v2'), OWNER.toBuffer()], CONFIG.programId);
const [SESSION] = PublicKey.findProgramAddressSync([Buffer.from('trading_session_v4'), VAULT.toBuffer(), AUTHORITY.toBuffer()], CONFIG.programId);

// Check On-Chain Session Expiry
let sessionExpirySec = null;
async function checkSessionStatus() {
  try {
    const res = await fetch(CONFIG.solanaRpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getAccountInfo',
        params: [SESSION.toBase58(), { encoding: 'base64' }]
      })
    }).then(r => r.json());
    if (res?.result?.value?.data?.[0]) {
      const buf = Buffer.from(res.result.value.data[0], 'base64');
      sessionExpirySec = buf.readInt32LE(105);
      const remainingHours = (sessionExpirySec - Date.now() / 1000) / 3600;
      console.log(new Date().toISOString(), `[ON-CHAIN SESSION] Active until ${new Date(sessionExpirySec * 1000).toISOString()} (${remainingHours.toFixed(1)}h remaining)`);
      if (remainingHours <= 1.0) {
        console.warn(new Date().toISOString(), `[WARNING] On-chain session expiring soon! (${remainingHours.toFixed(1)}h)`);
      }
    }
  } catch (e) {
    console.warn(new Date().toISOString(), '[SESSION CHECK WARN] Could not query Solana RPC:', e.message);
  }
}
checkSessionStatus();
setInterval(checkSessionStatus, 30 * 60 * 1000);

// File Watcher on auth.json / .env for hot reloading
if (fs.existsSync(AUTH_FILE)) {
  fs.watchFile(AUTH_FILE, { interval: 1000 }, () => {
    try {
      const updated = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8'));
      authData = updated;
      if (updated.privyToken && updated.privyToken !== PRIVY_TOKEN) {
        console.log(new Date().toISOString(), '[AUTH] auth.json reloaded with new token!');
        authFailed = false;
        PRIVY_TOKEN = updated.privyToken;
        if (dailyWs && dailyWs.readyState === WebSocket.OPEN) {
          dailyWs.send(JSON.stringify({ type: 'auth', token: PRIVY_TOKEN }));
        }
      }
    } catch (e) {}
  });
}

console.log(new Date().toISOString(), 'DAILY M7 CONTINUOUS SNIPER INITIALIZED:');
console.log('  Mode:           ', CONFIG.paperMode ? 'PAPER (Simulation only)' : 'LIVE (Capital Active)');
console.log('  Owner:          ', OWNER.toBase58());
console.log('  Session Auth:   ', AUTHORITY.toBase58());
console.log('  Vault PDA:      ', VAULT.toBase58());
console.log('  Session PDA:    ', SESSION.toBase58());
console.log('  Timing Window:  ', `[${CONFIG.windowRemainingMin}s, ${CONFIG.windowRemainingMax}s] continuous sniper`);
console.log('  Early Entries:  ', `before ${CONFIG.lateEntryTau}s remaining only if sigmaM < ${CONFIG.earlyEntryMaxSigma}`);
console.log('  Z-Score Filter: ', `Z >= ${CONFIG.minZScore}`);
console.log('  Edge Margin:    ', `edge >= +${(CONFIG.minEdgeMargin * 100).toFixed(1)}% net`);
console.log('  Max Entry Ask:  ', `<=${CONFIG.maxEntryAsk}$`);
console.log('  Stop Loss Cut:  ', `q_surv <= ${(CONFIG.stopLossMaxQ * 100).toFixed(0)}% or d <= -${CONFIG.stopLossMaxAdverseUsd}$ [${CONFIG.stopLossMinTau}s-${CONFIG.stopLossMaxTau}s]`);
console.log('  Capital Sizing: ', `${(CONFIG.sizingFraction * 100)}% of the balance, at most ${CONFIG.maxStakeUsd} USD per position`);
if (process.env.SIZING_FRACTION && parseFloat(process.env.SIZING_FRACTION) !== CONFIG.sizingFraction) {
  console.warn(`[CONFIG WARNING] SIZING_FRACTION=${process.env.SIZING_FRACTION} ignored, using ${CONFIG.sizingFraction}`);
}
if (process.env.MIN_BALANCE_FLOOR_USD) {
  console.warn(`[CONFIG WARNING] MIN_BALANCE_FLOOR_USD=${process.env.MIN_BALANCE_FLOOR_USD} replaces the trailing floor`);
}

// Math Helpers

// Normal CDF (Abramowitz & Stegun 7.1.26, max error 1.5e-7)
function normalCdf(x) {
  const a1 =  0.254829592;
  const a2 = -0.284496736;
  const a3 =  1.421413741;
  const a4 = -1.453152027;
  const a5 =  1.061405429;
  const p  =  0.3275911;

  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x) / Math.sqrt(2.0);
  const t = 1.0 / (1.0 + p * absX);
  const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
  return 0.5 * (1.0 + sign * y);
}

// Fee formula from Daily.fun matching engine: 700 bps * p * (1 - p)
function fee(p, feeRateBps = 700) {
  return (feeRateBps / 10000.0) * p * (1.0 - p);
}

// Calculate max price where net edge stays >= minEdgeFloor (default 0.025 = +2.5%)
function findMaxPriceForEdge(q, minEdge = 0.025, feeRateBps = 700) {
  const targetCost = q - minEdge;
  if (targetCost <= 0.05) return 0.05;
  if (!feeRateBps || feeRateBps <= 0) return Math.min(0.95, targetCost);
  const k = feeRateBps / 10000.0;
  const a = k;
  const b = -(1.0 + k);
  const c = targetCost;
  const disc = b * b - 4 * a * c;
  if (disc < 0) return Math.min(0.95, targetCost);
  const p = (-b - Math.sqrt(disc)) / (2 * a);
  return Math.min(0.95, Math.max(0.05, p));
}

// Automatic Privy Session Token Refresh
let isRefreshingPrivy = false;
async function refreshPrivySession() {
  if (isRefreshingPrivy) return false;
  if (!CONFIG.privyAppId || !PRIVY_TOKEN || !PRIVY_REFRESH_TOKEN) {
    return false;
  }
  isRefreshingPrivy = true;
  try {
    const res = await fetch('https://auth.privy.io/api/v1/sessions', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'privy-app-id': CONFIG.privyAppId,
        'origin': CONFIG.dailyOrigin,
        'authorization': `Bearer ${PRIVY_TOKEN}`
      },
      body: JSON.stringify({ refresh_token: PRIVY_REFRESH_TOKEN })
    });

    if (!res.ok) {
      console.error(new Date().toISOString(), '[PRIVY REFRESH HTTP ERROR]', res.status, await res.text());
      isRefreshingPrivy = false;
      return false;
    }

    const data = await res.json();
    if (data.token) {
      const rotated = data.token !== PRIVY_TOKEN;
      PRIVY_TOKEN = data.token;
      if (data.refresh_token) PRIVY_REFRESH_TOKEN = data.refresh_token;

      // Persist rotated tokens, creating auth.json when only .env exists: Privy rotates the refresh token, so one kept in memory only
      // is lost at the next restart (second instance, 2026-11-09 01:32: the .env refresh token was refused with 401 after two rotations)
      try {
        const a = fs.existsSync(AUTH_FILE) ? JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8')) : {};
        a.privyToken = PRIVY_TOKEN;
        if (data.refresh_token) a.privyRefreshToken = data.refresh_token;
        fs.writeFileSync(AUTH_FILE, JSON.stringify(a, null, 2), { mode: 0o600 });
      } catch (e) { console.error(new Date().toISOString(), `[PRIVY] could not save the rotated tokens: ${e.message}`); }

      authFailed = false;
      console.log(new Date().toISOString(), `[PRIVY AUTO-REFRESH SUCCESS] rotated=${rotated} expiresIn=${jwtExpiresIn(PRIVY_TOKEN)}s`);
      if (dailyWs && dailyWs.readyState === WebSocket.OPEN) {
        dailyWs.send(JSON.stringify({ type: 'auth', token: PRIVY_TOKEN }));
      }
      isRefreshingPrivy = false;
      return true;
    }
  } catch (e) {
    console.error(new Date().toISOString(), '[PRIVY AUTO-REFRESH EXCEPTION]', e.message);
  }
  isRefreshingPrivy = false;
  return false;
}

// Realized Volatility: 500 ms grid sampling over 600 s window (min 300 s required)
function calculateRealizedVol(history, windowSec = 600, stepMs = 500) {
  if (!history || history.length < 50) return null;
  const now = history.at(-1).t;
  const availableSpanMs = now - history[0].t;
  
  if (availableSpanMs < 300000) {
    return null; // Warmup: need at least 5 minutes of continuous price ticks
  }

  const startTime = Math.max(history[0].t, now - windowSec * 1000);

  const grid = [];
  let j = 0;
  for (let t = startTime; t <= now; t += stepMs) {
    while (j + 1 < history.length && history[j + 1].t <= t) {
      j++;
    }
    grid.push(history[j].p);
  }

  if (grid.length < 2) return null;

  let sumSqDiff = 0.0;
  for (let i = 1; i < grid.length; i++) {
    const dp = grid[i] - grid[i - 1];
    sumSqDiff += dp * dp;
  }

  const dtSec = (now - startTime) / 1000;
  return dtSec > 0 ? Math.sqrt(sumSqDiff / dtSec) : null;
}

// Exact Price Interpolation at targetMs (f0 at startMs)
function getPriceAt(history, targetMs, maxGapMs = 3000) {
  if (!history || history.length === 0) return null;
  if (targetMs < history[0].t || targetMs > history.at(-1).t) {
    return null;
  }

  let low = 0, high = history.length - 1;
  while (low <= high) {
    const mid = (low + high) >> 1;
    if (history[mid].t < targetMs) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  const i = low;
  if (i === 0) return history[0].p;
  const p0 = history[i - 1];
  const p1 = history[i];

  if (p1.t - p0.t > maxGapMs) {
    return null;
  }

  const ratio = (targetMs - p0.t) / (p1.t - p0.t);
  return p0.p + ratio * (p1.p - p0.p);
}

// Taker Intent v4 Serialization
function u64le(val) {
  const b = Buffer.alloc(8);
  b.writeBigUInt64LE(BigInt(val), 0);
  return b;
}

function buildIntentHash(intent, programId, market, usdcMint, takerOwner, takerVault, tradingSession) {
  const prefix = Buffer.from('daily:markets:taker-intent:v4');
  const sideByte = Buffer.from([intent.side === 'buy' ? 0 : 1]);
  const outcomeByte = Buffer.from([intent.outcomeIndex]);
  const tifByte = Buffer.from([intent.timeInForce === 'fak' ? 0 : 1]);
  
  const payload = Buffer.concat([
    prefix,
    programId.toBuffer(),
    market.toBuffer(),
    usdcMint.toBuffer(),
    takerOwner.toBuffer(),
    takerVault.toBuffer(),
    tradingSession.toBuffer(),
    sideByte,
    outcomeByte,
    tifByte,
    u64le(intent.limitPriceMicros),
    u64le(intent.maxShares),
    u64le(intent.maxNotional),
    u64le(intent.minFillShares),
    u64le(intent.salt),
    u64le(intent.expiry)
  ]);
  
  return crypto.createHash('sha256').update(payload).digest();
}

function buildCashActionHash(cashSession, actionId, maxNotional, expiry, nonce) {
  const prefix = Buffer.from("daily.cash.action.v1\0");
  const deploymentDomain = Buffer.from(cashSession.deploymentDomain, "hex");
  const zeroByte = Buffer.from([0]);
  const expBuf = Buffer.alloc(8);
  expBuf.writeBigInt64LE(BigInt(expiry), 0);
  
  const payload = Buffer.concat([
    prefix,
    new PublicKey(cashSession.programId).toBuffer(),
    deploymentDomain,
    new PublicKey(cashSession.vault).toBuffer(),
    new PublicKey(cashSession.session).toBuffer(),
    u64le(cashSession.sessionGeneration),
    new PublicKey(cashSession.adapterProgram).toBuffer(),
    zeroByte,
    actionId,
    actionId,
    new PublicKey(cashSession.destination).toBuffer(),
    u64le(maxNotional),
    expBuf,
    u64le(nonce)
  ]);
  return crypto.createHash("sha256").update(payload).digest();
}

// Bot State
let dailyWs = null;
let binanceWs = null;
let dailyPingTimer = null;
let currentBfPrice = null;
let lastBfTickMs = 0;
let lastDailyMsgMs = 0;
let isDailyAuthed = false;
const priceHistory = []; // { t, p }
// Ticks saved by the previous run: a restart then needs no 5-min warmup and keeps the open price of the round in progress.
// Only a recent save is used, so a long outage never shows up as a flat, falsely calm stretch.
const HISTORY_FILE = path.join(process.cwd(), 'price-history.json');
try {
  const saved = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
  const lastT = saved.at(-1)?.[0] || 0;
  if (Date.now() - lastT < 60000) {
    for (const [t, p] of saved) if (t > Date.now() - 720000 && p > 0) priceHistory.push({ t, p });
    console.log(new Date().toISOString(), `[HISTORY RESTORED] ${priceHistory.length} ticks, last ${((Date.now() - lastT) / 1000).toFixed(1)} s ago`);
  } else {
    console.log(new Date().toISOString(), `[HISTORY SKIPPED] last saved tick ${((Date.now() - lastT) / 1000).toFixed(0)} s ago, warming up`);
  }
} catch {}
const markets = new Map(); // slug -> market meta
const books = new Map(); // tokenId -> { bids, asks }
const tradedRounds = new Set();
const activePositions = new Map(); // slug -> { market, targetOutcome, targetSideName, sharesBought, f0, entryPrice, entryTau, cashoutAttempted }
let subscribedTokenIds = '';
let authFailed = false;
let currentUsdcBalance = null;
let initialDetectedBalance = null;
let peakBalance = null; // highest free balance seen, persisted; the trailing floor follows it
let lastFloorLog = 0;
let lastBalanceCheck = 0;

function notePeak() {
  const bal = currentUsdcBalance; // one pool: the peak is the wallet's
  if (bal > (peakBalance || 0)) {
    peakBalance = bal;
    saveBotState();
  }
}

// Order path: the trading session (vault, session, cash nonce) is kept warm in the background and
// markets are prewarmed once when they appear, so a trade sends without waiting on HTTP.
let sessionCache = null; // { session, at, nonceSpent }
let sessionGen = 0;      // bumped when a buy consumes the nonce; older fetches are then discarded
function spendNonce() { sessionGen++; if (sessionCache) sessionCache.nonceSpent = true; }
function invalidateSession() { spendNonce(); fetchSession(); }
// The session API can still show the nonce of a buy just processed (MATCHER_CASH_SESSION_STALE "signed nonce 157, indexed next
// nonce 158", 10 times on 2026-11-06, always one behind): for 10 s after a processed buy, a buy signs at least the nonce after it.
const signedNonces = new Map(); // requestId -> { key, nonce } of a buy in flight
let nonceFloor = { key: '', nonce: 0n, at: 0 };
function noteProcessedNonce(requestId) {
  const sn = signedNonces.get(requestId);
  signedNonces.delete(requestId);
  if (sn && (sn.key !== nonceFloor.key || sn.nonce + 1n > nonceFloor.nonce)) nonceFloor = { key: sn.key, nonce: sn.nonce + 1n, at: Date.now() };
}
function fetchSession() {
  const gen = sessionGen;
  return fetch(`https://daily.fun/chain-api/daily-markets/trading-session?authority=${AUTHORITY.toBase58()}`, {
    headers: { 'Authorization': `Bearer ${PRIVY_TOKEN}`, 'Origin': CONFIG.dailyOrigin },
    signal: AbortSignal.timeout(1500)
  }).then(r => (r.ok ? r.json() : null)).then(j => {
    const s = j?.tradingSession || null;
    if (s && gen === sessionGen) sessionCache = { session: s, at: Date.now(), nonceSpent: false };
    return s;
  }).catch(() => null);
}
// A buy needs the current cash nonce; a sell signs none, so any cached session (same vault and session) will do.
async function getSession(forSell = false) {
  if (sessionCache && (forSell || (!sessionCache.nonceSpent && Date.now() - sessionCache.at < 30000))) return sessionCache.session;
  return fetchSession();
}
const prewarmed = new Set();
function prewarmMarket(marketPda) {
  if (!marketPda || prewarmed.has(marketPda) || !PRIVY_TOKEN) return;
  prewarmed.add(marketPda);
  fetch('https://daily.fun/chain-api/daily-markets/trading-session/prewarm', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${PRIVY_TOKEN}`, 'Origin': CONFIG.dailyOrigin },
    body: JSON.stringify({ market: marketPda, authority: AUTHORITY.toBase58(), routeRevision: 1 }),
    signal: AbortSignal.timeout(3000)
  }).then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); }).catch((e) => {
    prewarmed.delete(marketPda);
    console.warn(new Date().toISOString(), `[PREWARM FAILED] ${marketPda} ${e.message}`);
  });
}

function jwtExpiresIn(token) {
  try { return Math.round(JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString()).exp - Date.now() / 1000); } catch { return null; }
}

function balanceFloor() {
  const configured = parseFloat(process.env.MIN_BALANCE_FLOOR_USD);
  if (!isNaN(configured)) return configured;
  return peakBalance ? Math.max(CONFIG.minBalanceFloorMinUsd, peakBalance * (1 - CONFIG.trailingFloorDrawdown)) : 10.0;
}
let vaultUsdcAta = CONFIG.dailyUsdcAta;

// Daily gain cap (owner 2026-11-07): once the wallet has gained the day's cap since the UTC day began, new entries of the bot
// and the leg stop until the next UTC day. platform-day.mjs writes the cap (half of what the platform made the day before) to
// day-cap.json; no file, no cap. ponytail: the gain is the free balance against its value at the day's first flat moment, so
// stakes still open count as spent (the cap can be passed by one round's stakes) and a deposit or withdrawal moves it.
const DAYCAP_FILE = path.join(process.cwd(), 'day-cap.json');
let dayStart = null; // { day, usdc, hit }, persisted in bot-state.json
let dayCap = { readAt: 0, cap: null, day: null };
function dayCapReached() {
  const now = Date.now(), day = new Date(now).toISOString().slice(0, 10);
  if (now - dayCap.readAt > 60000) {
    try { const j = JSON.parse(fs.readFileSync(DAYCAP_FILE, 'utf8')); dayCap = { readAt: now, cap: Number.isFinite(j.cap) ? j.cap : null, day: j.day }; }
    catch { dayCap = { readAt: now, cap: null, day: null }; }
  }
  if (dayStart?.day !== day && currentUsdcBalance !== null && !activePositions.size && !(lgb && (Object.keys(lgb.pos).length || Object.keys(lgb.orders).length))) {
    dayStart = { day, usdc: currentUsdcBalance, hit: false };
    saveBotState();
  }
  if (dayStart?.day !== day || dayCap.cap === null) return false;
  if (!dayStart.hit && currentUsdcBalance - dayStart.usdc >= dayCap.cap) {
    dayStart.hit = true;
    saveBotState();
    console.warn(new Date().toISOString(), `[DAY CAP] wallet +${(currentUsdcBalance - dayStart.usdc).toFixed(2)}$ since the day's start ${dayStart.usdc.toFixed(2)}$ >= cap ${dayCap.cap.toFixed(2)}$ (${dayCap.day}): new entries stop until 00:00 UTC`);
  }
  return dayStart.hit;
}
function dayCapNote() {
  const day = new Date().toISOString().slice(0, 10);
  return dayStart?.day === day ? ` day=${(currentUsdcBalance - dayStart.usdc).toFixed(2)}$/${dayCap.cap === null ? 'nocap' : dayCap.cap.toFixed(2) + '$'}${dayStart.hit ? '/CAPPED' : ''}` : '';
}

// Persistent State Management (D2 Fix)
const STATE_FILE = path.join(process.cwd(), 'bot-state.json');

function saveBotState() {
  try {
    const serializedPositions = [];
    for (const [slug, pos] of activePositions.entries()) {
      serializedPositions.push([
        slug,
        {
          targetOutcome: pos.targetOutcome,
          targetSideName: pos.targetSideName,
          sharesBought: pos.sharesBought,
          f0: pos.f0,
          entryPrice: pos.entryPrice,
          entryTau: pos.entryTau,
          cashoutAttempted: pos.cashoutAttempted,
          marketPda: pos.market?.marketPda || pos.marketPda,
          marketEndMs: pos.market?.endMs || pos.marketEndMs,
          marketFeeRateBps: pos.market?.feeRateBps || pos.marketFeeRateBps,
          // so a restart neither re-sends a sell in flight nor forgets which buy it waits for
          buyRequestId: pos.buyRequestId,
          buySentAt: pos.buySentAt,
          buyAccepted: pos.buyAccepted,
          buyProcessed: pos.buyProcessed,
          pendingSell: pos.pendingSell,
          lastCashoutAttemptMs: pos.lastCashoutAttemptMs,
          cashoutAttempts: pos.cashoutAttempts
        }
      ]);
    }
    const state = {
      tradedRounds: Array.from(tradedRounds),
      activePositions: serializedPositions,
      initialDetectedBalance,
      peakBalance,
      dayStart,
      savedAt: Date.now()
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (err) {
    console.error('Failed to save bot state:', err.message);
  }
}

function loadBotState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      if (Array.isArray(data.tradedRounds)) {
        for (const r of data.tradedRounds) tradedRounds.add(r);
      }
      if (Array.isArray(data.activePositions)) {
        for (const [slug, pos] of data.activePositions) {
          activePositions.set(slug, {
            ...pos,
            market: {
              marketPda: pos.marketPda,
              endMs: pos.marketEndMs,
              feeRateBps: pos.marketFeeRateBps
            }
          });
        }
      }
      if (data.initialDetectedBalance && !isNaN(data.initialDetectedBalance)) {
        initialDetectedBalance = data.initialDetectedBalance;
      }
      if (data.peakBalance > 0) peakBalance = data.peakBalance;
      if (data.dayStart?.day) dayStart = data.dayStart;
      if (process.env.RESET_PEAK === '1') { // after a withdrawal: the next balance read becomes the new peak
        console.warn(new Date().toISOString(), `[RESET_PEAK] peak ${peakBalance}$ discarded`);
        peakBalance = null;
      }
      console.log(new Date().toISOString(), `[STATE RESTORED] Loaded ${tradedRounds.size} traded rounds, ${activePositions.size} active positions, initialDetectedBalance=${initialDetectedBalance}$ peakBalance=${peakBalance}$`);
    }
  } catch (err) {
    console.error('Failed to load bot state:', err.message);
  }
}
loadBotState();

// ---- LightGBM leg (on with LGB_PORT, off otherwise) ----
// A second strategy in the same wallet. lgbm/trader.py decides and sends its orders here over UDP on 127.0.0.1; this file sizes
// them, sends them, answers with the venue's fill and settles the leg's shares from the venue's verdict. One pool: both strategies
// size on the whole wallet (5 % each), and one trailing floor on the wallet blocks new buys of both. The leg's ledger
// (lgb-state.json: cash, shares per market side, spend per round) only keeps which shares are the leg's and the leg's PnL.
const LGB_PORT = +process.env.LGB_PORT || 0;
const LGB = { replyPort: +process.env.LGB_REPLY_PORT || 0, pool0: +process.env.LGB_POOL0 || 100, capFrac: 0.05, quietMs: 15000, orderTimeoutMs: 15000, walletEveryMs: 10000,
  // owner's H2c (2026-11-09): pause leg buys 10 min once 27 of the last 30 venue answers to leg buys are refusals. On the leg's real
  // history (11-06..11-08: 95 % refused in the 11-08 blind window, 43 % elsewhere) it fires from 02:52:57 in that window and never
  // outside it; 18 of 20 also fired at 11-07 05:14, 05:34 and 11-08 02:41. LGB_PAUSE_WIN / LGB_PAUSE_REFUSED override it (the harness).
  pauseWin: +process.env.LGB_PAUSE_WIN || 30, pauseRefused: +process.env.LGB_PAUSE_REFUSED || 27, pauseMs: 600000 };
const lgbBuyAnswers = []; let lgbPausedUntil = 0; // true = filled, false = refused by the venue; in memory only
function lgbNoteBuy(filled) {
  lgbBuyAnswers.push(filled); if (lgbBuyAnswers.length > LGB.pauseWin) lgbBuyAnswers.shift();
  const refused = lgbBuyAnswers.filter((x) => !x).length;
  if (lgbBuyAnswers.length < LGB.pauseWin || refused < LGB.pauseRefused) return;
  lgbPausedUntil = Date.now() + LGB.pauseMs; lgbBuyAnswers.length = 0;
  console.warn(new Date().toISOString(), `[LGB PAUSE] ${refused} of the last ${LGB.pauseWin} leg buys refused: leg buys paused until ${new Date(lgbPausedUntil).toISOString()}`);
}
const LGB_FILE = path.join(process.cwd(), 'lgb-state.json');
let lgb = null; // { cash, halted, pos: { tokenId: { slug, marketId, marketPda, outcome, endMs, shares } }, spent, cap: { slug: usd }, orders: { requestId: order }, touched: { tokenId: ms }, changedAt }
let lgbSending = false;
let lgbWalletAt = 0;
const lgbFetching = new Set();
const lgbSock = LGB_PORT ? dgram.createSocket('udp4') : null;

function botBalance() { return currentUsdcBalance === null ? null : currentUsdcBalance - (lgb ? lgb.cash : 0); }
function saveLgb() { try { fs.writeFileSync(LGB_FILE, JSON.stringify(lgb, null, 1)); } catch (e) { console.error(new Date().toISOString(), '[LGB] save failed', e.message); } }
function lgbTouch(tokenId) { lgb.touched[tokenId] = lgb.changedAt = Date.now(); }
function lgbReply(o) { if (lgbSock && LGB.replyPort) lgbSock.send(JSON.stringify(o), LGB.replyPort, '127.0.0.1'); }
// A leg order on this market side is in flight or just landed: the venue's quantity may not show it yet.
function lgbBusy(tokenId, now = Date.now()) {
  return !!lgb && (Object.values(lgb.orders).some((o) => o.tokenId === tokenId) || now - (lgb.touched[tokenId] || 0) < LGB.quietMs);
}
// sign +1 books a venue fill in the leg's ledger, -1 takes it back (an error after the acceptance)
function lgbBook(o, f, sign) {
  const p = (lgb.pos[o.tokenId] ??= { slug: o.slug, marketId: o.marketId, marketPda: o.marketPda, outcome: o.outcome, endMs: o.endMs, shares: 0 });
  const dir = o.buy ? sign : -sign;
  p.shares = Math.max(0, p.shares + dir * f.sh); lgb.cash -= dir * f.usd;
  if (o.buy) lgb.spent[o.slug] = (lgb.spent[o.slug] || 0) + sign * f.usd;
  if (p.shares < 1e-6) delete lgb.pos[o.tokenId];
  lgbTouch(o.tokenId);
}

if (LGB_PORT) {
  try { lgb = JSON.parse(fs.readFileSync(LGB_FILE, 'utf8')); } catch {}
  if (!lgb) {
    lgb = { cash: LGB.pool0, halted: false, pos: {}, spent: {}, cap: {}, orders: {}, touched: {}, changedAt: 0, createdAt: new Date().toISOString() };
    console.log(new Date().toISOString(), `[LGB] new leg ledger, PnL counted from ${LGB.pool0} USD`);
    saveLgb();
  }
  lgbSock.on('message', (buf) => { let o; try { o = JSON.parse(buf); } catch { return; } lgbOrder(o); });
  lgbSock.on('error', (e) => console.error(new Date().toISOString(), '[LGB] socket', e.message));
  lgbSock.bind(LGB_PORT, '127.0.0.1');
  console.log(new Date().toISOString(), `[LGB] leg on udp ${LGB_PORT}, answers to ${LGB.replyPort}: cash ${lgb.cash.toFixed(2)} USD, ${Object.keys(lgb.pos).length} positions, at most ${LGB.capFrac * 100} % of the wallet and ${CONFIG.lgbMaxStakeUsd} USD per round${CONFIG.lgbMinSigma > 0 ? ` (${CONFIG.lgbCalmStakeUsd} while sigmaM < ${CONFIG.lgbMinSigma})` : ''}, buys stop at the wallet floor${lgb.halted ? ' (HALTED)' : ''}`);
}

async function lgbOrder(o) {
  const t0 = Date.now();
  const drop = (why) => {
    console.log(new Date().toISOString(), `[LGB DROP] ${o?.slug} ${o?.act} ${o?.side} ${why}`);
    if (typeof o?.k === 'string') lgbReply({ k: o.k, sh: 0, cost: 0, fe: 0, st: 'dropped', why });
  };
  if (!o || typeof o.k !== 'string' || !['entry', 'add', 'sell'].includes(o.act) || !['up', 'down'].includes(o.side) || !(o.limit > 0.01 && o.limit < 0.99)) return drop('bad order');
  if (CONFIG.paperMode || !isDailyAuthed || !dailyWs || dailyWs.readyState !== WebSocket.OPEN) return drop('venue not ready');
  if (lgbSending || Object.keys(lgb.orders).length) return drop('previous order pending');
  const m = markets.get(o.slug);
  if (!m?.marketPda || !m.marketId || !m.endMs || m.endMs - t0 < 10000) return drop('market unknown or closing');
  const outcome = o.side === 'up' ? 0 : 1, tokenId = `${m.marketPda}:${outcome}`, buy = o.act !== 'sell', feeBps = m.feeRateBps ?? 700;
  let shares;
  if (buy) {
    const wallet = currentUsdcBalance;
    if (!(wallet > balanceFloor())) return drop('floor');
    if (dayCapReached()) return drop('day cap');
    if (Date.now() < lgbPausedUntil) return drop(`paused after refusals until ${new Date(lgbPausedUntil).toISOString().slice(11, 19)}`);
    const sig = calculateRealizedVol(priceHistory, 600, 500); // the heartbeat's sigmaM
    if (CONFIG.lgbMinSigma > 0 && !Number.isFinite(sig)) return drop('sigmaM warmup');
    const roundMax = !(CONFIG.lgbMinSigma > 0) || sig >= CONFIG.lgbMinSigma ? CONFIG.lgbMaxStakeUsd : CONFIG.lgbCalmStakeUsd;
    const cap = (lgb.cap[o.slug] ??= Math.min(LGB.capFrac * wallet, roundMax)); // fixed at the round's first leg buy: the backtest's 5 % cap, at most 60 USD (30 while calm)
    const room = Math.min(cap - (lgb.spent[o.slug] || 0), wallet - balanceFloor() - 0.5);
    shares = Math.min(Math.floor(Number(o.target) || 0), Math.floor(room / (o.limit + fee(o.limit, feeBps))));
    if (!(shares >= 1) || shares * o.limit < 1) return drop(`room ${room.toFixed(2)} USD`); // the venue's 1 USD minimum
  } else {
    shares = Math.min(Number(o.shares) || 0, lgb.pos[tokenId]?.shares || 0);
    if (!(shares > 0)) return drop('no leg shares');
  }
  lgbSending = true;
  try {
    const session = await getSession(!buy);
    if (!session?.cashSession?.enabled) return drop('no trading session');
    if (dailyWs.readyState !== WebSocket.OPEN) return drop('venue not ready');
    const limitPriceMicros = BigInt(Math.round(o.limit * 1e6)), sharesMicros = BigInt(Math.round(shares * 1e6));
    const maxNotional = buy
      ? (sharesMicros * limitPriceMicros) / 1_000_000n + (sharesMicros * BigInt(feeBps) * limitPriceMicros * (1_000_000n - limitPriceMicros)) / (10_000n * 1_000_000n * 1_000_000n)
      : 0xffffffffffffffffn;
    const { frame, requestId } = buildOrderFrame(session, { marketPda: m.marketPda, side: buy ? 'buy' : 'sell', outcomeIndex: outcome, limitPriceMicros, sharesMicros, maxNotional, minFillShares: buy ? 1_000_000n : 1n });
    lgb.orders[requestId] = { k: o.k, act: o.act, slug: o.slug, tokenId, marketId: m.marketId, marketPda: m.marketPda, outcome, endMs: m.endMs, buy, shares, limit: o.limit, sentAt: Date.now() };
    lgbTouch(tokenId); saveLgb();
    dailyWs.send(JSON.stringify(frame));
    if (buy) spendNonce();
    console.log(new Date().toISOString(), `[LGB ${o.act.toUpperCase()}] ${o.slug} ${o.side} shares=${shares} limit=${o.limit} p=${o.p} lag=${Date.now() - t0}ms cash=${lgb.cash.toFixed(2)}$`);
    logTrade({ event: 'lgb_order_sent', requestId, ...lgb.orders[requestId], p: o.p, t_model: o.t });
  } finally {
    lgbSending = false;
  }
}

function lgbResult(msg) {
  const o = lgb.orders[msg.requestId];
  if (msg.type === 'daily_markets_trade_executed') {
    const e = msg.execution || {}, sh = Number(e.acceptedSize) / 1_000_000;
    if (!(sh > 0) || o.filled) return;
    const notional = Number(e.acceptedNotional || 0) / 1_000_000, fe = Number(e.acceptedFee || 0) / 1_000_000;
    o.filled = { sh, usd: o.buy ? notional + fe : notional - fe, fe }; // a buy costs notional + fee, a sale brings notional - fee
    lgbBook(o, o.filled, 1);
    lgbReply({ k: o.k, sh, cost: o.filled.usd, fe, st: 'filled' });
    if (o.buy) lgbNoteBuy(true);
    console.log(new Date().toISOString(), `[LGB FILLED] ${o.slug} ${o.act} ${sh} shares, ${o.buy ? 'cost' : 'proceeds'} ${o.filled.usd.toFixed(4)}$ cash=${lgb.cash.toFixed(2)}$`);
  } else {
    o.processed = true; o.processedAt = Date.now(); // without an accepted size, lgbTick closes it as empty 1 s later
  }
  if (o.filled && o.processed) delete lgb.orders[msg.requestId];
  saveLgb();
}

function lgbError(msg) {
  const o = lgb.orders[msg.requestId];
  delete lgb.orders[msg.requestId];
  if (o.filled) lgbBook(o, o.filled, -1);
  lgbTouch(o.tokenId); saveLgb();
  lgbReply({ k: o.k, sh: 0, cost: 0, fe: 0, st: 'rejected', code: msg.code });
  if (o.buy) lgbNoteBuy(false);
  console.warn(new Date().toISOString(), `[LGB REJECTED] ${o.slug} ${o.act} code=${msg.code}${o.filled ? ', accepted fill taken back' : ''} cash=${lgb.cash.toFixed(2)}$`);
}

// Order timeouts, settlement from the venue's verdict, pruning (every second)
function lgbTick() {
  const now = Date.now();
  // the trader sizes its orders on the whole wallet too (5 % cap per round)
  if (currentUsdcBalance > 0 && now - lgbWalletAt >= LGB.walletEveryMs) { lgbWalletAt = now; lgbReply({ wallet: currentUsdcBalance }); }
  for (const [rid, o] of Object.entries(lgb.orders)) {
    const empty = o.processed && !o.filled && now - o.processedAt > 1000;
    if (!empty && now - o.sentAt < LGB.orderTimeoutMs) continue;
    delete lgb.orders[rid]; lgbTouch(o.tokenId); saveLgb();
    if (!o.filled) lgbReply({ k: o.k, sh: 0, cost: 0, fe: 0, st: empty ? 'empty' : 'timeout' });
    if (empty) console.log(new Date().toISOString(), `[LGB EMPTY] ${o.slug} ${o.act} processed with nothing filled`);
    else console.warn(new Date().toISOString(), `[LGB TIMEOUT] ${o.slug} ${o.act} ${o.filled ? 'accepted, never processed' : 'no venue answer'} after ${LGB.orderTimeoutMs / 1000} s`);
  }
  for (const [tokenId, p] of Object.entries(lgb.pos)) {
    if (p.endMs + 2000 <= now && !(p.nextTry > now) && !lgbFetching.has(tokenId)) lgbSettle(tokenId, p);
  }
  const old = now / 1000 - 3600;
  for (const d of [lgb.spent, lgb.cap]) for (const s of Object.keys(d)) if (+s.split('-').pop() < old) delete d[s];
  for (const [t, ms] of Object.entries(lgb.touched)) if (now - ms > 3600000) delete lgb.touched[t];
}

async function lgbSettle(tokenId, p) {
  lgbFetching.add(tokenId);
  p.tries = (p.tries || 0) + 1; p.nextTry = Date.now() + (p.tries < 40 ? 3000 : 30000);
  try {
    const r = await fetch(`https://daily.fun/chain-api/daily-markets/${p.marketId}`, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(5000) });
    const mk = r.ok ? (await r.json())?.market : null;
    if (mk?.state !== 'resolved' || lgb.pos[tokenId] !== p) return;
    const pay = Number(mk.payoutVector?.[p.outcome]) / 1_000_000; // 1 USD per share to the winning outcome
    if (!(pay >= 0 && pay <= 1)) return;
    lgb.cash += p.shares * pay; delete lgb.pos[tokenId]; lgbTouch(tokenId); saveLgb();
    console.log(new Date().toISOString(), `[LGB SETTLED] ${p.slug} outcome ${p.outcome}: ${p.shares} shares x ${pay} = ${(p.shares * pay).toFixed(2)}$ cash=${lgb.cash.toFixed(2)}$`);
    logTrade({ event: 'lgb_settled', slug: p.slug, outcome: p.outcome, shares: p.shares, payout: pay, cash: lgb.cash });
  } catch {} finally {
    lgbFetching.delete(tokenId);
  }
}

async function resolveVaultUsdcAta() {
  if (vaultUsdcAta) return vaultUsdcAta;
  try {
    const res = await fetch(CONFIG.solanaRpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getTokenAccountsByOwner',
        params: [
          VAULT.toBase58(),
          { mint: CONFIG.usdcMint.toBase58() },
          { encoding: 'jsonParsed' }
        ]
      })
    }).then(r => r.json());
    const account = res?.result?.value?.[0];
    if (account?.pubkey) {
      vaultUsdcAta = account.pubkey;
      const initialAmt = account.account?.data?.parsed?.info?.tokenAmount?.uiAmount;
      if (initialAmt !== undefined && currentUsdcBalance === null) {
        currentUsdcBalance = initialAmt;
      }
      console.log(new Date().toISOString(), `[VAULT USDC ATA RESOLVED] ${vaultUsdcAta} | Solde: ${currentUsdcBalance !== null ? currentUsdcBalance.toFixed(2) : '0.00'}$`);
      return vaultUsdcAta;
    }
  } catch (e) {
    console.error(new Date().toISOString(), '[VAULT ATA RESOLUTION ERROR]', e.message);
  }
  return null;
}

let isFetchingBalance = false;
async function updateUsdcBalance() {
  const now = Date.now();
  if (now - lastBalanceCheck < 10000 && currentUsdcBalance !== null) return currentUsdcBalance;
  if (isFetchingBalance) return currentUsdcBalance;
  isFetchingBalance = true;
  try {
    if (!vaultUsdcAta) {
      await resolveVaultUsdcAta();
    }
    if (!vaultUsdcAta) return currentUsdcBalance;

    const res = await fetch(CONFIG.solanaRpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getTokenAccountBalance',
        params: [vaultUsdcAta]
      })
    }).then(r => r.json());
    if (res?.result?.value?.uiAmount !== undefined) {
      currentUsdcBalance = res.result.value.uiAmount;
      notePeak();
      if (initialDetectedBalance === null && currentUsdcBalance > 0) {
        initialDetectedBalance = currentUsdcBalance;
        saveBotState();
        console.log(new Date().toISOString(), `[CAPITAL ACTIF DÉTECTÉ] Solde initial: ${initialDetectedBalance.toFixed(2)}$ | Sizing proportionnel: ${(CONFIG.sizingFraction * 100)}% par trade`);
      } else if (initialDetectedBalance !== null && currentUsdcBalance > initialDetectedBalance * 1.15) {
        console.log(new Date().toISOString(), `[DÉPÔT DÉTECTÉ] Solde rehaussé: ${initialDetectedBalance.toFixed(2)}$ -> ${currentUsdcBalance.toFixed(2)}$ | Floor stop-loss ajusté`);
        initialDetectedBalance = currentUsdcBalance;
        saveBotState();
      }
      lastBalanceCheck = now;
    }
  } catch (e) {
  } finally {
    isFetchingBalance = false;
  }
  return currentUsdcBalance;
}

// Binance Futures WebSocket
let binanceConnectTimeMs = 0;
function initBinance() {
  binanceConnectTimeMs = Date.now();
  binanceWs = new WebSocket(CONFIG.binanceFuturesWs);
  binanceWs.on('open', () => console.log(new Date().toISOString(), 'Binance Futures WS connected'));
  binanceWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      const b = parseFloat(msg.b), a = parseFloat(msg.a);
      if (b && a) {
        currentBfPrice = (b + a) / 2;
        lastBfTickMs = Date.now();
        // Throttle tick history to 1 tick per 50ms
        if (priceHistory.length === 0 || lastBfTickMs - priceHistory[priceHistory.length - 1].t >= 50) {
          priceHistory.push({ t: lastBfTickMs, p: currentBfPrice });
        }
        // Prune older than 12m every 10s
        if (priceHistory.length > 0 && lastBfTickMs - (globalThis.__lastPrune || 0) > 10000) {
          globalThis.__lastPrune = lastBfTickMs;
          const cutoff = lastBfTickMs - 720000;
          let pruneIdx = 0;
          while (pruneIdx < priceHistory.length && priceHistory[pruneIdx].t < cutoff) pruneIdx++;
          if (pruneIdx > 0) priceHistory.splice(0, pruneIdx);
          try { // atomic: a crash mid-write never leaves a truncated file
            fs.writeFileSync(HISTORY_FILE + '.tmp', JSON.stringify(priceHistory.map((x) => [x.t, x.p])));
            fs.renameSync(HISTORY_FILE + '.tmp', HISTORY_FILE);
          } catch (e) { console.warn('Failed to save price history:', e.message); }
        }
      }
    } catch (e) {}
  });
  binanceWs.on('close', () => {
    console.log(new Date().toISOString(), 'Binance WS closed, reconnecting in 2s');
    setTimeout(initBinance, 2000);
  });
  binanceWs.on('error', () => {});
}

// Logging Trade Microstructure
function logTrade(record) {
  const line = JSON.stringify({ t: Date.now(), iso: new Date().toISOString(), ...record }) + '\n';
  fs.appendFileSync(CONFIG.tradesLog, line);
  console.log(new Date().toISOString(), 'TRADE LOGGED:', JSON.stringify(record));
}

// Daily.fun WebSocket
function initDaily() {
  lastDailyMsgMs = Date.now();
  if (dailyPingTimer) {
    clearInterval(dailyPingTimer);
    dailyPingTimer = null;
  }

  dailyWs = new WebSocket(CONFIG.dailyWs, {
    headers: {
      Origin: CONFIG.dailyOrigin,
      'User-Agent': CONFIG.dailyUserAgent
    }
  });

  dailyWs.on('open', () => {
    console.log(new Date().toISOString(), 'Daily.fun WS connected');
    lastDailyMsgMs = Date.now();
    if (PRIVY_TOKEN && !CONFIG.paperMode && !authFailed) {
      dailyWs.send(JSON.stringify({ type: 'auth', token: PRIVY_TOKEN }));
    }
    dailyWs.send(JSON.stringify({ type: 'updown_subscribe' }));

    dailyPingTimer = setInterval(() => {
      if (dailyWs && dailyWs.readyState === WebSocket.OPEN) {
        dailyWs.send(JSON.stringify({ type: 'ping' }));
      }
    }, 15000);
  });

  dailyWs.on('message', (data) => {
    lastDailyMsgMs = Date.now();
    try {
      const msg = JSON.parse(data);
      handleDailyMessage(msg);
    } catch (e) {}
  });

  dailyWs.on('close', () => {
    console.log(new Date().toISOString(), 'Daily WS closed, reconnecting in 2s');
    isDailyAuthed = false;
    subscribedTokenIds = '';
    books.clear();
    if (dailyPingTimer) {
      clearInterval(dailyPingTimer);
      dailyPingTimer = null;
    }
    setTimeout(initDaily, 2000);
  });

  dailyWs.on('error', (err) => {
    console.error(new Date().toISOString(), 'Daily WS error:', err.message);
  });
}

function updateSubscriptions() {
  const tokenIds = [];
  const now = Date.now();
  for (const m of markets.values()) {
    if (m.symbol === 'BTC' && m.endMs > now) {
      tokenIds.push(`${m.marketPda}:0`, `${m.marketPda}:1`);
    }
  }
  const serialized = tokenIds.sort().join(',');
  if (serialized !== subscribedTokenIds && tokenIds.length > 0) {
    subscribedTokenIds = serialized;
    if (dailyWs && dailyWs.readyState === WebSocket.OPEN) {
      dailyWs.send(JSON.stringify({
        type: 'updown_book_subscribe',
        tokenIds
      }));
    }
  }
}

function findPositionByRequest(requestId) {
  for (const [slug, pos] of activePositions.entries()) {
    if (pos.buyRequestId === requestId || pos.pendingSell?.requestId === requestId || pos.pendingAdd?.requestId === requestId) return [slug, pos];
  }
  return [null, null];
}

// 'executed' means accepted (state prepared, filledSize 0); only 'processed' confirms on-chain.
function trackOrderResult(msg) {
  if (msg.type === 'daily_markets_trade_processed') { noteProcessedNonce(msg.requestId); invalidateSession(); } // a processed buy has consumed the cash nonce
  if (lgb?.orders[msg.requestId]) return lgbResult(msg);
  const [slug, pos] = findPositionByRequest(msg.requestId);
  if (!pos) return;
  const isSell = pos.pendingSell?.requestId === msg.requestId, isAdd = pos.pendingAdd?.requestId === msg.requestId;
  if (msg.type === 'daily_markets_trade_executed') {
    const accepted = Number(msg.execution?.acceptedSize) / 1_000_000;
    if (!(accepted > 0)) return;
    pos.lastFillAt = Date.now();
    if (isSell) pos.pendingSell.acceptedShares = accepted;
    else if (isAdd) {
      if (pos.pendingAdd.filled) return;
      pos.pendingAdd.filled = accepted; pos.sharesBought += accepted; pos.adds = (pos.adds || 0) + 1;
      console.log(new Date().toISOString(), `[ADD FILLED] ${slug} +${accepted} shares, position ${pos.sharesBought}`);
    }
    else { pos.sharesBought = accepted; pos.buyAccepted = true; }
  } else if (isAdd) {
    pos.pendingAdd = null; // processed: filled above, or nothing filled (retried after the gap)
  } else if (!isSell) {
    pos.buyProcessed = true;
  } else {
    const sold = pos.pendingSell.acceptedShares ?? pos.pendingSell.shares;
    pos.pendingSell = null;
    pos.sharesBought = Math.max(0, pos.sharesBought - sold);
    if (pos.sharesBought < 0.000001) {
      console.log(new Date().toISOString(), `[CASHOUT FILLED] ${slug} sold=${sold} position closed`);
      activePositions.delete(slug);
    } else {
      console.log(new Date().toISOString(), `[CASHOUT PARTIAL] ${slug} sold=${sold} remaining=${pos.sharesBought}`);
    }
  }
  saveBotState();
}

// The venue's own position list is the truth on size. It settles a sell whose confirmation was lost, a restart
// in the middle of a sell, and a partial fill, without trusting our bookkeeping of request results.
function syncVenuePositions(list) {
  let changed = false;
  const botSides = new Set();
  for (const [slug, pos] of activePositions.entries()) {
    const tokenId = `${pos.market?.marketPda}:${pos.targetOutcome}`;
    botSides.add(tokenId);
    const v = list.find((x) => x.market?.marketPda === pos.market?.marketPda && x.outcomeIndex === pos.targetOutcome);
    if (!v) continue;
    // a list written before our last fill on this side is stale: 6 Nov 10:02:36, our buy was processed at .320 and the list at .581
    // still showed only the leg's 10:00:09 fill, so the bot dropped its own 28 shares
    if (pos.lastFillAt && Date.parse(v.lastTradeAt) < pos.lastFillAt - 2000) continue;
    if (lgbBusy(tokenId)) continue; // a leg order on this side may not show in the venue's quantity yet
    const qty = Number(v.quantity) / 1_000_000 - (lgb?.pos[tokenId]?.shares || 0); // the venue's quantity holds both strategies' shares
    if (!Number.isFinite(qty)) continue;
    if (!pos.buyProcessed && !(pos.buySentAt && Date.now() - pos.buySentAt > 15000) && pos.buySentAt) continue; // buy still in flight
    if (pos.pendingAdd) continue; // an add in flight may not show in the venue's quantity yet
    if (pos.pendingSell) {
      if (qty >= (pos.pendingSell.qtyAtSend ?? pos.sharesBought) - 1e-6) continue; // the sell has not landed yet
      pos.pendingSell = null; // it landed: the venue quantity below replaces our bookkeeping
    }
    if (qty < 1e-6) {
      console.log(new Date().toISOString(), `[VENUE SYNC] ${slug} venue holds 0 shares, position closed`);
      activePositions.delete(slug); changed = true;
    } else if (Math.abs(qty - pos.sharesBought) > 1e-6) {
      console.log(new Date().toISOString(), `[VENUE SYNC] ${slug} shares ${pos.sharesBought} -> ${qty}`);
      pos.sharesBought = qty; changed = true;
    }
  }
  if (changed) saveBotState();
  // A side only the leg holds: the venue lowers the leg's size (a sale whose answer was lost), never raises it: more shares than the
  // leg's own fills are someone else's (6 Nov 10:02:36, the bot's 28 shares were booked to the leg and paid to it at settlement)
  for (const [tokenId, p] of Object.entries(lgb?.pos || {})) {
    if (botSides.has(tokenId) || p.endMs - Date.now() < 15000 || lgbBusy(tokenId)) continue;
    const v = list.find((x) => x.market?.marketPda === p.marketPda && x.outcomeIndex === p.outcome);
    const qty = v ? Number(v.quantity) / 1_000_000 : NaN;
    if (!Number.isFinite(qty) || qty > p.shares - 1e-6) continue;
    console.warn(new Date().toISOString(), `[LGB SYNC] ${p.slug} leg shares ${p.shares} -> ${qty}`);
    if (qty < 1e-6) delete lgb.pos[tokenId]; else p.shares = qty;
    lgbTouch(tokenId); saveLgb();
  }
}

function trackOrderError(msg) {
  signedNonces.delete(msg.requestId);
  invalidateSession();
  if (lgb?.orders[msg.requestId]) return lgbError(msg);
  const [slug, pos] = findPositionByRequest(msg.requestId);
  if (!pos) return;
  if (pos.pendingSell?.requestId === msg.requestId) {
    console.warn(new Date().toISOString(), `[CASHOUT REJECTED] ${slug} code=${msg.code}, will retry`);
    pos.pendingSell = null;
    pos.lastCashoutAttemptMs = Date.now() - 1200; // nothing is pending after a rejection: retry 0.3 s later, not 1.5 s
  } else if (pos.pendingAdd?.requestId === msg.requestId) { // a refused add leaves the entry's position as it was
    if (pos.pendingAdd.filled) { pos.sharesBought -= pos.pendingAdd.filled; pos.adds -= 1; }
    console.warn(new Date().toISOString(), `[ADD REJECTED] ${slug} code=${msg.code}, position ${pos.sharesBought} shares kept`);
    pos.pendingAdd = null;
  } else {
    console.warn(new Date().toISOString(), `[BUY REJECTED] ${slug} code=${msg.code}, no position opened`);
    activePositions.delete(slug);
  }
  saveBotState();
}

function handleDailyMessage(msg) {
  if (msg.type === 'updown_markets') {
    for (const m of msg.markets || []) {
      let startSec = null, endSec = null;
      if (m.roundId && m.roundId.includes(':')) {
        const parts = m.roundId.split(':');
        if (parts.length >= 4) {
          startSec = parseInt(parts[2]);
          endSec = parseInt(parts[3]);
        }
      }
      if (!startSec && m.startTime) startSec = m.startTime;
      if (!endSec && m.endTime) endSec = m.endTime;
      if (!startSec && endSec) startSec = endSec - 300;

      const startMs = startSec ? startSec * 1000 : null;
      const endMs = endSec ? endSec * 1000 : (m.secondsToEnd ? Date.now() + m.secondsToEnd * 1000 : null);

      markets.set(m.slug, {
        ...m,
        startMs,
        endMs
      });
      if (m.symbol === 'BTC') prewarmMarket(m.marketPda);

      for (const o of m.outcomes || []) {
        if (o.book && o.tokenId) {
          books.set(o.tokenId, {
            bids: o.book.bids || [],
            asks: o.book.asks || [],
            receivedAtMs: Date.now(),
            ageMs: o.book.ageMs,
            stale: o.book.stale
          });
        }
      }
    }
    updateSubscriptions();
  } else if (msg.type === 'updown_book') {
    books.set(msg.tokenId, {
      bids: msg.bids || [],
      asks: msg.asks || [],
      receivedAtMs: Date.now(),
      ageMs: msg.ageMs,
      stale: msg.stale
    });
  } else if (msg.type === 'daily_markets_trade_processed' || msg.type === 'daily_markets_trade_executed') {
    console.log(new Date().toISOString(), '[TRADE SUCCESS] Daily trade executed:', JSON.stringify(msg));
    logTrade({ event: 'trade_executed', details: msg });
    trackOrderResult(msg);
    updateUsdcBalance();
  } else if (msg.type === 'authed') {
    isDailyAuthed = true;
    console.log(new Date().toISOString(), 'DAILY AUTH SUCCESS:', JSON.stringify(msg));
  } else if (msg.type === 'cash_account_changed.v1') {
    logTrade({ event: 'daily_ws_event', type: msg.type, details: msg });
    const atomic = parseFloat(msg.availableAtomic || msg.totalAtomic || '0');
    if (atomic > 0) {
      const bal = atomic / 1_000_000;
      currentUsdcBalance = bal;
      notePeak();
      if (initialDetectedBalance === null && currentUsdcBalance > 0) {
        initialDetectedBalance = currentUsdcBalance;
        saveBotState();
        console.log(new Date().toISOString(), `[CAPITAL ACTIF DÉTECTÉ] Solde initial: ${initialDetectedBalance.toFixed(2)}$ | Sizing proportionnel: ${(CONFIG.sizingFraction * 100)}% par trade`);
      } else if (initialDetectedBalance !== null && currentUsdcBalance > initialDetectedBalance * 1.15) {
        console.log(new Date().toISOString(), `[DÉPÔT DÉTECTÉ] Solde rehaussé: ${initialDetectedBalance.toFixed(2)}$ -> ${currentUsdcBalance.toFixed(2)}$ | Floor stop-loss ajusté`);
        initialDetectedBalance = currentUsdcBalance;
        saveBotState();
      }
      lastBalanceCheck = Date.now();
      console.log(new Date().toISOString(), `[CASH EVENT] Daily USDC balance: ${currentUsdcBalance.toFixed(2)}$ (vault: ${msg.vault || 'n/a'})`);
    }
  } else if (msg.type === 'error' || msg.type === 'daily_markets_error') {
    const bookSnapshots = {};
    for (const [tokenId, b] of books.entries()) {
      bookSnapshots[tokenId] = {
        bestBid: b.bids?.[0],
        bestAsk: b.asks?.[0]
      };
    }
    console.error(new Date().toISOString(), 'DAILY ERROR MSG:', JSON.stringify(msg), 'BOOK SNAPSHOT:', JSON.stringify(bookSnapshots));
    logTrade({ event: 'daily_error', code: msg.code, message: msg.message, details: msg, bookSnapshot: bookSnapshots });
    if (msg.requestId) trackOrderError(msg);
    if (msg.message === 'bad token') {
      authFailed = true;
      isDailyAuthed = false;
      console.warn(new Date().toISOString(), '[BAD TOKEN] Triggering automatic Privy session refresh...');
      refreshPrivySession();
    } else if (msg.code === 'subscription_limit') {
      console.warn(new Date().toISOString(), '[SUBSCRIPTION LIMIT] Cycling Daily WS connection...');
      if (dailyWs) dailyWs.close();
    }
  } else if (msg.type === 'updown_update') {
    logTrade({ event: 'daily_ws_event', type: msg.type, details: msg });
    syncVenuePositions(msg.nativePositions || []);
  } else if (msg.type && !['updown_markets', 'updown_book', 'updown_pyth', 'updown_spot', 'updown_pyth_history', 'updown_spot_history', 'updown_public_trades', 'updown_spot_snapshot', 'updown_subscribed', 'updown_book_subscribed', 'ping', 'pong'].includes(msg.type)) {
    logTrade({ event: 'daily_ws_event', type: msg.type, details: msg });
  }
}

// S6 Strategy Evaluator (Called every 200 ms)
let lastHeartbeat = 0;
let lastExpiredWarning = 0;
function evaluateStrategy() {
  const now = Date.now();

  // Watchdogs
  const binanceSilence = now - Math.max(lastBfTickMs, binanceConnectTimeMs);
  if (binanceSilence > 15000 && binanceWs) {
    console.warn(new Date().toISOString(), '[WATCHDOG] Binance WS silent >15s, reconnecting...');
    binanceConnectTimeMs = now;
    binanceWs.terminate();
  }

  if (lastDailyMsgMs > 0 && now - lastDailyMsgMs > 30000 && dailyWs) {
    console.warn(new Date().toISOString(), '[WATCHDOG] Daily WS silent >30s, reconnecting...');
    lastDailyMsgMs = now;
    dailyWs.terminate();
  }

  // Prune markets older than 10m
  for (const [slug, m] of markets.entries()) {
    if (m.endMs && now - m.endMs > 600000) {
      books.delete(`${m.marketPda}:0`);
      books.delete(`${m.marketPda}:1`);
      markets.delete(slug);
      tradedRounds.delete(slug);
      if (activePositions.has(slug)) {
        activePositions.delete(slug);
        saveBotState();
      }
    }
  }

  if (!currentBfPrice) return;
  if (now - lastBfTickMs > 3000) return;

  const sigmaM = calculateRealizedVol(priceHistory, 600, 500);
  const sigmaEff = Math.max(sigmaM || 2.20, CONFIG.minSigmaFloor || 2.20);

  // =========================================================================
  // 1. D1 FIX: STOP-LOSS / CASH OUT FIRST FOR ALL OPEN POSITIONS
  // Always executed regardless of volatility or account balance floor!
  // =========================================================================
  for (const [slug, pos] of activePositions.entries()) {
    const m = pos.market || markets.get(slug);
    const endMs = m?.endMs || pos.marketEndMs;
    if (!endMs) continue;
    const remainingSeconds = (endMs - now) / 1000;

    if (remainingSeconds < 10) {
      activePositions.delete(slug);
      saveBotState();
      continue;
    }

    // A buy the venue never answered (no result, or an error without a requestId) leaves no shares: drop it.
    if (pos.buySentAt && !pos.buyAccepted && !pos.buyProcessed && now - pos.buySentAt > 15000) {
      console.warn(new Date().toISOString(), `[BUY UNCONFIRMED] ${slug} no venue result after 15 s, position dropped`);
      activePositions.delete(slug);
      saveBotState();
      continue;
    }
    if (pos.buySentAt && !pos.buyAccepted && !pos.buyProcessed) {
      continue; // nothing to sell before the venue confirms the buy
    }
    if (pos.pendingAdd && now - pos.pendingAdd.sentAt < 6000) continue; // an add in flight (expiry 4 s): sell once its shares are known
    if (pos.pendingAdd) { pos.pendingAdd = null; saveBotState(); } // never answered: the venue sync settles the size

    if (remainingSeconds >= CONFIG.stopLossMinTau && remainingSeconds <= CONFIG.stopLossMaxTau) {
      const relDelta = pos.targetOutcome === 0 ? (currentBfPrice - pos.f0) : -(currentBfPrice - pos.f0);
      const qSurv = normalCdf(relDelta / (CONFIG.cModel * sigmaEff * Math.sqrt(Math.max(remainingSeconds, 1))));
      if (qSurv <= CONFIG.stopLossMaxQ || relDelta <= -CONFIG.stopLossMaxAdverseUsd) {
        const lastAttempt = pos.lastCashoutAttemptMs || 0;
        const sellPending = pos.pendingSell && now - pos.pendingSell.sentAt < 6000; // Order expiry is 4 s
        if (!pos.sellInFlight && !sellPending && now - lastAttempt >= 1500) { // Retry every 1.5s until filled
          pos.sellInFlight = true;
          pos.lastCashoutAttemptMs = now;
          pos.cashoutAttempts = (pos.cashoutAttempts || 0) + 1;
          saveBotState();
          console.log(new Date().toISOString(), `[STOP LOSS / CASH OUT TRIGGERED] ${slug} attempt #${pos.cashoutAttempts} qSurv=${qSurv.toFixed(3)} relDelta=${relDelta.toFixed(1)}$ -> executing Cash Out`);
          executeCashOut({
            slug,
            market: m,
            outcomeIndex: pos.targetOutcome,
            sideName: pos.targetSideName,
            sharesToSell: pos.sharesBought,
            relDelta,
            qSurv,
            pos
          }).finally(() => { pos.sellInFlight = false; });
        }
      }
    }
  }

  // =========================================================================
  // 2. GATES FOR NEW ENTRIES ONLY
  // =========================================================================
  if (sessionExpirySec && now / 1000 >= sessionExpirySec) {
    if (now - lastExpiredWarning > 30000) {
      lastExpiredWarning = now;
      console.error(new Date().toISOString(), '[SESSION EXPIRED] On-chain session is expired. Halting trading.');
    }
    return;
  }

  updateUsdcBalance();

  // Trailing floor (blocks NEW entries only), on the whole wallet: one pool for the bot and the LightGBM leg
  const dynamicFloor = balanceFloor();
  const botBal = botBalance(); // the bot's share, for the heartbeat only
  const wallet = currentUsdcBalance;
  if (wallet !== null && wallet <= dynamicFloor && now - lastFloorLog > 60000) {
    lastFloorLog = now;
    console.warn(new Date().toISOString(), `[FLOOR] wallet ${wallet.toFixed(2)}$ <= floor ${dynamicFloor.toFixed(2)}$ (peak ${peakBalance?.toFixed(2)}$): new entries paused; after a withdrawal restart with RESET_PEAK=1`);
  }
  if (wallet === null || (now - lastBalanceCheck > 60000) || wallet <= dynamicFloor) {
    return;
  }

  const dayCapped = dayCapReached(); // before the heartbeat, which shows it
  if (now - lastHeartbeat > 10000) {
    lastHeartbeat = now;
    const activeBtc = Array.from(markets.values()).filter(m => m.symbol === 'BTC' && m.endMs > now);
    const nextRounds = activeBtc.map(m => `${m.slug} (${((m.endMs - now)/1000).toFixed(0)}s left)`).join(', ');
    const leg = lgb ? ` bot=${botBal.toFixed(2)}$ lgb=${lgb.cash.toFixed(2)}$/${Object.keys(lgb.pos).length}pos${lgb.halted ? '/HALTED' : ''}` : '';
    console.log(new Date().toISOString(), `[HEARTBEAT] bf=${currentBfPrice?.toFixed(1)} ticks=${priceHistory.length} sigmaM=${sigmaM ? sigmaM.toFixed(3) : 'warmup'} usdc=${currentUsdcBalance.toFixed(2)}$ floor=${dynamicFloor.toFixed(2)}$${leg}${dayCapNote()} authed=${isDailyAuthed} | ${nextRounds}`);
  }

  if (dayCapped) return; // daily gain cap reached: no new entry until 00:00 UTC, open positions are managed above

  if (!sigmaM || sigmaM >= CONFIG.calmVolThreshold) {
    return; // Volatility too high or warmup in progress for new entries
  }

  for (const [slug, m] of markets.entries()) {
    if (m.symbol !== 'BTC') continue;
    if (!m.startMs || !m.endMs) continue;

    const remainingSeconds = (m.endMs - now) / 1000;
    // D1: a round already traded is looked at again only for an add to its open, filled, never-sold position
    const held = tradedRounds.has(slug) ? activePositions.get(slug) : null;
    if (tradedRounds.has(slug) && !(held && (held.adds || 0) < CONFIG.addMax && held.buyAccepted && held.sharesBought > 0 && Number.isFinite(held.entryEdge)
      && !held.cashoutAttempts && !held.pendingSell && !held.sellInFlight && !held.pendingAdd && now - (held.lastBuyAt || held.buySentAt) >= CONFIG.addGapMs)) continue;

    // 2. Calibrated Sniping Window: 50s to 130s remaining (safe zone around 120s)
    if (remainingSeconds < CONFIG.windowRemainingMin || remainingSeconds > CONFIG.windowRemainingMax) {
      continue;
    }
    if (sigmaM >= CONFIG.earlyEntryMaxSigma && remainingSeconds > CONFIG.lateEntryTau) continue; // agitated market: early entries lost (5 Nov VPS day)

    const f0 = getPriceAt(priceHistory, m.startMs);
    if (!f0) continue;

    const d = currentBfPrice - f0;
    const tau = remainingSeconds;
    const sigmaEff = Math.max(sigmaM, CONFIG.minSigmaFloor || 2.20);
    const z = Math.abs(d) / (CONFIG.cModel * sigmaEff * Math.sqrt(Math.max(tau, 1)));

    // Statistical conviction filter: Z >= 0.70
    if (z < CONFIG.minZScore) {
      continue;
    }

    const targetOutcome = d > 0 ? 0 : 1;
    const targetSideName = d > 0 ? 'up' : 'down';
    const tokenId = `${m.marketPda}:${targetOutcome}`;
    const oppTokenId = `${m.marketPda}:${1 - targetOutcome}`;

    const book = books.get(tokenId);
    const oppBook = books.get(oppTokenId);
    
    if (!book || !book.asks || book.asks.length === 0 || !oppBook || !oppBook.asks || oppBook.asks.length === 0) {
      continue;
    }

    if (book.stale || oppBook.stale) continue;
    const bookAge = book.receivedAtMs ? (now - book.receivedAtMs) : 99999;
    const oppBookAge = oppBook.receivedAtMs ? (now - oppBook.receivedAtMs) : 99999;
    if (bookAge > 3000 || oppBookAge > 3000) continue;

    const level1Ask = book.asks[0].price;
    const level1Sz = book.asks[0].sizeShares;
    if (!level1Sz || isNaN(level1Sz) || level1Sz < 1) continue;

    // Filter expensive asks: only buy underpriced contracts
    if (level1Ask > CONFIG.maxEntryAsk) {
      continue;
    }

    const q = normalCdf(z);
    const cost = level1Ask + fee(level1Ask, m.feeRateBps ?? 700);
    const edge = q - cost;

    if (edge >= CONFIG.minEdgeMargin && held) {
      if (targetSideName !== held.targetSideName || edge < held.entryEdge + CONFIG.addEdgeDelta) continue;
      held.lastBuyAt = now; // one attempt per gap, filled or refused
      saveBotState();
      console.log(new Date().toISOString(), `[ADD TRIGGERED] ${slug} rem=${remainingSeconds.toFixed(1)}s side=${targetSideName} q=${q.toFixed(3)} ask=${level1Ask} edge=+${(edge * 100).toFixed(1)}% (entry +${(held.entryEdge * 100).toFixed(1)}%)`);
      executeTrade({ slug, market: m, remainingSeconds, tau, f0, f_now: currentBfPrice, d, sigmaM, z, q, targetOutcome, targetSideName, book, oppBook, level1Ask, level1Sz, edge, add: held });
      break;
    }
    if (edge >= CONFIG.minEdgeMargin) {
      tradedRounds.add(slug);
      saveBotState();
      console.log(new Date().toISOString(), `[SNIPE TRIGGERED] ${slug} rem=${remainingSeconds.toFixed(1)}s side=${targetSideName} d=${d.toFixed(1)} Z=${z.toFixed(2)} q=${q.toFixed(3)} ask=${level1Ask} cost=${cost.toFixed(3)} edge=+${(edge * 100).toFixed(1)}%`);
      executeTrade({
        slug,
        market: m,
        remainingSeconds,
        tau,
        f0,
        f_now: currentBfPrice,
        d,
        sigmaM,
        z,
        q,
        targetOutcome,
        targetSideName,
        book,
        oppBook,
        level1Ask,
        level1Sz,
        edge
      });
      break;
    }
  }
}

async function executeTrade(params) {
  const t_eval_ms = Date.now();

  if (!CONFIG.paperMode && !isDailyAuthed) {
    console.error(new Date().toISOString(), 'SKIPPING TRADE: Daily WS not authenticated yet');
    return;
  }

  if (currentUsdcBalance === null) {
    console.warn(new Date().toISOString(), 'SKIPPING TRADE: USDC balance not yet verified');
    return;
  }
  const balance = currentUsdcBalance; // one pool: the whole wallet
  const dynamicFloor = balanceFloor();
  const maxSafeSpend = Math.max(0, balance - dynamicFloor - 0.5);
  let baseTargetSpend = Math.min(balance * CONFIG.sizingFraction, CONFIG.maxStakeUsd, maxSafeSpend);

  // Fixed 1.0x sizing: no leverage on edge
  const edgeMultiplier = 1.0;
  let targetSpend = Math.min(baseTargetSpend * edgeMultiplier, maxSafeSpend);

  // Determine base ask price and available shares (Level 1 + Level 2 depth)
  let baseAskPrice = params.level1Ask;
  let maxFillableShares = params.level1Sz;

  const bookAsks = params.book?.asks || [];
  if (bookAsks.length > 1) {
    const level2Ask = bookAsks[1].price;
    const level2Sz = Number(bookAsks[1]?.sizeShares) || 0;
    if (level2Sz > 0) {
      const cost2 = level2Ask + fee(level2Ask, params.market.feeRateBps ?? 700);
      const edge2 = params.q - cost2;
      if (edge2 >= CONFIG.minEdgeMargin) {
        maxFillableShares = params.level1Sz + level2Sz;
        baseAskPrice = level2Ask;
      }
    }
  }

  // Generous slippage buffer (+20 ticks) strictly bounded by minEdgeFloor (+2.5% net)
  const maxPriceForEdge = findMaxPriceForEdge(params.q, 0.025, params.market.feeRateBps ?? 700);
  const limitPriceToUse = Math.min(baseAskPrice + 0.020, maxPriceForEdge);

  const priceEstWithSlippage = limitPriceToUse + fee(limitPriceToUse, params.market.feeRateBps ?? 700);
  const rawTargetShares = Math.floor(targetSpend / Math.max(0.1, priceEstWithSlippage));
  const sharesTarget = Math.max(1, Math.min(CONFIG.maxSharesSafetyCap, rawTargetShares));

  let sharesToBuy = Math.floor(Math.min(sharesTarget, maxFillableShares));
  let sharesMicros = BigInt(sharesToBuy) * 1_000_000n;
  const limitPriceMicros = BigInt(Math.round(limitPriceToUse * 1_000_000));
  const feeRateBps = params.market.feeRateBps ?? 700;
  let feeMicros = (sharesMicros * BigInt(feeRateBps) * limitPriceMicros * (1_000_000n - limitPriceMicros)) / (10_000n * 1_000_000n * 1_000_000n);
  let baseNotionalMicros = (sharesMicros * limitPriceMicros) / 1_000_000n;
  let maxNotional = baseNotionalMicros + feeMicros;

  // Daily.fun strict $1.00 minimum notional requirement
  if (baseNotionalMicros < 1_000_000n) {
    const minSharesForDollar = Math.ceil(1.0 / limitPriceToUse);
    if (minSharesForDollar <= maxFillableShares && minSharesForDollar <= CONFIG.maxSharesSafetyCap) {
      const minDollarSpend = minSharesForDollar * priceEstWithSlippage;
      if (minDollarSpend <= maxSafeSpend) {
        sharesToBuy = minSharesForDollar;
        sharesMicros = BigInt(sharesToBuy) * 1_000_000n;
        feeMicros = (sharesMicros * BigInt(feeRateBps) * limitPriceMicros * (1_000_000n - limitPriceMicros)) / (10_000n * 1_000_000n * 1_000_000n);
        baseNotionalMicros = (sharesMicros * limitPriceMicros) / 1_000_000n;
        maxNotional = baseNotionalMicros + feeMicros;
      } else {
        console.log(new Date().toISOString(), `SKIPPING TRADE: notional < $1.00 and cannot safely fund min trade`);
        return;
      }
    } else {
      console.log(new Date().toISOString(), `SKIPPING TRADE: notional < $1.00 and book depth insufficient`);
      return;
    }
  }

  if (sharesToBuy < 1) {
    console.log(new Date().toISOString(), 'SKIPPING TRADE: sizeShares < 1');
    return;
  }

  console.log(new Date().toISOString(), `[SIZING] balance=${balance.toFixed(2)}$ targetSpend=${targetSpend.toFixed(2)}$ notional=${(Number(baseNotionalMicros)/1e6).toFixed(2)}$ buying=${sharesToBuy} limitPrice=${limitPriceToUse}`);

  prewarmMarket(params.market.marketPda); // no-op when done at discovery
  const freshSession = await getSession(); // cached; waits on HTTP only when the cache is cold

  if (!freshSession || !freshSession.cashSession?.enabled) {
    console.error(new Date().toISOString(), 'ABORTING TRADE: could not load cash session with active nextNonce');
    return;
  }

  const { frame: tradeFrame, requestId, clientOrderId } = buildOrderFrame(freshSession, {
    marketPda: params.market.marketPda, side: 'buy', outcomeIndex: params.targetOutcome, limitPriceMicros, sharesMicros, maxNotional, minFillShares: 1_000_000n
  });

  const t_sent_ms = Date.now();
  console.log(new Date().toISOString(), CONFIG.paperMode ? '[PAPER] TRADE SIGNAL:' : '[LIVE] SUBMITTING TRADE:', JSON.stringify({
    slug: params.slug,
    side: params.targetSideName,
    shares: sharesToBuy,
    price: params.level1Ask,
    edge: params.edge.toFixed(3),
    q: params.q.toFixed(3),
    d: params.d.toFixed(1),
    sigmaM: params.sigmaM.toFixed(3)
  }));

  if (!CONFIG.paperMode) {
    if (dailyWs && dailyWs.readyState === WebSocket.OPEN) {
      dailyWs.send(JSON.stringify(tradeFrame));
      spendNonce(); // refetched when the result arrives
    } else {
      console.error('Cannot send trade: Daily WS not open');
      return;
    }
  }

  // Microstructure Trade Log
  logTrade({
    mode: CONFIG.paperMode ? 'paper' : 'live',
    marketId: params.market.marketPda,
    roundId: params.slug,
    requestId,
    clientOrderId,
    t_eval_ms,
    t_sent_ms,
    startMs: params.market.startMs,
    endMs: params.market.endMs,
    remaining_seconds: params.remainingSeconds,
    f0: params.f0,
    f_now: params.f_now,
    d: params.d,
    sigma_m: params.sigmaM,
    tau: params.tau,
    q: params.q,
    side: params.targetSideName,
    level1_ask: params.level1Ask,
    level1_sz: params.level1Sz,
    level2_ask: params.book.asks[1]?.price ?? null,
    level2_sz: params.book.asks[1]?.sizeShares ?? null,
    opp_best_bid: params.oppBook?.bids[0]?.price ?? null,
    opp_best_ask: params.oppBook?.asks[0]?.price ?? null,
    shares_requested: sharesToBuy,
    shares_micros: sharesMicros.toString(),
    max_notional_micros: maxNotional.toString(),
    limit_price_micros: limitPriceMicros.toString(),
    edge: params.edge,
    ...(params.add ? { add: true } : {}),
    status: CONFIG.paperMode ? 'paper_trade' : 'intent_sent'
  });

  if (params.add) { // D1: the round's position grows when the venue fills it (trackOrderResult)
    params.add.pendingAdd = { requestId, sentAt: Date.now(), shares: sharesToBuy };
    saveBotState();
    return;
  }

  // Track open position for dynamic Stop-Loss
  activePositions.set(params.slug, {
    market: params.market,
    buyRequestId: requestId,
    targetOutcome: params.targetOutcome,
    targetSideName: params.targetSideName,
    sharesBought: sharesToBuy,
    f0: params.f0,
    entryPrice: params.level1Ask,
    entryTau: params.tau,
    entryEdge: params.edge,
    cashoutAttempted: false,
    buySentAt: Date.now(),
    buyAccepted: false,
    buyProcessed: false
  });
  saveBotState();
}

// One signed FAK order for the Daily WS, used by the bot and the LightGBM leg. A buy carries the cash authorization of its
// maxNotional at the session's next nonce; a sell signs none (the Daily.fun protocol omits it for taker sells).
function buildOrderFrame(session, { marketPda, side, outcomeIndex, limitPriceMicros, sharesMicros, maxNotional, minFillShares }) {
  const salt = BigInt(Math.floor(Math.random() * 1e15));
  const expiry = Math.floor(Date.now() / 1000) + 4;
  const requestId = crypto.randomUUID();
  const clientOrderId = crypto.randomUUID();
  const intent = { side, outcomeIndex, timeInForce: 'fak', limitPriceMicros, maxShares: sharesMicros, maxNotional, minFillShares, salt, expiry };
  const market = new PublicKey(marketPda);
  const vault = new PublicKey(session.vault);
  const tradingSession = new PublicKey(session.session);
  const intentHash = buildIntentHash(intent, CONFIG.programId, market, CONFIG.usdcMint, OWNER, vault, tradingSession);
  const frame = { type: 'daily_markets_trade', requestId, clientOrderId, observeProcessed: true, acceptQueued: true, routeRevision: 1 };
  if (side === 'buy') {
    const key = `${session.session}:${session.cashSession.sessionGeneration}`;
    let nonce = BigInt(session.cashSession.nextNonce);
    if (nonceFloor.key === key && Date.now() - nonceFloor.at < 10000 && nonce < nonceFloor.nonce) nonce = nonceFloor.nonce;
    signedNonces.set(requestId, { key, nonce }); // ponytail: a buy never answered stays here, a few bytes a day
    const cashSig = nacl.sign.detached(buildCashActionHash(session.cashSession, intentHash, maxNotional, expiry, nonce), SECRET_KEY);
    frame.cashAuthorization = {
      protocolVersion: 1,
      sessionAuthority: AUTHORITY.toBase58(),
      sessionGeneration: session.cashSession.sessionGeneration.toString(),
      deploymentDomain: session.cashSession.deploymentDomain.toLowerCase(),
      amount: maxNotional.toString(),
      expiry,
      nonce: nonce.toString(),
      signature: Buffer.from(cashSig).toString('base64')
    };
  }
  frame.intent = {
    protocolVersion: 4,
    market: market.toBase58(),
    owner: OWNER.toBase58(),
    sessionAuthority: AUTHORITY.toBase58(),
    takerVault: vault.toBase58(),
    tradingSession: tradingSession.toBase58(),
    usdcMint: CONFIG.usdcMint.toBase58(),
    side,
    outcomeIndex,
    timeInForce: 'fak',
    limitPriceMicros: limitPriceMicros.toString(),
    maxShares: sharesMicros.toString(),
    maxNotional: maxNotional.toString(),
    minFillShares: minFillShares.toString(),
    salt: salt.toString(),
    expiry,
    signature: Buffer.from(nacl.sign.detached(intentHash, SECRET_KEY)).toString('base64')
  };
  return { frame, requestId, clientOrderId };
}

// Dynamic Stop-Loss / Cash Out Execution
async function executeCashOut(params) {
  try {
    const tokenId = `${params.market.marketPda}:${params.outcomeIndex}`;
    const book = books.get(tokenId);
    const bestBid = book?.bids?.[0]?.price || 0.25;
    console.log(new Date().toISOString(), `[CASHOUT EXECUTION] Attempting cashout for ${params.slug} shares=${params.sharesToSell} bestBid=${bestBid}`);
    
    // Selling on Daily.fun taker order
    const failedAttempts = Math.max(0, (params.pos?.cashoutAttempts || 1) - 1);
    // 15 c into the bid, 2 c more per failed attempt: the sale fills at the book, the limit only lets a moving bid through. Owner's G6
    // (2026-11-09): at 5 c, 21 of 101 first sales were refused and the retry 0.68 s later sold lower, 38.9 USD over 17 chains in 2.54 days.
    const limitPriceToUse = Math.max(0.05, bestBid - 0.15 - 0.02 * failedAttempts);
    const limitPriceMicros = BigInt(Math.round(limitPriceToUse * 1_000_000));
    const sharesMicros = BigInt(Math.round(params.sharesToSell * 1_000_000));
    // The site signs sells with the u64 maximum: a real value caps the proceeds and truncates the fill.
    const maxNotional = 0xffffffffffffffffn;

    const freshSession = await getSession(true);
    if (!freshSession) {
      console.warn(new Date().toISOString(), `[CASHOUT SKIPPED] ${params.slug} trading session unavailable`);
      return;
    }
    if (!freshSession?.cashSession?.enabled) {
      console.warn(new Date().toISOString(), `[CASHOUT SKIPPED] ${params.slug} cash session disabled`);
      return;
    }

    const { frame: sellFrame, requestId } = buildOrderFrame(freshSession, {
      marketPda: params.market.marketPda, side: 'sell', outcomeIndex: params.outcomeIndex, limitPriceMicros, sharesMicros, maxNotional, minFillShares: 1n
    });

    if (!dailyWs || dailyWs.readyState !== WebSocket.OPEN) {
      console.warn(new Date().toISOString(), `[CASHOUT SKIPPED] ${params.slug} Daily WS not open`);
      return;
    }
    params.pos.pendingSell = { requestId, sentAt: Date.now(), shares: params.sharesToSell, qtyAtSend: params.pos.sharesBought };
    dailyWs.send(JSON.stringify(sellFrame));
    console.log(new Date().toISOString(), `[CASHOUT SENT] ${params.slug} sell order sent to Daily WS`);
    logTrade({
      event: 'stop_loss_cashout_sent',
      slug: params.slug,
      side: params.sideName,
      shares: params.sharesToSell,
      bestBid,
      relDelta: params.relDelta,
      qSurv: params.qSurv
    });
  } catch (err) {
    console.error(new Date().toISOString(), '[CASHOUT ERROR]', err.message);
  }
}

// Start Main Loops
initBinance();
initDaily();
setInterval(evaluateStrategy, 200);
if (lgb) setInterval(lgbTick, 1000);
setInterval(refreshPrivySession, 20 * 60 * 1000);
fetchSession();
setInterval(fetchSession, 10000);
console.log(new Date().toISOString(), `[PRIVY] loaded token expires in ${jwtExpiresIn(PRIVY_TOKEN)}s`);
