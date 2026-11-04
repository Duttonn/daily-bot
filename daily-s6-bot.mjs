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

// Resolve credentials
const rawOwner = process.env.OWNER_ADDRESS || fileEnv.OWNER_ADDRESS || authData?.owner || null;
const rawSecret = process.env.SESSION_SECRET_KEY || fileEnv.SESSION_SECRET_KEY || authData?.secretKey || null;
let PRIVY_TOKEN = process.env.PRIVY_TOKEN || fileEnv.PRIVY_TOKEN || authData?.privyToken || null;
let PRIVY_REFRESH_TOKEN = process.env.PRIVY_REFRESH_TOKEN || fileEnv.PRIVY_REFRESH_TOKEN || authData?.privyRefreshToken || null;

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
  dailyUsdcAta: process.env.DAILY_USDC_ATA || null, // Dynamically resolved on startup from VAULT
  paperMode: isPaperMode,

  // S6 Mathematical Strategy (Locked pre-commitment)
  windowRemainingMin: 118, // F20: 2 minutes before round expiry (118s - 122s remaining)
  windowRemainingMax: 122,
  calmVolThreshold: 2.30,  // sigma_m filter ($/sqrt(s))
  cModel: 1.0,             // S6 forward model scaling factor
  minEdgeMargin: 0.05,     // Edge margin floor (net of fees)

  // Dynamic Capital Sizing
  sizingFraction: parseFloat(process.env.SIZING_FRACTION || '0.12'), // Default 12% of live USDC balance per trade
  maxSharesSafetyCap: 1000,    // Safety ceiling to absorb Level 1 + Level 2 liquidity depth
  minBalanceFloorRatio: 0.50,  // 50% capital drawdown stop-loss
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

console.log(new Date().toISOString(), 'DAILY S6 BOT INITIALIZED:');
console.log('  Mode:           ', CONFIG.paperMode ? 'PAPER (Simulation only)' : 'LIVE (Capital Active)');
console.log('  Owner:          ', OWNER.toBase58());
console.log('  Session Auth:   ', AUTHORITY.toBase58());
console.log('  Vault PDA:      ', VAULT.toBase58());
console.log('  Session PDA:    ', SESSION.toBase58());
console.log('  Timing Window:  ', `[${CONFIG.windowRemainingMin}s, ${CONFIG.windowRemainingMax}s] remaining (F20)`);
console.log('  Calm Filter:    ', `sigma_m < ${CONFIG.calmVolThreshold} $/sqrt(s)`);
console.log('  Edge Margin:    ', `edge >= +${CONFIG.minEdgeMargin}$`);
console.log('  Capital Sizing: ', `${(CONFIG.sizingFraction * 100)}% of live balance per trade`);

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
      PRIVY_TOKEN = data.token;
      if (data.refresh_token) PRIVY_REFRESH_TOKEN = data.refresh_token;

      // Persist rotated tokens
      if (fs.existsSync(AUTH_FILE)) {
        try {
          const a = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8'));
          a.privyToken = PRIVY_TOKEN;
          if (data.refresh_token) a.privyRefreshToken = data.refresh_token;
          fs.writeFileSync(AUTH_FILE, JSON.stringify(a, null, 2));
        } catch(e) {}
      }

      authFailed = false;
      console.log(new Date().toISOString(), '[PRIVY AUTO-REFRESH SUCCESS] Token auto-rotated successfully');
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
const markets = new Map(); // slug -> market meta
const books = new Map(); // tokenId -> { bids, asks }
const tradedRounds = new Set();
let subscribedTokenIds = '';
let authFailed = false;
let currentUsdcBalance = null;
let initialDetectedBalance = null;
let lastBalanceCheck = 0;
let vaultUsdcAta = CONFIG.dailyUsdcAta;

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
      if (initialDetectedBalance === null && currentUsdcBalance > 0) {
        initialDetectedBalance = currentUsdcBalance;
        console.log(new Date().toISOString(), `[CAPITAL ACTIF DÉTECTÉ] Solde initial: ${initialDetectedBalance.toFixed(2)}$ | Sizing proportionnel: ${(CONFIG.sizingFraction * 100)}% par trade`);
      } else if (initialDetectedBalance !== null && currentUsdcBalance > initialDetectedBalance * 1.15) {
        console.log(new Date().toISOString(), `[DÉPÔT DÉTECTÉ] Solde rehaussé: ${initialDetectedBalance.toFixed(2)}$ -> ${currentUsdcBalance.toFixed(2)}$ | Floor stop-loss ajusté`);
        initialDetectedBalance = currentUsdcBalance;
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
    updateUsdcBalance();
  } else if (msg.type === 'authed') {
    isDailyAuthed = true;
    console.log(new Date().toISOString(), 'DAILY AUTH SUCCESS:', JSON.stringify(msg));
  } else if (msg.type === 'error' || msg.type === 'daily_markets_error') {
    console.error(new Date().toISOString(), 'DAILY ERROR MSG:', JSON.stringify(msg));
    logTrade({ event: 'daily_error', code: msg.code, message: msg.message, details: msg });
    if (msg.message === 'bad token') {
      authFailed = true;
      isDailyAuthed = false;
      console.warn(new Date().toISOString(), '[BAD TOKEN] Triggering automatic Privy session refresh...');
      refreshPrivySession();
    } else if (msg.code === 'subscription_limit') {
      console.warn(new Date().toISOString(), '[SUBSCRIPTION LIMIT] Cycling Daily WS connection...');
      if (dailyWs) dailyWs.close();
    }
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
    }
  }

  if (!currentBfPrice) return;
  if (now - lastBfTickMs > 3000) return;

  if (sessionExpirySec && now / 1000 >= sessionExpirySec) {
    if (now - lastExpiredWarning > 30000) {
      lastExpiredWarning = now;
      console.error(new Date().toISOString(), '[SESSION EXPIRED] On-chain session is expired. Halting trading.');
    }
    return;
  }

  updateUsdcBalance();

  // Dynamic Stop-Loss Floor check
  const floorRatio = CONFIG.minBalanceFloorRatio;
  const configuredFloor = process.env.MIN_BALANCE_FLOOR_USD ? parseFloat(process.env.MIN_BALANCE_FLOOR_USD) : null;
  const dynamicFloor = (configuredFloor !== null && !isNaN(configuredFloor))
    ? configuredFloor
    : (initialDetectedBalance ? Math.max(CONFIG.minBalanceFloorMinUsd, initialDetectedBalance * floorRatio) : 10.0);
  if (currentUsdcBalance === null || (now - lastBalanceCheck > 60000) || currentUsdcBalance <= dynamicFloor) {
    return;
  }

  const sigmaM = calculateRealizedVol(priceHistory, 600, 500);

  if (now - lastHeartbeat > 10000) {
    lastHeartbeat = now;
    const activeBtc = Array.from(markets.values()).filter(m => m.symbol === 'BTC' && m.endMs > now);
    const nextRounds = activeBtc.map(m => `${m.slug} (${((m.endMs - now)/1000).toFixed(0)}s left)`).join(', ');
    console.log(new Date().toISOString(), `[HEARTBEAT] bf=${currentBfPrice?.toFixed(1)} ticks=${priceHistory.length} sigmaM=${sigmaM ? sigmaM.toFixed(3) : 'warmup'} usdc=${currentUsdcBalance.toFixed(2)}$ authed=${isDailyAuthed} | ${nextRounds}`);
  }

  if (!sigmaM || sigmaM >= CONFIG.calmVolThreshold) {
    return; // Volatility too high or warmup in progress
  }

  for (const [slug, m] of markets.entries()) {
    if (m.symbol !== 'BTC') continue;
    if (tradedRounds.has(slug)) continue;
    if (!m.startMs || !m.endMs) continue;

    const remainingSeconds = (m.endMs - now) / 1000;
    // F20 Window: 118s - 122s remaining (2 min before round expiry)
    if (remainingSeconds < CONFIG.windowRemainingMin || remainingSeconds > CONFIG.windowRemainingMax) {
      continue;
    }

    console.log(new Date().toISOString(), `[WINDOW ENTERED] ${slug} rem=${remainingSeconds.toFixed(1)}s sigmaM=${sigmaM?.toFixed(3)}`);

    const f0 = getPriceAt(priceHistory, m.startMs);
    if (!f0) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: startMs not covered in history`);
      continue;
    }

    const d = currentBfPrice - f0;
    if (Math.abs(d) < 1.0) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: d=${d.toFixed(2)} (< 1.0)`);
      continue;
    }

    const targetOutcome = d > 0 ? 0 : 1;
    const targetSideName = d > 0 ? 'up' : 'down';
    const tokenId = `${m.marketPda}:${targetOutcome}`;
    const oppTokenId = `${m.marketPda}:${1 - targetOutcome}`;

    const book = books.get(tokenId);
    const oppBook = books.get(oppTokenId);
    
    if (!book || !book.asks || book.asks.length === 0 || !oppBook || !oppBook.asks || oppBook.asks.length === 0) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: missing book on one or both sides`);
      continue;
    }

    if (book.stale || oppBook.stale) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: stale book`);
      continue;
    }
    const bookAge = book.receivedAtMs ? (now - book.receivedAtMs) : 99999;
    const oppBookAge = oppBook.receivedAtMs ? (now - oppBook.receivedAtMs) : 99999;
    if (bookAge > 3000 || oppBookAge > 3000) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: book too old (target=${bookAge}ms, opp=${oppBookAge}ms > 3000ms)`);
      continue;
    }

    tradedRounds.add(slug);

    const level1Ask = book.asks[0].price;
    const level1Sz = book.asks[0].sizeShares;
    if (!level1Sz || isNaN(level1Sz) || level1Sz < 1) {
      console.log(new Date().toISOString(), `[SKIP] ${slug}: invalid level1Sz=${level1Sz}`);
      continue;
    }

    const tau = remainingSeconds;
    const q = normalCdf(Math.abs(d) / (CONFIG.cModel * sigmaM * Math.sqrt(Math.max(tau, 1))));
    const cost = level1Ask + fee(level1Ask, m.feeRateBps ?? 700);
    const edge = q - cost;

    console.log(new Date().toISOString(), `[EVALUATED] ${slug} side=${targetSideName} d=${d.toFixed(1)} q=${q.toFixed(3)} ask=${level1Ask} cost=${cost.toFixed(3)} edge=${edge.toFixed(3)} (min=${CONFIG.minEdgeMargin})`);

    if (edge >= CONFIG.minEdgeMargin) {
      executeTrade({
        slug,
        market: m,
        remainingSeconds,
        tau,
        f0,
        f_now: currentBfPrice,
        d,
        sigmaM,
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
  const balance = currentUsdcBalance;
  const floorRatio = CONFIG.minBalanceFloorRatio;
  const configuredFloor = process.env.MIN_BALANCE_FLOOR_USD ? parseFloat(process.env.MIN_BALANCE_FLOOR_USD) : null;
  const dynamicFloor = (configuredFloor !== null && !isNaN(configuredFloor))
    ? configuredFloor
    : (initialDetectedBalance ? Math.max(CONFIG.minBalanceFloorMinUsd, initialDetectedBalance * floorRatio) : 10.0);
  const maxSafeSpend = Math.max(0, balance - dynamicFloor - 0.5);
  let targetSpend = Math.min(balance * CONFIG.sizingFraction, maxSafeSpend);

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

  const salt = BigInt(Math.floor(Math.random() * 1e15));
  const expiry = Math.floor(Date.now() / 1000) + 4;
  const requestId = crypto.randomUUID();
  const clientOrderId = crypto.randomUUID();

  const intent = {
    side: 'buy',
    outcomeIndex: params.targetOutcome,
    timeInForce: 'fak',
    limitPriceMicros,
    maxShares: sharesMicros,
    maxNotional,
    minFillShares: 1_000_000n,
    salt,
    expiry
  };

  const marketPda = new PublicKey(params.market.marketPda);

  // Prewarm market settlement accounts
  try {
    await fetch('https://daily.fun/chain-api/daily-markets/trading-session/prewarm', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${PRIVY_TOKEN}`,
        'Origin': CONFIG.dailyOrigin
      },
      body: JSON.stringify({
        market: marketPda.toBase58(),
        authority: AUTHORITY.toBase58(),
        routeRevision: 1
      })
    });
  } catch (e) {
    console.warn(new Date().toISOString(), 'Prewarm error (ignoring):', e.message);
  }

  // Fetch fresh session context & nextNonce
  let freshSession = null;
  try {
    const sRes = await fetch(`https://daily.fun/chain-api/daily-markets/trading-session?authority=${AUTHORITY.toBase58()}`, {
      headers: {
        'Authorization': `Bearer ${PRIVY_TOKEN}`,
        'Origin': CONFIG.dailyOrigin
      }
    });
    if (sRes.ok) {
      const sJson = await sRes.json();
      freshSession = sJson.tradingSession;
    }
  } catch (e) {
    console.error(new Date().toISOString(), 'Failed to fetch fresh session:', e.message);
  }

  if (!freshSession || !freshSession.cashSession?.enabled) {
    console.error(new Date().toISOString(), 'ABORTING TRADE: could not load cash session with active nextNonce');
    return;
  }

  const liveVaultPda = new PublicKey(freshSession.vault);
  const liveSessionPda = new PublicKey(freshSession.session);
  const currentNonce = BigInt(freshSession.cashSession.nextNonce);

  const intentHash = buildIntentHash(intent, CONFIG.programId, marketPda, CONFIG.usdcMint, OWNER, liveVaultPda, liveSessionPda);
  const signature = nacl.sign.detached(intentHash, SECRET_KEY);
  const signatureB64 = Buffer.from(signature).toString('base64');

  // Root Cash Authorization
  const cashHash = buildCashActionHash(freshSession.cashSession, intentHash, maxNotional, expiry, currentNonce);
  const cashSig = nacl.sign.detached(cashHash, SECRET_KEY);
  const cashSignatureB64 = Buffer.from(cashSig).toString('base64');

  const cashAuthorization = {
    protocolVersion: 1,
    sessionAuthority: AUTHORITY.toBase58(),
    sessionGeneration: freshSession.cashSession.sessionGeneration.toString(),
    deploymentDomain: freshSession.cashSession.deploymentDomain.toLowerCase(),
    amount: maxNotional.toString(),
    expiry: expiry,
    nonce: currentNonce.toString(),
    signature: cashSignatureB64
  };

  const tradeFrame = {
    type: 'daily_markets_trade',
    requestId,
    clientOrderId,
    observeProcessed: true,
    acceptQueued: true,
    routeRevision: 1,
    cashAuthorization,
    intent: {
      protocolVersion: 4,
      market: marketPda.toBase58(),
      owner: OWNER.toBase58(),
      sessionAuthority: AUTHORITY.toBase58(),
      takerVault: liveVaultPda.toBase58(),
      tradingSession: liveSessionPda.toBase58(),
      usdcMint: CONFIG.usdcMint.toBase58(),
      side: intent.side,
      outcomeIndex: intent.outcomeIndex,
      timeInForce: intent.timeInForce,
      limitPriceMicros: intent.limitPriceMicros.toString(),
      maxShares: intent.maxShares.toString(),
      maxNotional: intent.maxNotional.toString(),
      minFillShares: intent.minFillShares.toString(),
      salt: intent.salt.toString(),
      expiry: intent.expiry,
      signature: signatureB64
    }
  };

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
    } else {
      console.error('Cannot send trade: Daily WS not open');
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
    status: CONFIG.paperMode ? 'paper_trade' : 'intent_sent'
  });
}

// Start Main Loops
initBinance();
initDaily();
setInterval(evaluateStrategy, 200);
setInterval(refreshPrivySession, 20 * 60 * 1000);
