import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TRADES_FILE = process.env.TRADES_LOG || path.join(__dirname, 'trades.jsonl');
const BOT_LOG_FILE = process.env.BOT_LOG || path.join(__dirname, 'bot.log');

const lines = fs.existsSync(TRADES_FILE) 
  ? fs.readFileSync(TRADES_FILE, 'utf8').trim().split('\n').filter(Boolean)
  : [];

const trades = [];
const rounds = {};

for (const l of lines) {
  try {
    const o = JSON.parse(l);
    if (o.event === 'trade_executed' && o.details && o.details.type === 'daily_markets_trade_executed') {
      const ex = o.details.execution;
      trades.push({
        id: o.details.requestId,
        t: o.t,
        iso: o.iso,
        market: ex.market,
        side: ex.outcomeIndex === 0 ? 'UP' : 'DOWN',
        size: ex.acceptedSize / 1e6,
        price: ex.acceptedAveragePriceMicros / 1e6,
        cost: ex.acceptedNotional / 1e6,
        fee: ex.acceptedFee / 1e6,
        sig: ex.acceptedSignature
      });
    }
    if (o.event === 'daily_ws_event' && o.details && o.details.nativePositions) {
      for (const p of o.details.nativePositions) {
        if (p.market) {
          const mKey = p.market.marketPda || p.market.marketId;
          const pv = p.market.payoutVector;
          const meta = p.market.outcomes && p.market.outcomes[0] ? p.market.outcomes[0].metadata : {};
          rounds[mKey] = {
            pda: p.market.marketPda,
            marketId: p.market.marketId,
            id: p.market.id,
            slug: p.market.slug,
            title: p.market.title,
            closeTs: p.market.closeTs,
            windowStart: meta.windowStart ? parseInt(meta.windowStart) : null,
            orderCloseTs: meta.orderCloseTs ? parseInt(meta.orderCloseTs) : null,
            openPrice: meta.openPrice ? parseFloat(meta.openPrice) / 1e8 : null,
            state: p.market.state,
            resolution: pv ? (pv[0] === '1000000' ? 'UP' : 'DOWN') : null
          };
        }
      }
    }
  } catch(e) {}
}

let cumPnl = 0;
const startBalance = 51.62;

const list = trades.map((t, idx) => {
  const r = rounds[t.market] || {};
  const tradeSec = Math.floor(t.t / 1000);
  let secondsRemaining = 182;
  let elapsedSeconds = 118;
  if (r.orderCloseTs) {
    secondsRemaining = r.orderCloseTs - tradeSec;
    elapsedSeconds = 300 - secondsRemaining;
  }
  const won = r.resolution ? (r.resolution === t.side) : null;
  const payout = won ? t.size * 1.0 : 0.0;
  const pnl = payout - t.cost - t.fee;
  cumPnl += pnl;
  const roi = (pnl / (t.cost + t.fee)) * 100;
  return {
    index: idx + 1,
    time: t.iso,
    slug: r.slug || t.market,
    openPrice: r.openPrice,
    side: t.side,
    price: t.price,
    size: t.size,
    cost: t.cost,
    fee: t.fee,
    totalMise: t.cost + t.fee,
    secondsRemaining,
    elapsedSeconds,
    resolution: r.resolution,
    won,
    payout,
    pnl: Math.round(pnl * 100) / 100,
    cumPnl: Math.round(cumPnl * 100) / 100,
    balanceAfter: Math.round((startBalance + cumPnl) * 100) / 100,
    roi: Math.round(roi * 10) / 10,
    sig: t.sig
  };
});

// Parse latest heartbeat from bot.log
let heartbeat = { bf: null, sigmaM: null, usdc: 49.22, authed: true, time: new Date().toISOString() };
try {
  if (fs.existsSync(BOT_LOG_FILE)) {
    const botLog = fs.readFileSync(BOT_LOG_FILE, 'utf8');
  const hbMatches = botLog.match(/\[HEARTBEAT\][^\n]+/g);
  if (hbMatches && hbMatches.length) {
    const lastHb = hbMatches[hbMatches.length - 1];
    const bfMatch = lastHb.match(/bf=([\d.]+)/);
    const sigmaMatch = lastHb.match(/sigmaM=([\d.]+)/);
    const usdcMatch = lastHb.match(/usdc=([\d.]+)[$]/);
    const authedMatch = lastHb.match(/authed=(\w+)/);
    if (bfMatch) heartbeat.bf = parseFloat(bfMatch[1]);
    if (sigmaMatch) heartbeat.sigmaM = parseFloat(sigmaMatch[1]);
    if (usdcMatch) heartbeat.usdc = parseFloat(usdcMatch[1]);
    if (authedMatch) heartbeat.authed = authedMatch[1] === 'true';
    }
  }
} catch(e) {}

console.log(JSON.stringify({
  trades: list,
  balance: heartbeat.usdc || (Math.round((startBalance + cumPnl) * 100) / 100),
  heartbeat,
  updatedAt: new Date().toISOString()
}));
