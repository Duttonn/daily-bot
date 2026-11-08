// Daily.fun recorder (Solana, BTC 5 min Up/Down, Pyth settlement, CLOB where the operator is the main market maker), read-only public feeds,
// no account, no key, never the auth channel. One gzip CSV per UTC day in $OUT (default ./data), same shape as rec.mjs: t,src,...  (t = local receipt ms)
//   t,mk,SYM,slug,marketId,marketPda,startMs,endMs,{json}   window seen in updown_markets / REST (feeRateBps, priceToBeat source and status)
//   t,ptb,SYM,slug,priceToBeat,tickTime                     the strike (Pyth price at the window start tick) when it locks
//   t,bk,SYM,slug,side,bid,bidSz,ask,askSz,[[bids],[asks]]  book of the Up (side 0) and Down (side 1) token, top 3 levels [price,shares], on change
//   t,py,SYM,price,pubTs,seq                                Pyth Lazer tick of the settlement feed (updown_pyth, ~4/s for BTC)
//   t,sp,SYM,price,ts                                       Daily "spot" reference (updown_spot, 1/s: BTC ETH SOL XRP)
//   t,tr,SYM,slug,{json}                                    updown_public_trades entries (empty so far)
//   t,rs,SYM,slug,payoutUp,openPrice,closePrice,mode,resolvedAt   resolution from GET /chain-api/daily-markets/{marketId} (state resolved, payoutUp 1000000 = Up won)
//   t,bs,SYM,bid,ask                                        Binance spot bookTicker (written on change)
//   t,bm,ch,tokenId,serverTime,indexedAt                    every book the websocket pushes (ch b = updown_book, m = a book inside updown_markets), the server's own stamps in ms (bk has none, ws-lag.mjs reads these)
//   t,pg,rttMs                                              round trip of the 20 s {type:ping} to its pong (the pong carries no time)
//   t,bn,SYM,E                                              Binance spot miniTicker event time, own socket, 1/s per symbol (bs has no server time: the spot bookTicker carries none)
// SECS=n stops after n seconds (test run). SIGTERM and SIGINT end the gzip member and exit. Every 10 min the log prints lines per source and kB/s received.
import { createWriteStream, mkdirSync } from 'node:fs';
import { createGzip } from 'node:zlib';
import { createSocket } from 'node:dgram';
process.on('unhandledRejection', (e) => console.log(new Date().toISOString(), 'unhandled', e?.message)); // a stray rejection must not stop a long recording
const OUT = process.env.OUT || new URL('./data/', import.meta.url).pathname, API = 'https://daily.fun/chain-api', SPOT = ['BTC', 'ETH', 'SOL', 'XRP'];
mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// TAP_PORT=n[,m...] sends a copy of every line to 127.0.0.1 on each port (UDP, fire and forget) for local traders; off by default
const TAPS = String(process.env.TAP_PORT || '').split(',').map(Number).filter((p) => p > 0), tap = TAPS.length ? createSocket('udp4') : null;
if (tap) tap.on('error', () => {});
let day = '', gz = null, file = null, bytes = 0, t0 = Date.now(), stopping = false; const counts = {};
function w(src, line) {
  if (stopping) return; // a write after gz.end() throws ERR_STREAM_WRITE_AFTER_END and leaves the member without its trailer
  const t = Date.now(), d = new Date(t).toISOString().slice(0, 10);
  if (d !== day) { if (gz) gz.end(); day = d; gz = createGzip(); file = createWriteStream(`${OUT}daily-${d}.csv.gz`, { flags: 'a' }); gz.pipe(file); }
  const s = t + ',' + src + ',' + line + '\n'; gz.write(s); counts[src] = (counts[src] || 0) + 1;
  if (tap) for (const p of TAPS) tap.send(s, p, '127.0.0.1');
}
setInterval(() => gz && gz.flush(), 30000);
const report = () => { console.log(new Date().toISOString(), JSON.stringify(counts), (bytes / (Date.now() - t0)).toFixed(1) + ' kB/s'); for (const k in counts) counts[k] = 0; bytes = 0; t0 = Date.now(); };
setInterval(report, 600000);

async function get(path) {
  for (let k = 0; k < 4; k++) {
    const r = await fetch(API + path, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(15000) });
    if (r.status === 429) { await sleep(2000 * (k + 1)); continue; }
    if (!r.ok) throw new Error(path + ' ' + r.status);
    return r.json();
  }
  throw new Error(path + ' 429');
}

// ---- windows: from the websocket push (updown_markets) with a REST poll as backup ----
const mkts = new Map(); // slug -> {sym, marketId, pda, startMs, endMs, ptb, done, nextTry, tries}
let ws = null, bookKey = '';
const bk = {}; // tokenId -> last signature
function seeMarket(x) {
  let m = mkts.get(x.slug);
  if (!m) {
    m = { sym: x.symbol, marketId: x.marketId, pda: x.marketPda, startMs: Date.parse(x.startTime), endMs: Date.parse(x.endTime), ptb: null, done: false, nextTry: Date.parse(x.endTime) + 1000, tries: 0 };
    mkts.set(x.slug, m);
    w('mk', [x.symbol, x.slug, x.marketId, x.marketPda, m.startMs, m.endMs, JSON.stringify({ fee: x.feeRateBps, ptbSrc: x.priceToBeatSource, preOpen: x.preOpen, type: x.marketType, exec: x.executionStatus })].join(','));
  }
  if (x.priceToBeatStatus === 'locked' && x.priceToBeat != null && m.ptb !== x.priceToBeat) { m.ptb = x.priceToBeat; w('ptb', [x.symbol, x.slug, x.priceToBeat, x.priceToBeatTickTime].join(',')); }
  for (const o of x.outcomes || []) if (o.book) onBook(o.book, false);
}
function wantBooks() {
  const ids = []; for (const m of mkts.values()) if (m.endMs > Date.now() - 3000) ids.push(m.pda + ':0', m.pda + ':1');
  const key = ids.sort().join(','); if (key !== bookKey && ids.length && ws && ws.readyState === 1) { bookKey = key; ws.send(JSON.stringify({ type: 'updown_book_subscribe', tokenIds: ids })); }
}
function onBook(b, live) {
  const pda = b.tokenId.split(':')[0], side = +b.tokenId.split(':')[1];
  let slug = null, sym = 'BTC'; for (const [s, m] of mkts) if (m.pda === pda) { slug = s; sym = m.sym; }
  if (!slug) return;
  const top = (x) => (x || []).slice(0, 3).map((l) => [l.price, l.sizeShares]), bids = top(b.bids), asks = top(b.asks), j = JSON.stringify([bids, asks]);
  if (bk[b.tokenId] === j) return; bk[b.tokenId] = j;
  w('bk', [sym, slug, side, bids[0]?.[0] ?? '', bids[0]?.[1] ?? '', asks[0]?.[0] ?? '', asks[0]?.[1] ?? '', j].join(','));
}
const wb = (ch, b) => w('bm', [ch, b.tokenId, b.serverTime ?? '', Date.parse(b.indexedAt) || ''].join(','));
async function restPoll() { try { const r = await get('/updowns/markets'); for (const x of r.markets || []) seeMarket(x); wantBooks(); } catch (e) { console.log(new Date().toISOString(), 'rest', e.message); } }
async function resolveLoop() {
  for (const [slug, m] of mkts) {
    if (m.done) { if (Date.now() - m.endMs > 1800000) mkts.delete(slug); continue; }
    if (Date.now() < m.nextTry) continue;
    try {
      const d = (await get(`/daily-markets/${m.marketId}`)).market, r = d.resolution || {};
      if (d.state === 'resolved') { m.done = true; w('rs', [m.sym, slug, d.payoutVector?.[0] ?? '', r.openPrice ? +r.openPrice / 1e8 : '', r.closeTwapPrice ? +r.closeTwapPrice / 1e8 : '', r.mode ?? '', d.resolvedAt ?? ''].join(',')); }
      else { m.tries++; m.nextTry = Date.now() + (m.tries < 40 ? 3000 : 30000); }
    } catch (e) { m.nextTry = Date.now() + 5000; }
  }
}

// ---- Daily websocket: Origin header required; only the public channels (no auth) ----
let pingAt = 0;
function connect() {
  let last = Date.now(); ws = new WebSocket('wss://ws.daily.fun/ws', { headers: { Origin: 'https://daily.fun' } }); bookKey = '';
  ws.onopen = () => { console.log(new Date().toISOString(), 'daily open'); ws.send(JSON.stringify({ type: 'updown_subscribe' })); wantBooks(); };
  ws.onmessage = (ev) => {
    last = Date.now(); bytes += ev.data.length; let o; try { o = JSON.parse(ev.data); } catch (e) { return; }
    switch (o.type) {
      case 'updown_markets': for (const x of o.markets || []) { for (const oc of x.outcomes || []) if (oc.book) wb('m', oc.book); seeMarket(x); } wantBooks(); break;
      case 'updown_book': wb('b', o); onBook(o, true); break;
      case 'pong': if (pingAt) w('pg', Date.now() - pingAt); pingAt = 0; break;
      case 'updown_pyth': if (o.symbol === 'BTC' || [...mkts.values()].some((m) => m.sym === o.symbol)) w('py', [o.symbol, o.v, o.sourceT ?? o.t, o.seq].join(',')); break;
      case 'updown_spot': w('sp', [o.symbol, o.v, o.t].join(',')); break;
      case 'updown_public_trades': for (const t of o.trades || []) w('tr', `${t.symbol || 'BTC'},${t.slug || ''},${JSON.stringify(t)}`); break;
      case 'error':
        console.log(new Date().toISOString(), 'daily error', o.code, o.message);
        // The venue drops the fast updown_book channel on this error and leaves only 2 s books in updown_markets: reconnect, like the bot.
        if (o.code === 'subscription_limit') { try { ws.close(); } catch (e) {} }
        break;
      default: break;
    }
  };
  ws.onclose = () => { console.log(new Date().toISOString(), 'daily closed'); clearInterval(watch); setTimeout(connect, 3000); };
  ws.onerror = () => {};
  const watch = setInterval(() => { if (ws.readyState === 1) { pingAt = Date.now(); ws.send(JSON.stringify({ type: 'ping' })); } if (Date.now() - last > 60000) { last = Date.now(); try { ws.close(); } catch (e) {} } }, 20000);
}

// ---- Binance spot bookTicker, same source as rec.mjs ----
function sock(name, url, onMsg) {
  let s, l = Date.now();
  const open = () => { s = new WebSocket(url); l = Date.now(); s.onopen = () => console.log(new Date().toISOString(), name, 'open'); s.onmessage = (ev) => { l = Date.now(); bytes += ev.data.length; try { onMsg(JSON.parse(ev.data)); } catch (e) {} }; s.onclose = () => { console.log(new Date().toISOString(), name, 'closed'); setTimeout(open, 3000); }; s.onerror = () => {}; };
  setInterval(() => { if (Date.now() - l > 60000) { l = Date.now(); try { s.close(); } catch (e) {} } }, 20000);
  open();
}
const bs = {};
sock('binance-spot', `wss://stream.binance.com:9443/stream?streams=${SPOT.map((s) => `${s.toLowerCase()}usdt@bookTicker`).join('/')}`, (o) => {
  const d = o.data; if (!d || !d.s) return; const s = d.s.slice(0, -4), p = (bs[s] = bs[s] || {});
  if (d.b !== p.b || d.a !== p.a) { p.b = d.b; p.a = d.a; w('bs', `${s},${d.b},${d.a}`); }
});

// a second, lone Binance socket: its miniTicker carries an event time (the bookTicker above has none), so receipt minus E is this machine's lag to a reference server
sock('binance-time', `wss://stream.binance.com:9443/stream?streams=${SPOT.map((s) => `${s.toLowerCase()}usdt@miniTicker`).join('/')}`, (o) => { const d = o.data; if (d && d.s && d.E) w('bn', `${d.s.slice(0, -4)},${d.E}`); });

// t,bf,SYM,bid,ask,E   Binance USDT perp bookTicker, the trading bot's own reference, on change, with the exchange event time
const bf = {};
sock('binance-perp', 'wss://fstream.binance.com/stream?streams=btcusdt@bookTicker', (o) => {
  const d = o.data; if (!d || !d.s) return; const p = (bf[d.s] = bf[d.s] || {});
  if (d.b !== p.b || d.a !== p.a) { p.b = d.b; p.a = d.a; w('bf', `${d.s.slice(0, -4)},${d.b},${d.a},${d.E}`); }
});

const stop = () => { stopping = true; report(); if (!file) process.exit(0); file.on('close', () => process.exit(0)); gz.end(); };
if (process.env.SECS) setTimeout(stop, +process.env.SECS * 1000); // armed first: a slow start (rate limits) must not delay the stop
process.on('SIGTERM', stop); process.on('SIGINT', stop); process.on('SIGHUP', stop); // tmux kill-session sends SIGHUP // a kill must close the gzip member, or a restart appending to the day file makes it unreadable
connect();
await restPoll(); setInterval(restPoll, 20000); setInterval(resolveLoop, 2000);
