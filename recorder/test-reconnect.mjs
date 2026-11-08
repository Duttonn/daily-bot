// Offline check of rec-daily's Daily reconnect, against two fake Daily servers on 127.0.0.1:
//   hang    accepts TCP and never answers the handshake (the socket stays CONNECTING, as after 11-08 02:44:07)
//   nobook  completes the handshake and sends only updown_pyth frames, never an updown_book (as 11-07 02:00-05:00)
// Each build runs SECS (default 150) against each server; it reconnects when the server sees a second connection and logs
// 'daily stale'. The day file must still gunzip after the SECS stop. Binance sockets and the REST poll are real (public, read-only).
// node recorder/test-reconnect.mjs recorder/rec-daily.mjs [OLD.mjs]   OLD, optional, is a control build whose URL is read from DAILY_WS.
// KINDS=hang limits the cases. Run it on the VPS too: there (Node 22) close() on a CONNECTING socket never fires onclose.
import net from 'node:net'; import crypto from 'node:crypto'; import { spawn } from 'node:child_process'; import fs from 'node:fs'; import zlib from 'node:zlib'; import os from 'node:os'; import path from 'node:path';
const [NEW, OLD] = process.argv.slice(2), SECS = +(process.env.SECS || 150), RUNS = fs.mkdtempSync(path.join(os.tmpdir(), 'rec-reconnect-')) + '/';
function server(kind) {
  const st = { conns: 0, up: new Set() };
  const srv = net.createServer((c) => {
    st.conns++; c.on('error', () => {});
    if (kind === 'hang') return;
    let buf = '';
    c.on('data', (d) => {
      if (st.up.has(c)) return; // client frames (ping, subscribe) are ignored
      buf += d; const m = buf.match(/Sec-WebSocket-Key: *(\S+)/i); if (!buf.includes('\r\n\r\n') || !m) return;
      const acc = crypto.createHash('sha1').update(m[1] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
      c.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${acc}\r\n\r\n`); st.up.add(c);
      const iv = setInterval(() => { const p = Buffer.from(JSON.stringify({ type: 'updown_pyth', symbol: 'BTC', v: 1, t: Date.now(), seq: 1 })); c.write(Buffer.concat([Buffer.from([0x81, p.length]), p])); }, 1000);
      c.on('close', () => clearInterval(iv));
    });
  });
  return new Promise((r) => srv.listen(0, '127.0.0.1', () => r({ srv, st, port: srv.address().port })));
}
const runs = [];
console.log('runs in', RUNS);
for (const [name, file] of [['new', NEW], ['old', OLD]].filter(([, f]) => f)) for (const kind of (process.env.KINDS || 'hang,nobook').split(',')) {
  const s = await server(kind), out = `${RUNS}${name}-${kind}/`; fs.mkdirSync(out, { recursive: true });
  const p = spawn(process.execPath, [file], { env: { ...process.env, DAILY_WS: `ws://127.0.0.1:${s.port}`, OUT: out, SECS: String(SECS), TAP_PORT: '' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = ''; p.stdout.on('data', (d) => (log += d)); p.stderr.on('data', (d) => (log += d));
  runs.push(new Promise((r) => p.on('exit', (code) => r({ name, kind, code, conns: s.st.conns, log, out }))));
}
for (const r of await Promise.all(runs)) {
  fs.writeFileSync(`${r.out}stdout.log`, r.log);
  let gz = 'none'; for (const f of fs.readdirSync(r.out).filter((f) => f.endsWith('.gz'))) { try { zlib.gunzipSync(fs.readFileSync(r.out + f)); gz = 'ok'; } catch (e) { gz = 'BAD ' + e.message; } }
  const stale = (r.log.match(/daily stale.*/g) || []).join(' | '), verdict = r.name === 'new' ? (stale && r.conns >= 2 && r.code === 0 && gz === 'ok' ? 'PASS' : 'FAIL') : 'control';
  console.log(verdict.padEnd(7), `${r.name}/${r.kind}`.padEnd(11), 'connections', r.conns, 'exit', r.code, 'gzip', gz, stale ? '| ' + stale.slice(0, 200) : '');
}
process.exit(0);
