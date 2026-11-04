// Minimal zero-dependency local server for Daily Up/Down dashboard with in-memory caching
import { createServer } from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
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

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  
  if (url.pathname === '/' || url.pathname === '/updown') {
    const html = readFileSync(HTML_PATH, 'utf8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
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
