"""Send a recorder file (daily-YYYY-MM-DD.csv.gz) through UDP to a paper trader in replay mode, one line per datagram, as the recorder does.
python3 replay.py FILE.csv.gz --port 47012 [--until-ms T] [--rate 8000] [--only-used]
--only-used sends only the line types the trader reads (bf bk mk ptb rs); the trader output does not change, the load does.
Sends every line from the start of the file (the trader needs the same first tick and history as the offline extract), stops after the
first line with t > until, then sends the end marker "0,__end__". Tolerates a gzip still being written (drops the last partial line)."""
import sys, zlib, socket, time, argparse

def read_lines(path, chunk=1 << 20):
    """streaming gzip reader (multi-member, tolerates a file still being written: stops at the first incomplete member and drops the
    last partial line), so memory stays flat on the VPS"""
    with open(path, "rb") as f:
        d = zlib.decompressobj(31); rest = b""; data = b""
        while True:
            if not data:
                data = f.read(chunk)
                if not data: break
            try: txt = d.decompress(data)
            except zlib.error: return
            data = d.unused_data if d.eof else b""
            if d.eof: d = zlib.decompressobj(31)
            parts = (rest + txt).split(b"\n"); rest = parts.pop()
            for ln in parts: yield ln

USED = {b"bf", b"bk", b"mk", b"ptb", b"rs"}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("file"); ap.add_argument("--port", type=int, default=47012)
    ap.add_argument("--until-ms", type=int); ap.add_argument("--rate", type=float, default=8000.0); ap.add_argument("--only-used", action="store_true")
    a = ap.parse_args()
    if a.port == 47011: sys.exit("47011 is the live feed; replay to another port")
    lines = read_lines(a.file)
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    n, t0, last_t = 0, time.time(), None
    for ln in lines:
        c = ln.find(b",")
        try: t = int(ln[:c])
        except ValueError: continue
        if a.until_ms is not None and t > a.until_ms: break
        if a.only_used and ln[c + 1: c + 4].rstrip(b",") not in USED: continue
        sk.sendto(ln, ("127.0.0.1", a.port)); n += 1; last_t = t
        if n % 200 == 0:
            ahead = n / a.rate - (time.time() - t0)
            if ahead > 0: time.sleep(ahead)
    time.sleep(1.0); sk.sendto(b"0,__end__", ("127.0.0.1", a.port))
    print("sent %d lines, last t %s, %.0f s" % (n, last_t, time.time() - t0))

if __name__ == "__main__":
    main()
