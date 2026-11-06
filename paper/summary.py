"""Summary of a paper trader run: python3 summary.py [~/lgbm-paper/paper.jsonl]
PnL, rounds traded, win rate, buys / sells, lowest pool and worst peak-to-trough drop on the settled pool path, by hour (UTC, round start)."""
import sys, os, json, time, collections
path = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/lgbm-paper/paper.jsonl")
ev = [json.loads(l) for l in open(path) if l.strip()]
starts = [e for e in ev if e.get("ev") == "start"]
pool0 = starts[-1]["pool0"] if starts else 100.0
if len(starts) > 1:   # several runs appended to one file: report the last one
    i = max(i for i, e in enumerate(ev) if e.get("ev") == "start"); ev = ev[i:]
st = [e for e in ev if e.get("ev") == "settle"]
fills = [e for e in ev if e.get("ev") == "fill"]
pool = pool0; low = pool0; peak = pool0; worst = 0.0
for e in st:
    pool = e["pool_settled"]; low = min(low, pool); peak = max(peak, pool); worst = max(worst, (peak - pool) / peak if peak > 0 else 0.0)
pnl = sum(e["pnl"] for e in st); wins = sum(1 for e in st if e["pnl"] > 0)
acts = collections.Counter(e["act"] for e in fills)
print("run started %s (%s), model %s..., cfg %s" % (starts[-1]["wall"] if starts else "?", starts[-1]["mode"] if starts else "?",
      starts[-1]["sha256"]["model.txt"][:12] if starts else "?", starts[-1]["cfg"] if starts else "?"))
print("PnL %+.2f USD | rounds traded %d, won %d (%s) | buys %d (entry %d, add %d), sells %d, misses %d" % (
    pnl, len(st), wins, ("%.0f%%" % (100.0 * wins / len(st))) if st else "-", acts["entry"] + acts["add"], acts["entry"], acts["add"], sum(v for k, v in acts.items() if k.endswith("_sell")),
    sum(1 for e in ev if e.get("ev") == "miss")))
print("pool start %.2f now %.2f | lowest %.2f (%.0f%% of start) | worst peak-to-trough drop %.1f%% | outcome venue/pyth %d/%d, pyth disagreed %d" % (
    pool0, pool, low, 100 * low / pool0, 100 * worst, sum(e["src"] == "venue" for e in st), sum(e["src"] == "pyth" for e in st),
    sum(1 for e in st if e.get("pyth_up") is not None and e["pyth_up"] != e["up"])))
errs = [e for e in ev if e.get("ev") in ("error", "stop")]
if errs: print("errors / stops: %d, last %s" % (len(errs), errs[-1]))
by = collections.OrderedDict()
for e in st:
    h = time.strftime("%m-%d %H", time.gmtime(e["rs"])); x = by.setdefault(h, [0, 0, 0.0]); x[0] += 1; x[1] += e["pnl"] > 0; x[2] += e["pnl"]
print("hour UTC   rounds  won   PnL USD")
for h, (n, w, p) in by.items(): print("%s h  %6d %4d  %+8.2f" % (h, n, w, p))
