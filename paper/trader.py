"""LightGBM paper trader (O12). Paper only: no venue connection, no keys, no orders. Its one input is the recorder's UDP line feed.

From the feed it rebuilds, tick by tick, what the offline pipeline builds from the recorder file:
  rows        the ws200 extract (rec-replay extract_vps.py): one row per 200 ms tick and market in -5 <= secondsToEnd <= 305, as of the tick
  rounds      m7engine.Prep: rows rs <= t <= rs + 300, f0 = perp mid at the tick <= rs (<= 3 s old), engine sigma on the 0.5 s grid
  features    entry-ml b3v_vps.build_round, up side, one decision per whole second g = 0..290 (the grid the policy was chosen on)
  model       LGB-resid on bot inputs: p_up = sigmoid(logit(q) + sum of the 14 trees), evaluated from model.txt
  policy      v3b_sim.sim_round (copied unchanged), model pool without stop = the model pool of V-indep, stress (1)+(2), 5 % cap on a
              compounding paper pool. The bot is not simulated: its real orders are already in the recorded book.
The simulator is re-run on the rows seen so far once per decision second; it is causal, so each run extends the previous one.
Settlement: the recorder's rs line (payoutUp, the venue's verdict); Pyth (next strike >= this strike) if no rs line 60 s after the close.

  live:   nice -n 10 python3 trader.py --port 47011 --out ~/lgbm-paper
  replay: python3 trader.py --port 47012 --out DIR --replay --trade-from RS0 --trade-to RS1   (fed by replay.py; ends on its end marker)
  leg:    nice -n 10 python3 trader.py --port 47014 --out ~/lgbm-leg --orders-port 47012 --fills-port 47013
Leg mode (real money, executed by daily-s6-bot.mjs with LGB_PORT): every model order is sent as JSON to the bot on --orders-port as
the simulator sends it; the bot sizes it against the leg's own ledger, sends it to the venue and answers on --fills-port with the
venue's fill (shares, cost incl fee or proceeds net of fee, fee). From then on the simulator uses that answer instead of its book walk,
so its holdings, its later decisions and its PnL follow the real fills. Orders more than 2 s old by the feed clock are not sent."""
import sys, os, json, math, bisect, socket, time, hashlib, argparse, resource, statistics, collections
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import lib, m7engine                      # copies of rec-replay lib.py / m7engine.py (fee, ncdf, max_price_for_edge, Prep.ladder)
def _no(*a, **k): raise RuntimeError("fill_mode real is not used by the paper trader")
m7engine._real_row = _no; m7engine._rtt = _no   # names v3b_sim imports from the backtest engine; only called in fill_mode real
import v3b_sim                            # copy of entry-ml v3b_sim.py (+ v3_sim.py), unchanged
from lib import ncdf
NAN = float("nan"); ND = statistics.NormalDist()
KEEP = {"bf", "mk", "ptb", "bk", "rs"}
SIDE_COLS = ["bid", "bsz", "ask", "asz", "a2", "a2s", "a3", "a3s", "b2", "b2s", "b3", "b3s", "nasks", "nbids", "seq", "stale"]
RCOLS = ["t", "bf"] + [p + c for p in ("u_", "d_") for c in SIDE_COLS]
VAR = {"solo_nostop": dict(model_pool="model", bot_pool=None, stop_pools=set())}

def sha(path): return hashlib.sha256(open(path, "rb").read()).hexdigest()

class Model:
    """LightGBM text model, numerical splits only (LightGBM NumericalDecision: NaN -> 0 unless missing type NaN; default direction for
    missing values from decision_type bit 1). raw(x) = sum of leaf values in tree order, as LightGBM's predict(raw_score=True)."""
    def __init__(self, path):
        self.names, self.trees, cur = None, [], None
        for ln in open(path).read().split("\n"):
            if ln.startswith("end of trees"): cur = None; break
            if ln.startswith("feature_names=") and cur is None: self.names = ln.split("=", 1)[1].split()
            elif ln.startswith("Tree="): cur = {}; self.trees.append(cur)
            elif cur is not None and "=" in ln:
                k, v = ln.split("=", 1)
                if k in ("split_feature", "decision_type", "left_child", "right_child"): cur[k] = [int(x) for x in v.split()]
                elif k in ("threshold", "leaf_value"): cur[k] = [float(x) for x in v.split()]
                elif k == "num_leaves": cur[k] = int(v)
                elif k == "num_cat": assert int(v) == 0, "categorical splits not supported"
    def raw(self, x):
        s = 0.0
        for tr in self.trees:
            if tr["num_leaves"] == 1: s += tr["leaf_value"][0]; continue
            node = 0
            while node >= 0:
                f = x[tr["split_feature"][node]]; dt = tr["decision_type"][node]; mt = (dt >> 2) & 3
                if f != f and mt != 2: f = 0.0
                if (mt == 1 and -1e-35 <= f <= 1e-35) or (mt == 2 and f != f):
                    node = tr["left_child"][node] if dt & 2 else tr["right_child"][node]
                else:
                    node = tr["left_child"][node] if f <= tr["threshold"][node] else tr["right_child"][node]
            s += tr["leaf_value"][~node]
        return s

def lvl(L, i): return (float(L[i][0]), float(L[i][1])) if len(L) > i else (NAN, NAN)

class RoundP:
    """The slice of m7engine.Prep that v3b_sim reads, for one round: c (columns, local row index), rounds[rs], ladder()."""
    def __init__(self, c, R): self.c, self.rounds = c, {R["rs"]: R}
    def ladder(self, i, side, kind):   # copy of m7engine.Prep.ladder
        c = self.c
        s = "u" if side == "up" else "d"
        if kind == "ask":
            L = [(c[s + "_ask"][i], c[s + "_asz"][i]), (c[s + "_a2"][i], c[s + "_a2s"][i]), (c[s + "_a3"][i], c[s + "_a3s"][i])]
        else:
            L = [(c[s + "_bid"][i], c[s + "_bsz"][i]), (c[s + "_b2"][i], c[s + "_b2s"][i]), (c[s + "_b3"][i], c[s + "_b3s"][i])]
        return [(p, z) for p, z in L if not (math.isnan(p) or math.isnan(z)) and z > 0]

class Round:
    def __init__(self, slug, m):
        self.slug, self.rs, self.end_ms = slug, m["start"] // 1000, m["end"]
        self.c = {k: [] for k in RCOLS}; self.sig = []; self.mid = []
        self.f0 = NAN; self.first_t = None; self.dec_t, self.dec_k, self.dec_p = [], [], []; self.g_next = 0
        self.eligible = None; self.cap = None; self.log = []; self.nproc = 0; self.complete = False; self.settled = False

class Trader:
    def __init__(self, a):
        self.a = a; self.pol = json.load(open(os.path.join(HERE, "policy.json")))
        assert sha(os.path.join(HERE, self.pol["model"])) == self.pol["model_sha256"], "model file does not match policy.json"
        self.model = Model(os.path.join(HERE, self.pol["model"]))
        assert self.model.names == self.pol["feats"], "model feature names differ from policy.json"
        self.cfg, self.prm, self.stress = self.pol["cfg"], self.pol["prm"], self.pol["stress"]
        self.var = VAR[self.pol["variant"]]
        os.makedirs(a.out, exist_ok=True)
        self.fj = open(os.path.join(a.out, "paper.jsonl"), "a", buffering=1)
        self.fl = open(os.path.join(a.out, "paper.log"), "a", buffering=1)
        self.fd = open(os.path.join(a.out, "decisions.csv"), "a", buffering=1)
        if self.fd.tell() == 0: self.fd.write("rs,g,t,p,q,raw" + ("," + ",".join(self.pol["feats"]) if a.dump_features else "") + "\n")
        self.markets, self.mk_sorted, self.ptb, self.rsl, self.book, self.seqc = {}, [], {}, {}, {}, collections.Counter()
        self.bf, self.T, self.tmax, self.buf = None, None, None, collections.deque()
        self.tk_t, self.tk_bf = [], []
        self.g0 = self.t_first = None; self.glast = NAN; self.gprev = NAN; self.gk = 0; self.gs = 0.0; self.pre = [0.0]; self.pre_base = 0
        self.rounds = {}; self.pool = self.pol["pool0"]; self.cnt = collections.Counter(); self.late = 0; self.last_hb = time.time()
        self.n_settled = 0; self.last_trim = 0.0; self.ntot = 0; self.by_start = {}
        self.leg = bool(a.orders_port); self.fills, self.sent, self.pending = {}, set(), {}
        if self.leg:
            self.osk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.fsk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); self.fsk.bind(("127.0.0.1", a.fills_port)); self.fsk.setblocking(False)
        files =["trader.py", "policy.json", self.pol["model"], "v3b_sim.py", "v3_sim.py", "lib.py", "m7engine.py"]
        hdr = dict(ev="start", wall=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), mode="replay" if a.replay else "live", port=a.port,
                   leg=dict(orders_port=a.orders_port, fills_port=a.fills_port) if self.leg else None,
                   sha256={f: sha(os.path.join(HERE, f)) for f in files}, feats_sha256=hashlib.sha256(" ".join(self.pol["feats"]).encode()).hexdigest(),
                   cfg=self.cfg, variant=self.pol["variant"], pool0=self.pol["pool0"], cap_frac=self.pol["cap_frac"], warmup_s=self.pol["warmup_s"],
                   trade_from=a.trade_from, trade_to=a.trade_to, python=sys.version.split()[0])
        self.emit(hdr); self.fl.write("start %s\n" % json.dumps(hdr))

    # ---------------- output
    def emit(self, d): self.fj.write(json.dumps(d, separators=(",", ":")) + "\n")
    def pool_cash(self):
        x = self.pool
        for R in self.rounds.values():
            if not R.settled:
                for e in R.log:
                    if e["sh"]: x += (-1.0 if e["act"] in ("entry", "add", "flip_buy") else 1.0) * e["sh"] * e["px"]
        return x

    # ---------------- feed (vpslib.parse semantics, applied in time order)
    def line(self, ln):
        c = ln.find(",")
        if c < 0: return
        try: t = int(ln[:c])
        except ValueError: return
        src = ln[c + 1: c + 1 + ln[c + 1:].find(",")]
        self.cnt[src] += 1; self.ntot += 1
        if src not in KEEP:
            if self.tmax is None or t > self.tmax: self.tmax = t
            self.advance(); return
        if self.T is not None and t <= self.T: self.late += 1   # would have been in an already built tick (never seen on localhost)
        self.buf.append((t, src, ln))
        if self.tmax is None or t > self.tmax: self.tmax = t
        if src == "bf" and self.T is None:
            self.T = -(-t // 200) * 200 - 200            # first tick = ceil(first bf / 200 ms), as extract_vps (T0); T = last built
        self.advance()

    def apply(self, t, src, ln):
        if src == "bf":
            p = ln.split(","); self.bf = (float(p[3]) + float(p[4])) / 2
        elif src == "bk":
            p = ln.split(",", 9); slug, side = p[3], int(p[4])
            try: bids, asks = json.loads(p[9])
            except Exception: return
            self.seqc[(slug, side)] += 1
            self.book[(slug, side)] = ([tuple(x) for x in bids], [tuple(x) for x in asks], self.seqc[(slug, side)])
        elif src == "mk":
            p = ln.split(",", 8); slug = p[3]
            if slug not in self.markets:
                try: meta = json.loads(p[8])
                except Exception: meta = {}
                self.markets[slug] = {"pda": p[5], "start": int(p[6]), "end": int(p[7]), "fee": meta.get("fee", 700)}
                bisect.insort(self.mk_sorted, (int(p[6]), slug)); self.by_start[int(p[6])] = slug
        elif src == "ptb":
            p = ln.split(","); self.ptb[p[3]] = (t, float(p[4]))
        elif src == "rs":
            p = ln.split(","); self.rsl[p[3]] = (t, lib_fnum(p[4]), lib_fnum(p[5]), lib_fnum(p[6]), p[7])

    def advance(self):
        built = False
        while self.T is not None and self.tmax is not None and self.T + 200 < self.tmax:
            T = self.T + 200
            while self.buf and self.buf[0][0] <= T:
                self.apply(*self.buf.popleft())
            self.tick(T); self.T = T; built = True
        if built: self.settle_ready()
        if time.time() - self.last_hb >= 60: self.heartbeat()

    # ---------------- ticks, rows, rounds
    def tick(self, T):
        if self.bf is None: return
        t = T / 1000.0
        rows = []
        for _, slug in self.mk_sorted:
            m = self.markets[slug]; sec = (m["end"] - T) / 1000.0
            if not (-5.0 <= sec <= 305.0): continue
            r = {"t": t, "bf": self.bf}
            for side, pfx in ((0, "u_"), (1, "d_")):
                bids, asks, seq = self.book.get((slug, side), ([], [], NAN))
                r[pfx + "bid"], r[pfx + "bsz"] = lvl(bids, 0); r[pfx + "ask"], r[pfx + "asz"] = lvl(asks, 0)
                r[pfx + "a2"], r[pfx + "a2s"] = lvl(asks, 1); r[pfx + "a3"], r[pfx + "a3s"] = lvl(asks, 2)
                r[pfx + "b2"], r[pfx + "b2s"] = lvl(bids, 1); r[pfx + "b3"], r[pfx + "b3s"] = lvl(bids, 2)
                r[pfx + "nasks"], r[pfx + "nbids"] = float(len(asks)), float(len(bids)); r[pfx + "seq"] = float(seq); r[pfx + "stale"] = 0.0
            rows.append((slug, m, r))
        if not rows: return
        self.tk_t.append(t); self.tk_bf.append(self.bf); self.grid_add(t, self.bf)
        for slug, m, r in rows:
            R = self.rounds.get(m["start"] // 1000)
            if R is None: R = self.rounds[m["start"] // 1000] = Round(slug, m)
            rs = R.rs
            if t == rs:   # m7engine.Prep f0: last tick <= rs, <= 3 s old
                k = bisect.bisect_right(self.tk_t, rs) - 1
                if k >= 0 and rs - self.tk_t[k] <= 3.0 and not math.isnan(self.tk_bf[k]): R.f0 = self.tk_bf[k]
            if not (rs <= t <= rs + 300) or R.complete: continue
            if R.first_t is None: R.first_t = t
            for k_, v in r.items(): R.c[k_].append(v)
            R.sig.append(self.sigma(t)); R.mid.append((r["u_bid"] + r["u_ask"]) / 2.0)
            if R.eligible is None: R.eligible = self.is_eligible(R)
            if not R.eligible:
                if t >= rs + 300: R.complete = True
                continue
            g = R.g_next   # decision g is due once the tick rs + g is built
            while g <= self.pol["gmax"] and rs + g <= t:
                self.decide(R, g); g += 1
            R.g_next = g
            n_proc = bisect.bisect_left(R.dec_t, t)   # decisions with t_g < t: processed by the simulator at this row
            if n_proc > R.nproc: self.run_sim(R, n_proc); R.nproc = n_proc
            if t >= rs + 300: R.complete = True
        if t - self.last_trim > 600: self.trim(t)

    def is_eligible(self, R):
        if R.first_t != R.rs or math.isnan(R.f0): return False
        if R.rs < self.t_first + self.pol["warmup_s"]: return False
        if self.a.trade_from is not None and R.rs < self.a.trade_from: return False
        if self.a.trade_to is not None and R.rs >= self.a.trade_to: return False
        return True

    # ---------------- 0.5 s grid, engine sigma, realised vol (lib.grid_series / vol_prefix / sigma_at, b3v_vps.rvol), incremental
    def grid_push(self, v):
        if self.gk >= 1 and not (math.isnan(v) or math.isnan(self.gprev)): self.gs += (v - self.gprev) ** 2
        self.pre.append(self.gs); self.gprev = v; self.gk += 1
    def grid_add(self, t, px):
        if self.g0 is None: self.g0 = self.t_first = t
        while self.g0 + self.gk * 0.5 < t: self.grid_push(self.glast)
        if not math.isnan(px): self.glast = px
        while self.g0 + self.gk * 0.5 <= t: self.grid_push(self.glast)
    def pre_g(self, k):
        while k >= self.gk + 1: self.grid_push(self.glast)   # float edge: grid point a hair after the last tick, no later tick yet
        return self.pre[k - self.pre_base]
    def sigma(self, t):
        if t - self.t_first < 300.0: return None
        k_now = int((t - self.g0) / 0.5); k_start = max(0, k_now - int(600.0 / 0.5)); dt = (k_now - k_start) * 0.5
        if dt <= 0: return None
        s = self.pre_g(k_now + 1) - self.pre_g(k_start + 1)
        return math.sqrt(max(s, 0.0) / dt)
    def rvol(self, t, w):
        k_now = int((t - self.g0) / 0.5); k_st = k_now - int(w / 0.5)
        if k_st < 0: return NAN
        return math.sqrt(max(self.pre_g(k_now + 1) - self.pre_g(max(k_st, 0) + 1), 0.0) / w)
    def trim(self, t):
        self.last_trim = t
        j = bisect.bisect_left(self.tk_t, t - 7200.0)
        if j > 0: del self.tk_t[:j]; del self.tk_bf[:j]
        kk = int((t - 7300.0 - self.g0) / 0.5) - self.pre_base
        if kk > 0: del self.pre[:kk]; self.pre_base += kk
        for rs in [rs for rs, R in self.rounds.items() if t > rs + 400 and (R.settled or not R.eligible)]: del self.rounds[rs]

    # ---------------- features (b3v_vps.build_round, up side) and model
    def features(self, R, kk):
        c = R.c; t = c["t"][kk]; rs = R.rs; tau = rs + 300.0 - t
        sg = R.sig[kk]; sig = NAN if sg is None else sg
        se = max(sig, self.prm["sigma_floor"]); sq = math.sqrt(max(tau, 1.0))
        kt = bisect.bisect_right(self.tk_t, t) - 1; now = self.tk_bf[kt]
        F = {"tau": tau, "ds_bf": now - R.f0}
        for w in (2, 5, 15, 60):
            F["ret%d_bf" % w] = now - self.tk_bf[max(bisect.bisect_right(self.tk_t, t - w) - 1, 0)]
        F["sigma"] = sig; F["se"] = se
        v = {w: self.rvol(t, w) for w in (300, 900, 3600)}
        for w in (300, 900, 3600): F["vol%dm_bf" % (w // 60)] = v[w]
        F["vr5_60_bf"] = ndiv(v[300], v[3600]); F["vr15_60_bf"] = ndiv(v[900], v[3600])
        z = F["ds_bf"] / (se * sq); F["z"] = z
        q = ncdf(z) if not math.isnan(z) else NAN; F["q"] = q
        ask, bid, oask, obid = c["u_ask"][kk], c["u_bid"][kk], c["d_ask"][kk], c["d_bid"][kk]
        F.update(ask=ask, bid=bid, spread=ask - bid, oask=oask, obid=obid, ospread=oask - obid)
        F["edge"] = q - (ask + 0.07 * ask * (1 - ask)); F["ask_m_fair"] = ask - q
        F["overround_ask"] = ask + oask; F["overround_bid"] = bid + obid
        F["asz"], F["bsz"], F["oasz"], F["obsz"] = c["u_asz"][kk], c["u_bsz"][kk], c["d_asz"][kk], c["d_bsz"][kk]
        F["depth_ask"] = sum(x for x in (c["u_asz"][kk], c["u_a2s"][kk], c["u_a3s"][kk]) if not math.isnan(x))
        F["depth_bid"] = sum(x for x in (c["u_bsz"][kk], c["u_b2s"][kk], c["u_b3s"][kk]) if not math.isnan(x))
        F["ask2_m_ask"] = c["u_a2"][kk] - ask
        F["nasks"], F["nbids"] = c["u_nasks"][kk], c["u_nbids"][kk]
        tt = c["t"]; msd = R.mid
        for w in (2, 5, 15):
            F["bret%d" % w] = msd[kk] - msd[max(bisect.bisect_right(tt, t - w) - 1, 0)]
        mk = msd[kk]
        F["impl_d"] = (ND.inv_cdf(min(max(mk, 0.02), 0.98)) * se * sq) if not math.isnan(mk) else NAN
        F["lag_bf"] = F["ds_bf"] - F["impl_d"]
        hour = (t % 86400.0) / 3600.0
        F["hour_sin"] = math.sin(2 * math.pi * hour / 24); F["hour_cos"] = math.cos(2 * math.pi * hour / 24)
        return F

    def decide(self, R, g):
        kk = bisect.bisect_right(R.c["t"], R.rs + g) - 1
        if kk < 0: return
        F = self.features(R, kk)
        x = [F[f] for f in self.pol["feats"]]
        raw = self.model.raw(x)
        q = F["q"]; qq = min(max(0.5 if math.isnan(q) else q, 1e-4), 1 - 1e-4)
        p = 1.0 / (1.0 + math.exp(-(math.log(qq / (1 - qq)) + raw)))
        p = min(max(p, 1e-4), 1 - 1e-4)
        R.dec_t.append(R.c["t"][kk]); R.dec_k.append(kk); R.dec_p.append(p)
        self.fd.write("%d,%d,%.1f,%.17g,%.17g,%.17g%s\n" % (R.rs, g, R.c["t"][kk], p, q, raw, ("," + ",".join("%.17g" % v for v in x)) if self.a.dump_features else ""))

    # ---------------- simulator (v3b_sim.sim_round, unchanged) on the rows seen so far
    def sim(self, R, n_proc, up=False):
        n = len(R.c["t"])
        Rd = {"rs": R.rs, "rows": list(range(n)), "t": R.c["t"], "p": R.c["bf"], "sig": R.sig, "f0": R.f0, "up": up}
        log = []
        cap = R.cap if R.cap is not None else self.pol["cap_frac"] * max(self.pool, 0.0)
        res = v3b_sim.sim_round(RoundP(R.c, Rd), R.rs, self.prm, self.var, model_dec=(R.dec_t[:n_proc], R.dec_k[:n_proc], R.dec_p[:n_proc]),
                                cfg=self.cfg, liq=True, log=log, stress=self.stress, round_cap=cap,
                                on_send=(lambda o: self.order(R, o)) if self.leg else None, fills=self.fills if self.leg else None)
        return res, log, cap

    # ---------------- leg mode: orders out to the bot, the venue's fills back
    def order(self, R, o):
        k = v3b_sim.okey(R.rs, o)
        if k in self.sent or k in self.fills: return
        self.sent.add(k)
        # not sent, and a miss for the simulator: flip (off in the policy), an order decided on an old book, or one decided before the
        # venue answered the previous order (its holdings and its cap room were then the simulator's guess, not the venue's fill)
        now = self.tmax / 1000.0
        for k2 in [k2 for k2, t2 in self.pending.items() if now - t2 > 20.0]:   # the bot answers within 15 s or not at all: count it as unfilled
            del self.pending[k2]; self.fills.setdefault(k2, (0, 0.0, 0.0)); self.emit(dict(ev="venue", k=k2, sh=0, st="no answer"))
        why = "flip" if o["act"] == "flip" else "late" if now - o["t"] > 1.0 else "previous unanswered" if self.pending else None
        if why:
            self.fills[k] = (0, 0.0, 0.0); self.emit(dict(ev="order", k=k, sent=False, why=why)); return
        self.pending[k] = now
        d = dict(k=k, slug=R.slug, act=o["act"], side=o["side"], limit=round(o["limit"], 6), t=o["t"], p=o.get("p"))
        if o["kind"] == "buy": d["target"] = int(o["target"])
        else: d["shares"] = float(o["shares"])
        self.osk.sendto(json.dumps(d).encode(), ("127.0.0.1", self.a.orders_port))
        self.emit(dict(ev="order", sent=True, **d))
    def drain_fills(self):
        while self.leg:
            try: data = self.fsk.recv(65535)
            except (BlockingIOError, InterruptedError): return
            try: f = json.loads(data)
            except ValueError: continue
            if not isinstance(f, dict) or "k" not in f: continue
            self.fills[f["k"]] = (float(f.get("sh", 0)), float(f.get("cost", 0)), float(f.get("fe", 0))); self.pending.pop(f["k"], None)
            self.emit(dict(ev="venue", **f))

    def run_sim(self, R, n_proc):
        res, log, cap = self.sim(R, n_proc)
        old = R.log
        if self.leg:   # a venue answer rewrites the simulated fill it replaces: the log is final only at settlement
            if log and R.cap is None: R.cap = cap; self.emit(dict(ev="round", rs=R.rs, slug=R.slug, cap=cap, pool=self.pool))
            R.log = log; return
        if log[:len(old)] != old: self.emit(dict(ev="error", rs=R.rs, msg="simulator prefix changed", n_old=len(old), n_new=len(log)))
        if log and R.cap is None:
            R.cap = cap; self.emit(dict(ev="round", rs=R.rs, slug=R.slug, cap=cap, pool=self.pool))
        R.log = log
        for e in log[len(old):]:
            d = dict(ev="fill" if e["sh"] else "miss", rs=R.rs, t=e["t"], tau=e["tau"], act=e["act"], side=e["side"], p=e["p"], sh=e["sh"], px=e["px"])
            d["pool_settled"] = self.pool; d["pool_cash"] = self.pool_cash()
            self.emit(d)

    # ---------------- settlement
    def settle_ready(self):
        if self.tmax is None: return
        for rs, R in list(self.rounds.items()):
            if R.settled or not R.complete or not R.eligible: continue
            up, src = None, None
            v = self.rsl.get(R.slug)
            if v is not None and not math.isnan(v[1]): up, src = v[1] > 0.5, "venue"
            nxt = self.by_start.get(R.end_ms)
            py = (self.ptb[nxt][1] >= self.ptb[R.slug][1]) if (nxt in self.ptb and R.slug in self.ptb) else None
            if up is None and self.tmax >= R.end_ms + 1000 * self.pol["settle_fallback_s"] and py is not None: up, src = py, "pyth"
            if up is None: continue
            R.settled = True
            if not R.log:   # no order in this round
                self.n_settled += 1; continue
            res, log, cap = self.sim(R, len(R.dec_t), up=up)
            if self.leg:
                for e in log: self.emit(dict(ev="fill" if e["sh"] else "miss", rs=R.rs, t=e["t"], tau=e["tau"], act=e["act"], side=e["side"], p=e["p"], sh=e["sh"], px=e["px"]))
            elif log != R.log: self.emit(dict(ev="error", rs=rs, msg="final run differs from the incremental runs"))
            self.pool += res["pnl"]; self.n_settled += 1
            self.emit(dict(ev="settle", rs=rs, slug=R.slug, up=up, src=src, pyth_up=py, pnl=res["pnl"], spent=res["mspent"], cap=R.cap,
                           acts={k: v for k, v in res["acts"].items() if v}, pool_settled=self.pool, pool_cash=self.pool_cash()))

    # ---------------- heartbeat
    def heartbeat(self):
        self.last_hb = time.time()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        rss = ru.ru_maxrss / (1024.0 if sys.platform.startswith("linux") else 1024.0 * 1024.0)
        open_r = [R for R in self.rounds.values() if R.eligible and not R.settled]
        s = "hb %s lag_s %s ticks_T %s lines %s late %d rounds_open %d settled %d pool %.2f cash %.2f rss_mb %.0f cpu_s %.0f" % (
            time.strftime("%H:%M:%S", time.gmtime()), ("%.1f" % (time.time() - self.tmax / 1000.0)) if self.tmax else "-",
            time.strftime("%H:%M:%S", time.gmtime(self.T / 1000.0)) if self.T else "-", dict(self.cnt), self.late, len(open_r), self.n_settled,
            self.pool, self.pool_cash(), rss, ru.ru_utime + ru.ru_stime)
        self.fl.write(s + "\n"); self.cnt.clear()
        if rss > 280:
            self.fl.write("rss above 280 MB, exiting\n"); self.emit(dict(ev="stop", reason="rss %.0f MB" % rss)); sys.exit(3)

def ndiv(a, b):
    """a / b with numpy float semantics (x / 0 -> +-inf, 0 / 0 -> nan, nan propagates); b is a vol, never negative"""
    if b == 0.0: return NAN if (a == 0.0 or a != a) else math.copysign(math.inf, a)
    return a / b

def lib_fnum(x):
    try: return float(x) if x != "" else NAN
    except ValueError: return NAN

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=47011); ap.add_argument("--out", default=os.path.expanduser("~/lgbm-paper"))
    ap.add_argument("--replay", action="store_true"); ap.add_argument("--trade-from", type=int); ap.add_argument("--trade-to", type=int)
    ap.add_argument("--dump-features", action="store_true")
    ap.add_argument("--orders-port", type=int, default=0); ap.add_argument("--fills-port", type=int, default=0)
    a = ap.parse_args()
    if bool(a.orders_port) != bool(a.fills_port): sys.exit("leg mode needs both --orders-port and --fills-port")
    tr = Trader(a)
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try: sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
    except OSError: pass
    sk.bind(("127.0.0.1", a.port)); sk.settimeout(5.0)
    n = 0
    while True:
        tr.drain_fills()
        try: data = sk.recv(65535)
        except socket.timeout:
            if time.time() - tr.last_hb >= 60: tr.heartbeat()
            continue
        n += 1
        for ln in data.decode("utf-8", "replace").split("\n"):
            ln = ln.rstrip("\r")
            if not ln: continue
            if a.replay and ln == "0,__end__":
                tr.settle_ready()
                tr.emit(dict(ev="end", datagrams=n - 1, lines=tr.ntot, late=tr.late, pool_settled=tr.pool,
                             unsettled=[R.rs for R in tr.rounds.values() if R.eligible and R.log and not R.settled]))
                tr.heartbeat(); return
            tr.line(ln)

if __name__ == "__main__":
    main()
