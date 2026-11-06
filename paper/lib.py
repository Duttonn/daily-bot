"""Shared helpers: load columnar recorder extract (cols/*.f8), build rounds, ticks, vol grid, fee, normal cdf."""
import array, os, math, bisect, datetime, statistics, random, json

# Copy of agents/backtest/lib.py (unchanged except these two lines): columns come from the VPS extract, folder chosen by env RR_COLS.
B = "/private/tmp/claude-501/-Users-ndn18-PersonalProjects-daily-bot/bccc1ff4-9f68-4122-879b-2eab53fc03c5/scratchpad/agents/rec-replay/"
COLDIR = os.environ.get("RR_COLS", B + "cols_ws200/").rstrip("/") + "/"
NAN = float("nan")

def load_cols():
    cols = {}
    for fn in os.listdir(COLDIR):
        if fn.endswith(".f8"):
            a = array.array("d")
            with open(COLDIR + fn, "rb") as f:
                a.frombytes(f.read())
            cols[fn[:-3]] = a
    return cols

def ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def nppf(p):
    # inverse normal cdf (Acklam), adequate for p in (1e-6, 1-1e-6)
    p = min(max(p, 1e-9), 1 - 1e-9)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > 1 - pl:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)

def fee(p, bps=700.0):
    """Daily.fun taker fee per share, dollars: bps/1e4 * p * (1-p). Verified on 41 real fills (max abs diff 3.7e-5 on the trade)."""
    return bps / 1e4 * p * (1.0 - p)

def day_of(t):
    return datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")

def hms(t):
    return datetime.datetime.utcfromtimestamp(t).strftime("%m-%d %H:%M:%S")

class Data:
    """Row-level arrays + per-round index + deduplicated tick price series."""
    def __init__(self):
        c = self.c = load_cols()
        self.n = len(c["t"])
        n = self.n
        # round index
        self.rounds = {}  # rstart -> list of row idx (in time order)
        rs = c["rstart"]
        for i in range(n):
            self.rounds.setdefault(int(rs[i]), []).append(i)
        # price series by unique tick
        tid = c["tickid"]
        self.tk_t, self.tk_cb, self.tk_bn, self.tk_bf = [], [], [], []
        last = -1
        for i in range(n):
            if tid[i] != last:
                last = tid[i]
                self.tk_t.append(c["t"][i]); self.tk_cb.append(c["cb"][i]); self.tk_bn.append(c["bn"][i]); self.tk_bf.append(c["bf"][i])
        self.t0 = self.tk_t[0]
        self.t1 = self.tk_t[-1]

    def series(self, name):
        return {"cb": self.tk_cb, "bn": self.tk_bn, "bf": self.tk_bf}[name]

def grid_series(tk_t, px, step=0.5):
    """Carry-forward 500ms grid like the bot's calculateRealizedVol. Returns (g0, grid list, ok mask of valid price)."""
    g0 = tk_t[0]
    ng = int((tk_t[-1] - g0) / step) + 1
    grid = [NAN] * ng
    j = 0
    n = len(tk_t)
    last = NAN
    for k in range(ng):
        tg = g0 + k * step
        while j < n and tk_t[j] <= tg:
            if not math.isnan(px[j]):
                last = px[j]
            j += 1
        grid[k] = last
    return g0, grid

def vol_prefix(grid):
    """prefix sums of squared grid diffs, nan-safe (diff to nan treated as 0)."""
    pre = [0.0] * (len(grid) + 1)
    s = 0.0
    for i in range(1, len(grid)):
        a, b = grid[i], grid[i - 1]
        if not (math.isnan(a) or math.isnan(b)):
            s += (a - b) ** 2
        pre[i + 1] = s
    return pre

def sigma_at(t, g0, pre, step=0.5, win=600.0, min_span=300.0, ts_first=None):
    """Realised vol in $/sqrt(s) at time t, bot definition: sqrt(sum dp^2 / dtSec) over <=600s window of 500ms grid; None during warmup (<300s)."""
    if ts_first is not None and t - ts_first < min_span:
        return None
    k_now = int((t - g0) / step)
    k_start = max(0, k_now - int(win / step))
    dt = (k_now - k_start) * step
    if dt <= 0:
        return None
    s = pre[k_now + 1] - pre[k_start + 1]
    return math.sqrt(max(s, 0.0) / dt)

def percentile(a, p):
    a = sorted(a)
    if not a:
        return NAN
    k = (len(a) - 1) * p
    f = int(math.floor(k)); c = min(f + 1, len(a) - 1)
    return a[f] + (a[c] - a[f]) * (k - f)

def bootstrap_ci(vals_pairs, nboot=5000, seed=1, stat=None):
    """vals_pairs = list of (pnl, cost) per trade; ROI = sum pnl / sum cost. Resample trades with replacement. Returns (lo, hi, mean)."""
    rnd = random.Random(seed)
    n = len(vals_pairs)
    if n == 0:
        return (NAN, NAN, NAN)
    rois = []
    for _ in range(nboot):
        sp = sc = 0.0
        for _ in range(n):
            p, c = vals_pairs[rnd.randrange(n)]
            sp += p; sc += c
        rois.append(sp / sc if sc > 0 else 0.0)
    rois.sort()
    return (rois[int(0.025 * nboot)], rois[int(0.975 * nboot) - 1], sum(rois) / nboot)

def block_bootstrap_ci(day_blocks, nboot=5000, seed=1):
    """cluster bootstrap by day: day_blocks = list of lists of (pnl,cost). ROI CI."""
    rnd = random.Random(seed)
    nb = len(day_blocks)
    rois = []
    for _ in range(nboot):
        sp = sc = 0.0
        for _ in range(nb):
            blk = day_blocks[rnd.randrange(nb)]
            for p, c in blk:
                sp += p; sc += c
        rois.append(sp / sc if sc > 0 else 0.0)
    rois.sort()
    return (rois[int(0.025 * nboot)], rois[int(0.975 * nboot) - 1])
