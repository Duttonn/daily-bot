"""Calibrated-M7 backtest engine on the recorder extract. Faithful to daily-s6-bot.mjs (evaluateStrategy, executeTrade, executeCashOut)
but fills/exits are taken from the recorded book after a latency, fees follow the venue formula, settlement = Pyth strike of the next round."""
import sys
sys.path.insert(0, "/private/tmp/claude-501/-Users-ndn18-PersonalProjects-daily-bot/bccc1ff4-9f68-4122-879b-2eab53fc03c5/scratchpad/agents/rec-replay")  # copy of agents/backtest/m7engine.py, only this path changed
import math, bisect, random, statistics
from lib import *

def max_price_for_edge(q, min_edge=0.025, bps=700.0):
    """bot findMaxPriceForEdge"""
    target = q - min_edge
    if target <= 0.05: return 0.05
    k = bps / 1e4
    a, b, cc = k, -(1 + k), target
    disc = b * b - 4 * a * cc
    if disc < 0: return min(0.95, target)
    p = (-b - math.sqrt(disc)) / (2 * a)
    return min(0.95, max(0.05, p))

class Prep:
    """Per price-source precomputation: per round row lists with time, price, sigma, book ladders."""
    def __init__(self, D, src="bf", partial_last=False):   # rec-replay: partial_last keeps the newest round (next strike unknown, outcome placeholder) for live-parity checks only
        self.D, self.src = D, src
        c = D.c
        px = D.series(src)
        g0, grid = grid_series(D.tk_t, px)
        pre = vol_prefix(grid)
        self.rounds = {}
        starts = sorted(D.rounds)
        # outcome from next round's locked ptb
        ptb = {}
        for rs in starts:
            for i in D.rounds[rs]:
                if c["stc"][i] == 1.0 and not math.isnan(c["ptb"][i]):
                    ptb[rs] = c["ptb"][i]; break
        self.ptb = ptb
        tk_t = D.tk_t
        t_first = tk_t[0]
        for rs in starts:
            if rs not in ptb or ((rs + 300) not in ptb and not partial_last):
                continue
            idx = D.rounds[rs]
            # f0: last valid price tick at or before rs (<=3s old), bot getPriceAt
            k = bisect.bisect_right(tk_t, rs) - 1
            f0 = NAN
            if k >= 0 and rs - tk_t[k] <= 3.0 and not math.isnan(px[k]):
                f0 = px[k]
            rows = [i for i in idx if rs <= c["t"][i] <= rs + 300 and not math.isnan(c[src][i])]
            if not rows or math.isnan(f0):
                continue
            tt = [c["t"][i] for i in rows]
            pp = [c[src][i] for i in rows]
            sg = [sigma_at(t, g0, pre, ts_first=t_first) for t in tt]
            cc = D.c
            # rec-replay: Pyth-anchored displacement per row = (last Pyth tick - strike) + (perp now - perp at the time of that tick); needs the py/bf_at_py columns
            dd = [(cc["py"][i] - ptb[rs]) + (cc[src][i] - cc["bf_at_py"][i]) if ("py" in cc and not math.isnan(cc["py"][i]) and not math.isnan(cc["bf_at_py"][i])) else NAN for i in rows] if "py" in cc else [NAN] * len(rows)
            self.rounds[rs] = {"dd": dd,
                "rs": rs, "rows": rows, "t": tt, "p": pp, "sig": sg, "f0": f0, "ptb": ptb[rs], "ptb_next": ptb.get(rs + 300, ptb[rs]),
                "up": ptb.get(rs + 300, ptb[rs]) >= ptb[rs], "d_settle": ptb.get(rs + 300, ptb[rs]) - ptb[rs], "open_end": (rs + 300) not in ptb,
            }
        self.c = c
        self.starts = sorted(self.rounds)

    def ladder(self, i, side, kind):
        c = self.c
        s = "u" if side == "up" else "d"
        if kind == "ask":
            L = [(c[s + "_ask"][i], c[s + "_asz"][i]), (c[s + "_a2"][i], c[s + "_a2s"][i]), (c[s + "_a3"][i], c[s + "_a3s"][i])]
        else:
            L = [(c[s + "_bid"][i], c[s + "_bsz"][i]), (c[s + "_b2"][i], c[s + "_b2s"][i]), (c[s + "_b3"][i], c[s + "_b3s"][i])]
        return [(p, z) for p, z in L if not (math.isnan(p) or math.isnan(z)) and z > 0]

DEFAULT = dict(tau_min=50.0, tau_max=130.0, zmin=0.70, min_edge=0.10, max_ask=0.72, sigma_floor=2.20, calm=4.5, cmodel=1.0,
               stop=True, stop_q=0.30, stop_d=-5.0, stop_tau_min=30.0, stop_tau_max=90.0, stop_retry=False,
               lat_buy=0.70, lat_sell=0.90, slip=0.02, stake=21.40, fee_mode="venue", sell_fee=True, mode="normal", rng=None,
               use_floor=True, fixed_sigma=None, book_age_max=3000.0, require_fresh=True, persist=1,
               stop_gap=1.5, stop_off=None,
               buy_mode="slip", buy_off=0.02, buy_retry=0, buy_size_live=False, one_shot=False, use_dd=False, stop_k=None, stop_sig_t=30.0, o6=None)  # rec-replay: stop_k = volatility-scaled d trigger -k*sigmaEff*sqrt(stop_sig_t) (None = fixed stop_d); o6 = (sigmaM threshold, tau cap): no entry earlier than tau cap when sigmaM >= threshold  # rec-replay: buy limit rule (slip = old L1+slip capped at 2.5 % edge, live = bot rule, plus, edge), its offset, number of retries after a rejected buy  # rec-replay additions: gap between stop attempts (s); first-attempt limit offset below the bid at trigger (None = slip)

def _gate(P, R, k, tau, prm):
    c = P.c
    s = R["sig"][k]
    if s is None or s >= prm["calm"]: return None
    if prm["fixed_sigma"] is not None:
        se = prm["fixed_sigma"]
    else:
        se = max(s, prm["sigma_floor"]) if prm["use_floor"] else s
    d = R["dd"][k] if (prm["use_dd"] in ("both", "entry") and not math.isnan(R["dd"][k])) else R["p"][k] - R["f0"]
    z = abs(d) / (prm["cmodel"] * se * math.sqrt(max(tau, 1.0)))
    if z < prm["zmin"]: return None
    side = "up" if d > 0 else "down"
    i = R["rows"][k]
    su = "u" if side == "up" else "d"
    ask, asz = c[su + "_ask"][i], c[su + "_asz"][i]
    oask = c["d_ask" if side == "up" else "u_ask"][i]
    if math.isnan(ask) or math.isnan(asz) or math.isnan(oask): return None
    if prm["require_fresh"]:
        if c["u_stale"][i] == 1.0 or c["d_stale"][i] == 1.0: return None
        if c["u_age"][i] > prm["book_age_max"] or c["d_age"][i] > prm["book_age_max"]: return None
    if asz < 1 or ask > prm["max_ask"]: return None
    q = ncdf(z)
    fe = fee(ask) if prm["fee_mode"] == "venue" else (0.0 if prm["fee_mode"] == "none" else 0.01 * q)
    if q - (ask + fe) >= prm["min_edge"]:
        return (k, side, d, z, q, ask)
    return None

STATS = {"rej": 0, "retry": 0, "retry_fill": 0}   # rec-replay: rejected buys / retries taken / retries that filled, of the last run()

def run(P, **kw):
    """Run one parameter set across P.starts (optionally a subset via kw['rounds']). Returns list of trade dicts and miss count."""
    prm = dict(DEFAULT); prm.update(kw)
    rounds = prm.pop("rounds", None) or P.starts
    c = P.c
    trades, misses, signals = [], 0, 0
    STATS.update(rej=0, retry=0, retry_fill=0, events=[])
    rnd = prm["rng"]
    for rs in rounds:
        R = P.rounds[rs]
        tt, pp, sg, rows = R["t"], R["p"], R["sig"], R["rows"]
        f0 = R["f0"]
        rend = rs + 300
        n = len(tt)
        # window rows
        trig = None
        if prm["one_shot"] and prm["mode"] == "normal":
            # rec-replay: original S6 (a514957). Per tick of the window: sigmaM >= calm -> return; |d| < 1 or a missing/stale/old book -> try the next tick; otherwise the round is
            # marked traded BEFORE the edge check, so this is the only evaluation: level-1 size < 1 or edge < min_edge ends the round. No z gate, no sigma floor (use_floor False).
            for k in range(n):
                tau = rend - tt[k]
                if tau > prm["tau_max"]: continue
                if tau < prm["tau_min"]: break
                s_ = sg[k]
                if s_ is None or s_ >= prm["calm"]: continue
                d = R["p"][k] - f0
                if abs(d) < 1.0: continue
                su = "u" if d > 0 else "d"; i = rows[k]
                ask, asz = c[su + "_ask"][i], c[su + "_asz"][i]; oask = c[("d" if d > 0 else "u") + "_ask"][i]
                if math.isnan(ask) or math.isnan(asz) or math.isnan(oask): continue
                if prm["require_fresh"] and (c["u_stale"][i] == 1.0 or c["d_stale"][i] == 1.0 or c["u_age"][i] > prm["book_age_max"] or c["d_age"][i] > prm["book_age_max"]): continue
                if asz < 1: break
                se = s_ if not prm["use_floor"] else max(s_, prm["sigma_floor"])
                z = abs(d) / (prm["cmodel"] * se * math.sqrt(max(tau, 1.0)))
                q = ncdf(z)
                if q - (ask + fee(ask)) >= prm["min_edge"]: trig = (k, "up" if d > 0 else "down", d, z, q, ask)
                break
        elif prm["mode"] in ("normal", "flip"):
            streak = 0
            for k in range(n):
                tau = rend - tt[k]
                if tau > prm["tau_max"]: continue
                if tau < prm["tau_min"]: break
                if prm["o6"] and sg[k] is not None and sg[k] >= prm["o6"][0] and tau > prm["o6"][1]: continue
                g = _gate(P, R, k, tau, prm)
                if g is None:
                    streak = 0
                    continue
                streak += 1
                if streak >= prm.get("persist", 1):
                    trig = g
                    break
        elif prm["mode"] == "random":
            # random time in window, random side, accept ask<=max_ask; no signal at all
            cand = [k for k in range(n) if prm["tau_min"] <= rend - tt[k] <= prm["tau_max"] and sg[k] is not None]
            if cand:
                k = rnd.choice(cand)
                side = rnd.choice(["up", "down"])
                su = "u" if side == "up" else "d"
                i = rows[k]
                ask = c[su + "_ask"][i]
                if not math.isnan(ask) and c[su + "_asz"][i] >= 1 and ask <= prm["max_ask"]:
                    d = pp[k] - f0
                    trig = (k, side, d, 0.0, 0.5, ask)
        if trig is None: continue
        signals += 1
        k, side, d, z, q, ask0 = trig
        if prm["mode"] == "flip":
            side = "down" if side == "up" else "up"
            su = "u" if side == "up" else "d"
            ask0 = c[su + "_ask"][rows[k]]
            if math.isnan(ask0) or c[su + "_asz"][rows[k]] < 1:
                misses += 1; continue
        # --- buy (rec-replay: buy_mode / buy_off / buy_retry). Fill at the book of the first row >= t_dec + lat_buy, walking the levels up to the limit.
        # A buy with no ask <= limit at landing is a rejection (MATCHER_LIQUIDITY_CHANGED). buy_retry > 0: after a rejection take a fresh decision at the first tick >= t_dec + lat_buy, same gates.
        attempt, filled = 0, False
        while True:
            t_dec = tt[k]
            kf = bisect.bisect_left(tt, t_dec + prm["lat_buy"])
            if kf >= n: break
            bmode = prm["buy_mode"] if prm["mode"] == "normal" else "slip"
            if bmode == "slip":
                limit = min(ask0 + prm["slip"], max_price_for_edge(q if prm["mode"] != "random" else 0.99, 0.025)) if prm["mode"] != "random" else ask0 + prm["slip"]
            elif bmode == "live":   # daily-s6-bot.mjs: base = level 2 ask when level 2 still clears the edge floor, limit = min(base + 0.02, price keeping a 2.5 % edge)
                base = ask0
                lad0 = P.ladder(rows[k], side, "ask")
                if len(lad0) > 1 and q - (lad0[1][0] + fee(lad0[1][0])) >= prm["min_edge"]: base = lad0[1][0]
                limit = min(base + 0.02, max_price_for_edge(q, 0.025))
            elif bmode == "plus":   # level-1 ask at decision + buy_off, no edge cap
                limit = ask0 + prm["buy_off"]
            else:                   # "edge": highest price that still clears min_edge, at most ask + buy_off
                limit = min(max_price_for_edge(q, prm["min_edge"]), ask0 + prm["buy_off"])
            lad = P.ladder(rows[kf], side, "ask")
            limit_sz = limit
            if prm["buy_size_live"] and bmode in ("plus", "edge"):   # size the order as the live rule would (limit of rule "live"), so only the matching tolerance changes
                base = ask0
                lad0 = P.ladder(rows[k], side, "ask")
                if len(lad0) > 1 and q - (lad0[1][0] + fee(lad0[1][0])) >= prm["min_edge"]: base = lad0[1][0]
                limit_sz = min(base + 0.02, max_price_for_edge(q, 0.025))
            target_sh = max(1, int(prm["stake"] / (limit_sz + fee(limit_sz))))
            got, cost, fill_sh, gross = [], 0.0, 0, 0.0
            for px_, sz_ in lad:
                if px_ > limit + 1e-12: break
                take = min(sz_, target_sh - fill_sh)
                if take <= 0: break
                fe = fee(px_) if prm["fee_mode"] == "venue" else 0.0
                cost += take * (px_ + fe)
                gross += take * px_
                fill_sh += take
            STATS["events"].append((rs, attempt, t_dec, side, ask0, q, limit, tt[kf], lad[:3], int(fill_sh)))   # one entry per buy attempt
            # integer shares like the bot (FAK; partial allowed)
            fill_sh_int = int(fill_sh)
            if fill_sh_int >= 1:
                if fill_sh_int < fill_sh:
                    cost *= fill_sh_int / fill_sh; gross *= fill_sh_int / fill_sh
                fill_sh = fill_sh_int
                filled = True
                if attempt > 0: STATS["retry_fill"] += 1
                break
            STATS["rej"] += 1
            if attempt >= prm["buy_retry"]: break
            tau_r = rend - tt[kf]
            if tau_r < prm["tau_min"] or tau_r > prm["tau_max"]: break
            g = _gate(P, R, kf, tau_r, prm)
            if g is None: break
            k, side, d, z, q, ask0 = g
            attempt += 1; STATS["retry"] += 1
        if not filled:
            misses += 1; continue
        avg_px = (cost / fill_sh)
        t_fill = tt[kf]
        # --- stop
        sign = 1.0 if side == "up" else -1.0
        stopped, sell_proceeds, sell_sh, t_stop, stop_info = False, 0.0, 0, None, None
        if prm["stop"]:
            rem, attempts, next_ok = fill_sh, 0, -1e18
            att_log, sell_gross, bid_trig = [], 0.0, None   # rec-replay: one entry per attempt (trigger t, bid at trigger, limit, landing t, shares sold, gross proceeds)
            for m in range(kf, n):
                tau = rend - tt[m]
                if tau < prm["stop_tau_min"]: break
                if tau > prm["stop_tau_max"]: continue
                if tt[m] < next_ok: continue
                s = sg[m]
                if s is None: continue
                if prm["fixed_sigma"] is not None: se = prm["fixed_sigma"]
                else: se = max(s, prm["sigma_floor"]) if prm["use_floor"] else s
                rel = sign * (R["dd"][m] if (prm["use_dd"] in ("both", "stop") and not math.isnan(R["dd"][m])) else (pp[m] - f0))
                qs = ncdf(rel / (prm["cmodel"] * se * math.sqrt(max(tau, 1.0))))
                dthr = prm["stop_d"] if prm["stop_k"] is None else -prm["stop_k"] * se * math.sqrt(prm["stop_sig_t"])
                if qs <= prm["stop_q"] or rel <= dthr:
                    # sell at first row >= tt[m] + lat_sell, limit = bestBid(at trigger row) - slip
                    bb = P.ladder(rows[m], side, "bid")
                    best_bid = bb[0][0] if bb else 0.25
                    # stop_retry: like the live bot (9c75b2d), retry every 1.5 s while triggered, 2 c lower per failed attempt
                    off = prm["stop_off"] if prm["stop_off"] is not None else prm["slip"]
                    if bid_trig is None: bid_trig = best_bid
                    lim = max(0.05, best_bid - off - (0.02 * attempts if prm["stop_retry"] else 0.0))
                    ks = bisect.bisect_left(tt, tt[m] + prm["lat_sell"])
                    if ks >= n: break
                    sl = P.ladder(rows[ks], side, "bid")
                    attempts += 1
                    rem0, gross0 = rem, sell_gross
                    for px_, sz_ in sl:
                        if px_ < lim - 1e-12: break
                        take = min(sz_, rem)
                        if take <= 0: break
                        sell_gross += take * px_
                        fe = fee(px_) if (prm["sell_fee"] and prm["fee_mode"] == "venue") else 0.0
                        sell_proceeds += take * (px_ - fe) * (0.99 if prm["fee_mode"] == "script" else 1.0)
                        rem -= take
                    sell_sh = fill_sh - rem
                    att_log.append((tt[m], best_bid, lim, tt[ks], rem0 - rem, sell_gross - gross0))
                    stopped = True; t_stop = tt[ks]
                    stop_info = (rel, best_bid, (sell_proceeds / sell_sh) if sell_sh else None, tt[m])
                    if rem <= 0 or not prm["stop_retry"]: break
                    next_ok = tt[m] + prm["stop_gap"]
        won = (R["up"] == (side == "up"))
        rem_sh = fill_sh - sell_sh if stopped else fill_sh
        payout = rem_sh * (1.0 if won else 0.0)
        if prm["fee_mode"] == "script":
            payout *= 0.99   # the scripts' accounting: no entry fee, 1% of payout (and of sale)
        pnl = payout + sell_proceeds - cost
        trades.append({
            "rs": rs, "day": day_of(rs), "side": side, "tau": rend - t_dec, "d": d, "z": z, "q": q, "ask_dec": ask0, "px": avg_px, "sh": fill_sh,
            "cost": cost, "pnl": pnl, "won_settle": won, "stopped": stopped, "stop_info": stop_info, "sell_proceeds": sell_proceeds, "sell_sh": sell_sh,
            "t_dec": t_dec, "k": k, "d_settle": R["d_settle"], "limit": limit, "gross_px": gross / fill_sh, "attempt": attempt,
            "att": att_log if (prm["stop"]) else [], "bid_trig": bid_trig if prm["stop"] else None, "sell_gross": sell_gross if prm["stop"] else 0.0,
        })
    return trades, misses, signals

def covered_rounds(P, prm_tau_max=130.0):
    n = 0
    for rs in P.starts:
        R = P.rounds[rs]
        if any(s is not None for s, t in zip(R["sig"], R["t"]) if rs + 300 - t <= prm_tau_max):
            n += 1
    return n

def summarize(trades, rounds_cov=None, stake=21.40, bal0=428.0, label=""):
    n = len(trades)
    if n == 0:
        return {"label": label, "n": 0}
    cost = sum(t["cost"] for t in trades); pnl = sum(t["pnl"] for t in trades)
    wr_settle = sum(1 for t in trades if t["won_settle"]) / n
    wr_pnl = sum(1 for t in trades if t["pnl"] > 0) / n
    stops = sum(1 for t in trades if t["stopped"])
    days = (rounds_cov * 300 / 86400.0) if rounds_cov else None
    # flat-stake equity (stake fixed in $) -> normalise each trade to the stake
    eq, peak, mdd = bal0, bal0, 0.0
    for t in sorted(trades, key=lambda x: x["rs"]):
        scale = stake / t["cost"] if t["cost"] > 0 else 0
        eq += t["pnl"] * scale
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
    # compounding 5% of balance
    eqc, peakc, mddc = bal0, bal0, 0.0
    for t in sorted(trades, key=lambda x: x["rs"]):
        stk = max(1.0, 0.05 * eqc)
        eqc += (t["pnl"] / t["cost"]) * stk
        peakc = max(peakc, eqc)
        mddc = max(mddc, (peakc - eqc) / peakc)
    out = {"label": label, "n": n, "wr_settle": wr_settle, "wr_pnl": wr_pnl, "stops": stops, "roi": pnl / cost, "pnl_units": pnl, "cost": cost,
           "avg_px": sum(t["px"] for t in trades) / n, "mdd_flat": mdd, "mdd_comp": mddc, "final_comp": eqc}
    if days:
        out["days"] = days
        out["trades_per_day"] = n / days
        out["pnl_day_flat"] = (pnl / cost) * stake * n / days
    return out

def fmt(s):
    if s["n"] == 0:
        return "%-44s n=0" % s["label"]
    return "%-44s n=%-4d t/day=%5.1f WRset=%5.1f%% WRpnl=%5.1f%% stops=%-3d avg_px=%.3f ROI=%+6.2f%% PnL/day@21.4=%+7.2f maxDD(flat)=%4.1f%% maxDD(5%%comp)=%4.1f%%" % (
        s["label"], s["n"], s.get("trades_per_day", float("nan")), 100 * s["wr_settle"], 100 * s["wr_pnl"], s["stops"], s["avg_px"], 100 * s["roi"], s.get("pnl_day_flat", float("nan")), 100 * s["mdd_flat"], 100 * s["mdd_comp"])
