"""v3 position-management simulator: forward replay of one round, row by row, never reading a row later than the clock except to
execute an order already sent (fill at the first row >= send time + latency, as the engine does).
Fill code (buy walk, integer shares, sell walk, fees) and the stop (trigger, limit bid - 0.02 - 0.02 x attempts, retry every 1.5 s,
sale at the first row >= trigger + lat_sell) are copied from m7engine.run / labeler.label_trade; additions:
  - liquidity we took is removed from that side's book snapshot until its seq changes (liq=True);
  - model actions every grid second: entry, add, sell (cash out), flip (sell all, then buy the other side on the same snapshot);
  - one order (a flip = one action of two orders) per 1.5 s; the stop is exempt from the 1.5 s gap after an entry, as in the engine;
  - exposure cap: at most 3 x 21.40 USD bought per round.
Not modelled: the market maker's reaction to our own orders."""
import sys, math, bisect
BT = "/private/tmp/claude-501/-Users-ndn18-PersonalProjects-daily-bot/bccc1ff4-9f68-4122-879b-2eab53fc03c5/scratchpad/agents/backtest"
sys.path.insert(0, BT)
from lib import fee, ncdf
from m7engine import max_price_for_edge

STAKE = 21.40
CAP = 3 * STAKE
GAP = 1.5

class Liq:
    def __init__(self, P, on):
        self.P, self.on, self.d = P, on, {}
    def _seq(self, i, side):
        return self.P.c["u_seq" if side == "up" else "d_seq"][i]
    def ladder(self, i, side, kind):
        lad = self.P.ladder(i, side, kind)
        if not self.on: return lad
        e = self.d.get((side, kind))
        if e is None or e[0] != self._seq(i, side): return lad
        cons = e[1]
        return [(p, z - cons.get(p, 0.0)) for p, z in lad if z - cons.get(p, 0.0) > 1e-9]
    def take(self, i, side, kind, takes):
        if not self.on or not takes: return
        sq = self._seq(i, side)
        e = self.d.get((side, kind))
        if e is None or e[0] != sq:
            e = (sq, {}); self.d[(side, kind)] = e
        for p, z in takes: e[1][p] = e[1].get(p, 0.0) + z

def _buy_walk(lad, limit, target_sh):
    """engine buy walk + integer shares; returns (shares, cost incl fee, fee part, takes)"""
    cost, fill_sh, fees, takes = 0.0, 0, 0.0, []
    for px_, sz_ in lad:
        if px_ > limit + 1e-12: break
        take = min(sz_, target_sh - fill_sh)
        if take <= 0: break
        fe = fee(px_)
        cost += take * (px_ + fe); fees += take * fe
        fill_sh += take; takes.append((px_, take))
    if fill_sh < 1: return 0, 0.0, 0.0, []
    fill_sh_int = int(fill_sh)
    if fill_sh_int < fill_sh:
        r = fill_sh_int / fill_sh
        cost *= r; fees *= r; takes = [(p, z * r) for p, z in takes]
    if fill_sh_int < 1: return 0, 0.0, 0.0, []
    return fill_sh_int, cost, fees, takes

def _sell_walk(lad, lim, rem):
    """engine sell walk; returns (shares sold, proceeds net of fee, fee part, takes)"""
    proceeds, fees, sold, takes = 0.0, 0.0, 0.0, []
    for px_, sz_ in lad:
        if px_ < lim - 1e-12: break
        take = min(sz_, rem - sold)
        if take <= 0: break
        fe = fee(px_)
        proceeds += take * (px_ - fe); fees += take * fe; sold += take; takes.append((px_, take))
    return sold, proceeds, fees, takes

def sim_round(P, rs, prm, mode, dec, cfg=None, liq=True, log=None):
    """mode 'replay': dec = (k, side, q, ask0) of an engine trade, bought at row k with the engine limit, then the stop.
    mode 'model': dec = (t_g list, k_g list, p_up list); cfg = margins dict. Returns a result dict (USD)."""
    c = P.c
    R = P.rounds[rs]
    tt, pp, sg, rows = R["t"], R["p"], R["sig"], R["rows"]
    f0 = R["f0"]; rend = rs + 300.0; n = len(tt)
    up_wins = R["up"]
    L = Liq(P, liq)
    st = dict(side=None, sh=0.0, cash=0.0, fees=0.0, spent=0.0, orders=0, last=-1e18, pend=None,
              stop_on=False, stop_from=None, att=0, next_ok=-1e18)
    acts = dict(entry=0, add=0, sell=0, flip=0, stop=0, miss=0)
    use_stop = prm["stop"] if mode == "replay" else cfg["stop"]
    lat_b, lat_s = prm["lat_buy"], prm["lat_sell"]

    def open_or_add(side, kf, sh, cost, fe):
        if st["side"] is None:
            st["side"] = side; st["stop_from"] = kf; st["att"] = 0; st["next_ok"] = -1e18
        st["sh"] += sh; st["cash"] -= cost; st["fees"] += fe; st["spent"] += cost

    def apply_pending(m):
        pe = st["pend"]
        if pe is None or pe["row"] != m: return
        st["pend"] = None
        i = rows[m]
        if pe["kind"] == "buy":
            lad = L.ladder(i, pe["side"], "ask")
            sh, cost, fe, tk = _buy_walk(lad, pe["limit"], pe["target"])
            if sh >= 1:
                L.take(i, pe["side"], "ask", tk); open_or_add(pe["side"], m, sh, cost, fe); acts[pe["act"]] += 1
            else:
                acts["miss"] += 1
            if log is not None: log.append(dict(rs=rs, t=pe["t"], tau=rend - pe["t"], act=pe["act"], side=pe["side"], p=pe.get("p"), sh=sh, px=(cost / sh if sh else None), row=m, drow=pe.get("drow"), limit=pe["limit"]))
        else:  # sell (model sell, stop sale, or the sell leg of a flip)
            side = st["side"]
            lad = L.ladder(i, side, "bid")
            sold, proc, fe, tk = _sell_walk(lad, pe["limit"], st["sh"])
            L.take(i, side, "bid", tk)
            st["sh"] -= sold; st["cash"] += proc; st["fees"] += fe
            full = st["sh"] <= 1e-9
            if pe["act"] == "stop":
                acts["stop"] += 1
            if log is not None: log.append(dict(rs=rs, t=pe["t"], tau=rend - pe["t"], act=pe["act"] + "_sell", side=side, p=pe.get("p"), sh=sold, px=(proc / sold if sold else None), row=m, drow=pe.get("drow"), limit=pe["limit"]))
            if full:
                st["side"] = None; st["sh"] = 0.0
            if pe["act"] == "sell": acts["sell"] += 1
            if pe["act"] == "flip":
                if full and pe["buy_side"] is not None:
                    # buy leg on the same snapshot, the other side's ask ladder
                    lad = L.ladder(i, pe["buy_side"], "ask")
                    sh, cost, fe2, tk2 = _buy_walk(lad, pe["limit2"], pe["target2"])
                    st["orders"] += 1
                    if sh >= 1:
                        L.take(i, pe["buy_side"], "ask", tk2); open_or_add(pe["buy_side"], m, sh, cost, fe2); acts["flip"] += 1
                    else:
                        acts["sell"] += 1
                else:
                    acts["sell"] += 1

    def send(kind, t, row, **kw):
        st["pend"] = dict(kind=kind, t=t, row=row, **kw); st["orders"] += 1; st["last"] = t

    def stop_check(m):
        if not use_stop or st["side"] is None or st["pend"] is not None or m < st["stop_from"]: return
        tau = rend - tt[m]
        if tau < prm["stop_tau_min"] or tau > prm["stop_tau_max"]: return
        if tt[m] < st["next_ok"]: return
        s = sg[m]
        if s is None: return
        se = max(s, prm["sigma_floor"])
        sign = 1.0 if st["side"] == "up" else -1.0
        rel = sign * (pp[m] - f0)
        qs = ncdf(rel / (prm["cmodel"] * se * math.sqrt(max(tau, 1.0))))
        if qs <= prm["stop_q"] or rel <= prm["stop_d"]:
            bb = P.ladder(rows[m], st["side"], "bid")
            best_bid = bb[0][0] if bb else 0.25
            lim = max(0.05, best_bid - prm["slip"] - 0.02 * st["att"])
            ks = bisect.bisect_left(tt, tt[m] + lat_s)
            if ks >= n:
                st["next_ok"] = 1e18; return
            st["att"] += 1; st["next_ok"] = tt[m] + GAP
            send("sell", tt[m], ks, limit=lim, act="stop")

    def book(i, side):
        su = "u" if side == "up" else "d"
        return c[su + "_ask"][i], c[su + "_asz"][i], c[su + "_bid"][i]

    def model_decide(t_g, k_g, p_up):
        if st["pend"] is not None or t_g < st["last"] + GAP: return
        i = rows[k_g]
        if c["u_stale"][i] == 1.0 or c["d_stale"][i] == 1.0: return
        ps = {"up": p_up, "down": 1.0 - p_up}
        info = {}
        for s in ("up", "down"):
            ask, asz, bid = book(i, s)
            okb = not (math.isnan(ask) or math.isnan(asz)) and asz >= 1
            info[s] = (ask, bid, (ps[s] - (ask + fee(ask))) if okb else -9.0)
        budget = min(STAKE, CAP - st["spent"])
        kf = bisect.bisect_left(tt, t_g + lat_b); ks = bisect.bisect_left(tt, t_g + lat_s)
        def buy_args(s, margin):
            ask = info[s][0]
            limit = min(ask + prm["slip"], max_price_for_edge(ps[s], margin))
            tgt = int(budget / (limit + fee(limit))) if budget > 0 else 0
            return limit, tgt
        if st["side"] is None:
            s = max(("up", "down"), key=lambda x: info[x][2])
            if info[s][2] >= cfg["m_entry"] and kf < n:
                limit, tgt = buy_args(s, cfg["m_entry"])
                if tgt >= 1: send("buy", t_g, kf, drow=k_g, side=s, limit=limit, target=tgt, act="entry", p=ps[s])
            return
        s = st["side"]; o = "down" if s == "up" else "up"
        ask_s, bid_s, edge_s = info[s]
        gain_sell = ((bid_s - fee(bid_s)) - ps[s]) if not math.isnan(bid_s) else -9.0
        cands = []
        if cfg["m_sell"] is not None and gain_sell >= cfg["m_sell"] and ks < n:
            cands.append((gain_sell * st["sh"], "sell"))
        if cfg["m_flip"] is not None and info[o][2] >= cfg["m_flip"] and gain_sell >= (cfg["m_sell"] if cfg["m_sell"] is not None else 0.0) and ks < n:
            cands.append((gain_sell * st["sh"] + info[o][2] * (budget / max(info[o][0], 0.01)), "flip"))
        if cfg["m_add"] is not None and edge_s >= cfg["m_add"] and kf < n:
            cands.append((edge_s * (budget / max(ask_s, 0.01)), "add"))
        if not cands: return
        ev, a = max(cands)
        if a == "sell":
            send("sell", t_g, ks, drow=k_g, limit=max(0.05, bid_s - prm["slip"]), act="sell", p=ps[s])
        elif a == "flip":
            # the buy leg's budget: what the cap allows after this round's spending
            limit2 = min(info[o][0] + prm["slip"], max_price_for_edge(ps[o], cfg["m_flip"]))
            tgt2 = int(budget / (limit2 + fee(limit2))) if budget > 0 else 0
            send("sell", t_g, ks, drow=k_g, limit=max(0.05, bid_s - prm["slip"]), act="flip", p=ps[s], buy_side=(o if tgt2 >= 1 else None), limit2=limit2, target2=tgt2)
        else:
            limit, tgt = buy_args(s, cfg["m_add"])
            if tgt >= 1: send("buy", t_g, kf, drow=k_g, side=s, limit=limit, target=tgt, act="add", p=ps[s])

    if mode == "replay":
        k0, side0, q0, ask0 = dec
        events = {}
    else:
        tg, kg, pu = dec
        gi = 0; ng = len(tg)
    for m in range(n):
        if mode == "model":
            while gi < ng and tg[gi] < tt[m]:
                model_decide(tg[gi], kg[gi], pu[gi]); gi += 1
        apply_pending(m)
        if mode == "replay" and m == k0:
            kf = bisect.bisect_left(tt, tt[k0] + lat_b)
            if kf < n:
                limit = min(ask0 + prm["slip"], max_price_for_edge(q0, 0.025))
                tgt = max(1, int(prm["stake"] / (limit + fee(limit))))
                send("buy", tt[k0], kf, side=side0, limit=limit, target=tgt, act="entry", p=q0)
            else:
                acts["miss"] += 1
        stop_check(m)
    if mode == "model":
        while gi < ng:
            model_decide(tg[gi], kg[gi], pu[gi]); gi += 1
    if st["pend"] is not None and st["pend"]["kind"] == "buy":
        acts["miss"] += 1
    if st["side"] is not None and st["sh"] > 0:
        st["cash"] += st["sh"] * (1.0 if (up_wins == (st["side"] == "up")) else 0.0)
    return dict(rs=rs, pnl=st["cash"], fees=st["fees"], spent=st["spent"], orders=st["orders"], acts=acts)
