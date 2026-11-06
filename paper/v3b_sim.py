"""v3b simulator: v3_sim generalised to position pools, free sizes and the shared-management variants.
Forward replay row by row; decisions read only rows at or before the clock; an order fills at the first row >= send + latency
(buys lat_buy, sells lat_sell, as the engine), FAK, walking the 3 recorded levels, integer shares on buys; liquidity we took is
removed from that side's snapshot until its seq changes (shared by every pool). Fees 7 % x p x (1-p) per share on every fill.
Pools: 'bot' (forced live-rule entries, engine limit and 21.40 USD stake) and 'model', or one merged pool 'all'.
The live stop (copied from m7engine: q <= 0.45 or d <= -3 vs the round open, tau 10-300, limit bid - 0.02 - 0.02 x attempts,
retry every 1.5 s) watches the pools listed in variant['stop_pools'], per side held.
Model: every grid second, if no order is pending on its pool and >= 1.5 s since the last order of any pool: entry / add (size S USD),
sell a fraction f of one side, flip (sell all of a side, buy size S of the other on the same snapshot), or hold, by per-share EV
from the decision-row book and fees. Cap: at most 400 USD bought per round (all pools). Bot orders are exempt from the 1.5 s gap,
as in the engine. Not modelled: the market maker's reaction to our own orders."""
import sys, math, bisect
BT = "/private/tmp/claude-501/-Users-ndn18-PersonalProjects-daily-bot/bccc1ff4-9f68-4122-879b-2eab53fc03c5/scratchpad/agents/backtest"
sys.path.insert(0, BT)
from lib import fee, ncdf
from m7engine import max_price_for_edge, _real_row, _rtt
from v3_sim import Liq, _buy_walk, _sell_walk

BOT_STAKE = 21.40
CAP = 400.0
GAP = 1.5
OTHER = {"up": "down", "down": "up"}

def okey(rs, o):
    """the key of one model order, the same in every re-run of a round: round, send time, action, side"""
    return "%d|%.3f|%s|%s" % (rs, o["t"], o["act"], o["side"])

def sim_round(P, rs, prm, variant, model_dec=None, bot_dec=None, cfg=None, liq=True, bot_vps=None, log=None, up_override=None, stress=None, round_cap=None,
              on_send=None, fills=None):
    """variant: dict(model_pool='model'|'all'|None, bot_pool='bot'|'all'|None, stop_pools=set of pools).
    model_dec = (t_g list, k_g list, p_up list); bot_dec = (k, side, q, ask0) of the engine's live trade or None.
    cfg: m_entry, m_add, m_sell, m_flip, size, frac. bot_vps: the bot's realised value per share in this round under its own
    management (for the 'model sold bot shares' attribution).
    Live leg only (both None otherwise, which changes nothing): on_send(order) sees every model order as it is sent; fills maps
    okey -> (shares, cost incl fee | proceeds net of fee, fee) of the venue's answer, used instead of the book walk for that order."""
    c = P.c
    R = P.rounds[rs]
    tt, pp, sg, rows = R["t"], R["p"], R["sig"], R["rows"]
    f0 = R["f0"]; rend = rs + 300.0; n = len(tt); up_wins = R["up"] if up_override is None else up_override  # override: placebo outcome on train only
    L = Liq(P, liq)
    lat_b, lat_s = prm["lat_buy"], prm["lat_sell"]
    # fill_mode "real" (engine m7engine._real_row / _rtt, leak review): a row's data exist at stamp + max RTT; an order fills on
    # the first snapshot of that side newer than the decision row's, at or after landing (send + latency)
    real = prm.get("fill_mode", "base") == "real"
    def frow(k_dec, t_send, lat, side):
        return _real_row(P, rows, tt, k_dec, t_send + lat, side) if real else bisect.bisect_left(tt, t_send + lat)
    pools = set(x for x in (variant.get("model_pool"), variant.get("bot_pool")) if x)
    H = {pl: {"up": 0.0, "down": 0.0} for pl in pools}            # shares held
    HB = {pl: {"up": 0.0, "down": 0.0} for pl in pools}           # of which bot-origin (merged pool attribution, pro rata)
    cash = {pl: 0.0 for pl in pools}
    ST = {}                                                        # stop state per (pool, side)
    acc = dict(fees=0.0, spent=0.0, mspent=0.0, orders=0, last=-1e18, sold_bot=0.0, sold_bot_help=0.0, sold_bot_n=0)
    acts = dict(bot_entry=0, entry=0, add=0, sell=0, flip=0, stop=0, miss=0)
    pend = []
    mp_ = variant.get("model_pool"); stop_pools = variant.get("stop_pools", set())
    # stress options (lead check 1 and 2): impact = after our buy at X on a side, later buys there fill only at >= X; after our sale
    # at Y, later sales there only at <= Y; at most one of our fills per side per book snapshot (seq). max_orders = cap per round
    # (model orders; bot orders exempt but counted). gap_all = the 1.5 s gap also binds the bot entry and the stop.
    SX = stress or {}
    IMP = {"up": dict(bpx=None, spx=None, seq=None), "down": dict(bpx=None, spx=None, seq=None)}
    def imp_lad(i, side, kind, lad):
        if not SX.get("impact"): return lad
        e = IMP[side]
        if e["seq"] is not None and L._seq(i, side) == e["seq"]: return []
        if kind == "ask" and e["bpx"] is not None: return [(p_, z_) for p_, z_ in lad if p_ >= e["bpx"] - 1e-12]
        if kind == "bid" and e["spx"] is not None: return [(p_, z_) for p_, z_ in lad if p_ <= e["spx"] + 1e-12]
        return lad
    def imp_note(i, side, kind, tk):
        if not SX.get("impact") or not tk: return
        e = IMP[side]; e["seq"] = L._seq(i, side)
        if kind == "ask": e["bpx"] = max(p_ for p_, z_ in tk)
        else: e["spx"] = min(p_ for p_, z_ in tk)
    bot_wait = [None]

    def lg(**kw):
        if log is not None: log.append(dict(rs=rs, **kw))

    def bought(pl, side, m, sh, cost, fe, bot):
        if H[pl][side] <= 1e-9:
            ST[(pl, side)] = dict(frm=m, att=0, next_ok=-1e18)
        H[pl][side] += sh; cash[pl] -= cost; acc["fees"] += fe; acc["spent"] += cost
        if bot: HB[pl][side] += sh
        else: acc["mspent"] += cost

    def sold(pl, side, sh, proc, fe, by_model):
        if sh <= 0: return
        hb = HB[pl][side]; h = H[pl][side]
        b = sh * (hb / h) if h > 0 else 0.0
        if by_model and b > 1e-9 and pl != "bot":
            acc["sold_bot"] += b; acc["sold_bot_n"] += 1
            if bot_vps is not None: acc["sold_bot_help"] += b * (proc / sh - bot_vps)
        HB[pl][side] -= b; H[pl][side] -= sh; cash[pl] += proc; acc["fees"] += fe
        if H[pl][side] <= 1e-9: H[pl][side] = 0.0; HB[pl][side] = 0.0

    def send(o):
        pend.append(o); acc["orders"] += 1; acc["last"] = o["t"]
        if on_send is not None and o["act"] in ("entry", "add", "sell", "flip"): on_send(o)

    def apply(m):
        todo = [o for o in pend if o["row"] == m]
        for o in todo:
            pend.remove(o)
            i = rows[m]
            ov = fills.get(okey(rs, o)) if fills is not None and o["act"] in ("entry", "add", "sell") else None
            if o["kind"] == "buy":
                if ov is not None: sh, cost, fe, tk = ov[0], ov[1], ov[2], []
                else: sh, cost, fe, tk = _buy_walk(imp_lad(i, o["side"], "ask", L.ladder(i, o["side"], "ask")), o["limit"], o["target"])
                if sh >= 1:
                    imp_note(i, o["side"], "ask", tk)
                    L.take(i, o["side"], "ask", tk); bought(o["pool"], o["side"], m, sh, cost, fe, o["act"] == "bot_entry"); acts[o["act"]] += 1
                else:
                    acts["miss"] += 1
                lg(t=o["t"], tau=rend - o["t"], act=o["act"], side=o["side"], p=o.get("p"), sh=sh, px=(cost / sh if sh else None), pool=o["pool"])
            else:
                pl, side = o["pool"], o["side"]
                want = min(o["shares"], H[pl][side])
                if ov is not None: s_, proc, fe, tk = min(ov[0], want), ov[1], ov[2], []
                else: s_, proc, fe, tk = _sell_walk(imp_lad(i, side, "bid", L.ladder(i, side, "bid")), o["limit"], want)
                imp_note(i, side, "bid", tk)
                L.take(i, side, "bid", tk)
                sold(pl, side, s_, proc, fe, o["act"] != "stop")
                lg(t=o["t"], tau=rend - o["t"], act=o["act"] + "_sell", side=side, p=o.get("p"), sh=s_, px=(proc / s_ if s_ else None), pool=pl)
                if o["act"] == "stop": acts["stop"] += 1
                elif o["act"] == "sell": acts["sell"] += 1
                elif o["act"] == "flip":
                    if real and H[pl][side] <= 1e-9 and o["target2"] >= 1:
                        r2 = frow(m, tt[m], lat_b, OTHER[side])
                        if r2 < n:
                            send(dict(kind="buy", t=tt[m], row=r2, pool=pl, side=OTHER[side], limit=o["limit2"], target=o["target2"], act="flip", p=o.get("p2")))
                        else:
                            acts["miss"] += 1
                    elif H[pl][side] <= 1e-9 and o["target2"] >= 1:
                        acc["orders"] += 1
                        sh, cost, fe2, tk2 = _buy_walk(imp_lad(i, OTHER[side], "ask", L.ladder(i, OTHER[side], "ask")), o["limit2"], o["target2"])
                        if sh >= 1:
                            imp_note(i, OTHER[side], "ask", tk2)
                            L.take(i, OTHER[side], "ask", tk2); bought(pl, OTHER[side], m, sh, cost, fe2, False); acts["flip"] += 1
                            lg(t=o["t"], tau=rend - o["t"], act="flip_buy", side=OTHER[side], p=o.get("p2"), sh=sh, px=cost / sh, pool=pl)
                        else:
                            acts["sell"] += 1
                    else:
                        acts["sell"] += 1

    def pending_on(pl, side=None):
        return any(o["pool"] == pl and (side is None or o["side"] == side) for o in pend)

    def stop_check(m):
        tau = rend - tt[m]
        if tau < prm["stop_tau_min"] or tau > prm["stop_tau_max"]: return
        s = sg[m]
        if s is None: return
        se = max(s, prm["sigma_floor"])
        for pl in stop_pools:
            for side in ("up", "down"):
                # as the engine: a stop retry may go out while the previous stop sale is still in flight (non-stop orders block it)
                if H[pl][side] <= 1e-9 or any(o["pool"] == pl and o["side"] == side and o["act"] != "stop" for o in pend): continue
                stt = ST[(pl, side)]
                if m < stt["frm"] or tt[m] < stt["next_ok"]: continue
                if SX.get("gap_all") and tt[m] < acc["last"] + GAP: continue
                sign = 1.0 if side == "up" else -1.0
                rel = sign * (pp[m] - f0)
                qs = ncdf(rel / (prm["cmodel"] * se * math.sqrt(max(tau, 1.0))))
                if qs <= prm["stop_q"] or rel <= prm["stop_d"]:
                    bb = P.ladder(rows[m], side, "bid")
                    best_bid = bb[0][0] if bb else 0.25
                    off = prm.get("stop_off") if prm.get("stop_off") is not None else prm["slip"]   # live stop sale: bid - 0.05 (rec-replay engine stop_off)
                    lim = max(0.05, best_bid - off - 0.02 * stt["att"])
                    t_send = tt[m] + (_rtt(P, rows[m]) if real else 0.0)
                    ks = frow(m, t_send, lat_s, side)
                    if ks >= n:
                        stt["next_ok"] = 1e18; continue
                    stt["att"] += 1; stt["next_ok"] = tt[m] + prm.get("stop_gap", GAP)
                    send(dict(kind="sell", t=t_send, row=ks, pool=pl, side=side, shares=H[pl][side], limit=lim, act="stop"))

    def model_decide(t_g, k_g, p_up):
        if pending_on(mp_) or t_g < acc["last"] + GAP: return
        if SX.get("max_orders") is not None and acc["orders"] + 2 > SX["max_orders"]: return
        i = rows[k_g]
        if c["u_stale"][i] == 1.0 or c["d_stale"][i] == 1.0: return
        ps = {"up": p_up, "down": 1.0 - p_up}
        info = {}
        for s in ("up", "down"):
            su = "u" if s == "up" else "d"
            ask, asz, bid = c[su + "_ask"][i], c[su + "_asz"][i], c[su + "_bid"][i]
            okb = not (math.isnan(ask) or math.isnan(asz)) and asz >= 1
            gs = ((bid - fee(bid)) - ps[s]) if not math.isnan(bid) else -9.0
            info[s] = (ask, bid, (ps[s] - (ask + fee(ask))) if okb else -9.0, gs)
        size = min(cfg["size"], CAP - acc["spent"])
        if round_cap is not None: size = min(size, round_cap - acc["mspent"])   # capital cap: model buys (entry, adds, flip) per round <= round_cap
        kf = bisect.bisect_left(tt, t_g + lat_b); ks = bisect.bisect_left(tt, t_g + lat_s)
        h = H[mp_]
        cands = []
        def buy_target(s, margin):
            limit = min(info[s][0] + prm["slip"], max_price_for_edge(ps[s], margin))
            return limit, (int(size / (limit + fee(limit))) if size > 0 else 0)
        if h["up"] + h["down"] <= 1e-9:
            s = max(("up", "down"), key=lambda x: info[x][2])
            if info[s][2] >= cfg["m_entry"] and kf < n:
                cands.append((info[s][2] * size / max(info[s][0], 0.01), "entry", s))
        else:
            for s in ("up", "down"):
                if h[s] <= 1e-9: continue
                o = OTHER[s]; gs = info[s][3]
                if cfg["m_sell"] is not None and gs >= cfg["m_sell"] and ks < n:
                    cands.append((gs * cfg["frac"] * h[s], "sell", s))
                if cfg["m_flip"] is not None and info[o][2] >= cfg["m_flip"] and gs >= (cfg["m_sell"] if cfg["m_sell"] is not None else 0.0) and ks < n:
                    cands.append((gs * h[s] + info[o][2] * size / max(info[o][0], 0.01), "flip", s))
                if cfg["m_add"] is not None and info[s][2] >= cfg["m_add"] and kf < n:
                    cands.append((info[s][2] * size / max(info[s][0], 0.01), "add", s))
        if not cands: return
        ev, a, s = max(cands)
        if a in ("entry", "add"):
            limit, tgt = buy_target(s, cfg["m_entry"] if a == "entry" else cfg["m_add"])
            r_ = frow(k_g, t_g, lat_b, s)
            if tgt >= 1 and r_ < n: send(dict(kind="buy", t=t_g, row=r_, pool=mp_, side=s, limit=limit, target=tgt, act=a, p=ps[s]))
        elif a == "sell":
            want = h[s] if cfg["frac"] >= 1.0 else min(h[s], max(1.0, math.floor(cfg["frac"] * h[s])))
            r_ = frow(k_g, t_g, lat_s, s)
            if r_ < n: send(dict(kind="sell", t=t_g, row=r_, pool=mp_, side=s, shares=want, limit=max(0.05, info[s][1] - prm["slip"]), act="sell", p=ps[s]))
        else:
            o = OTHER[s]
            limit2, tgt2 = buy_target(o, cfg["m_flip"])
            r_ = frow(k_g, t_g, lat_s, s)
            if r_ < n: send(dict(kind="sell", t=t_g, row=r_, pool=mp_, side=s, shares=h[s], limit=max(0.05, info[s][1] - prm["slip"]), act="flip", p=ps[s],
                      limit2=limit2, target2=tgt2, p2=ps[o]))

    if model_dec is not None:
        tg, kg, pu = model_dec; gi, ng = 0, len(model_dec[0])
    for m in range(n):
        if model_dec is not None and mp_:
            while gi < ng and tg[gi] < tt[m]:
                model_decide(tg[gi], kg[gi], pu[gi]); gi += 1
        apply(m)
        if bot_dec is not None and (m == bot_dec[0] or bot_wait[0] == m):
            k0, side0, q0, ask0 = bot_dec
            if SX.get("gap_all") and tt[m] < acc["last"] + GAP:
                bot_wait[0] = m + 1 if m + 1 < n else None
                stop_check(m); continue
            bot_wait[0] = None
            t_send = tt[m] + (_rtt(P, rows[m]) if real else 0.0)
            kf = frow(k0, t_send, lat_b, side0)
            if kf < n:
                if prm.get("buy_mode") == "live":   # daily-s6-bot.mjs rule, copied from the rec-replay engine: level 2 when it still clears min_edge, + 0.02
                    lad0 = P.ladder(rows[k0], side0, "ask"); base = ask0
                    if len(lad0) > 1 and q0 - (lad0[1][0] + fee(lad0[1][0])) >= prm["min_edge"]: base = lad0[1][0]
                    limit = min(base + 0.02, max_price_for_edge(q0, 0.025))
                else:
                    limit = min(ask0 + prm["slip"], max_price_for_edge(q0, 0.025))
                tgt = max(1, int(BOT_STAKE / (limit + fee(limit))))
                send(dict(kind="buy", t=t_send, row=kf, pool=variant["bot_pool"], side=side0, limit=limit, target=tgt, act="bot_entry", p=q0))
            else:
                acts["miss"] += 1
        stop_check(m)
    if model_dec is not None and mp_:
        while gi < ng:
            model_decide(tg[gi], kg[gi], pu[gi]); gi += 1
    acts["miss"] += sum(1 for o in pend if o["kind"] == "buy")
    for pl in pools:
        for side in ("up", "down"):
            if H[pl][side] > 0: cash[pl] += H[pl][side] * (1.0 if (up_wins == (side == "up")) else 0.0)
    return dict(rs=rs, pnl=sum(cash.values()), pnl_pool=dict(cash), fees=acc["fees"], spent=acc["spent"], mspent=acc["mspent"], orders=acc["orders"], acts=acts,
                sold_bot=acc["sold_bot"], sold_bot_n=acc["sold_bot_n"], sold_bot_help=acc["sold_bot_help"])
