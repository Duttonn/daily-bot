"""Parity gate (dev machine): the paper trader's replay output against the offline pipeline on the same recorder file.
Reference = entry-ml lgbcf.py's path: rec-replay ws200 extract -> m7engine.Prep -> b3v_vps.build_round features -> LightGBM predict ->
v3b_sim.sim_round (model pool, no stop, stress (1)+(2)), with each round's 5 % cap taken from the trader's own round events.
python3 parity.py TRADER_OUT_DIR ENTRY_ML_DIR EXTRACT_SUBDIR RS_FROM RS_TO     e.g. ... entry-ml v5/cols_ws200 1793901600 1793905200"""
import sys, os, io, json, contextlib, collections
out_dir, EM, sub, rs0, rs1 = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
HERE = os.path.dirname(os.path.abspath(__file__))
pol = json.load(open(os.path.join(HERE, "policy.json")))
sys.argv = [sys.argv[0], sub]; sys.path.insert(0, EM)
with contextlib.redirect_stdout(io.StringIO()):
    import b3v_vps as V                  # extract + Prep + build_round (numpy / scipy)
import m7engine
def _no(*a, **k): raise RuntimeError
m7engine._real_row = _no; m7engine._rtt = _no
import v3b_sim
import numpy as np, pandas as pd, lightgbm as lgb
P = V.P
rounds = [rs for rs in P.starts if rs0 <= rs < rs1]
X = pd.concat([V.build_round(rs, gmax=pol["gmax"]) for rs in rounds], ignore_index=True)
U = X[X.side_up == 1].sort_values(["rs", "t_dec"]).reset_index(drop=True)
qv = np.clip(U.q.fillna(0.5).values, 1e-4, 1 - 1e-4)
raw = lgb.Booster(model_file=os.path.join(HERE, pol["model"])).predict(U[pol["feats"]], raw_score=True)
U["p"] = np.clip(1 / (1 + np.exp(-(np.log(qv / (1 - qv)) + raw))), 1e-4, 1 - 1e-4)
# trader output
ev = [json.loads(l) for l in open(os.path.join(out_dir, "paper.jsonl"))]
D = pd.read_csv(os.path.join(out_dir, "decisions.csv"))
D = D[(D.rs >= rs0) & (D.rs < rs1)]
m = U.merge(D, left_on=["rs", "t_dec"], right_on=["rs", "t"], how="outer", suffixes=("_ref", "_tr"), indicator=True)
both = m[m._merge == "both"]; dp = (both.p_ref - both.p_tr).abs()
lines = ["rounds %d (%s .. %s) | decision points: reference %d, trader %d, matched %d | p: exact %d, max |diff| %.3g" % (
    len(rounds), rounds[0], rounds[-1], len(U), len(D), len(both), int((dp == 0).sum()), dp.max() if len(dp) else float("nan"))]
caps = {e["rs"]: e["cap"] for e in ev if e.get("ev") == "round"}
tr_fill = collections.defaultdict(list)
for e in ev:
    if e.get("ev") in ("fill", "miss"): tr_fill[e["rs"]].append((round(e["t"], 3), e["act"], e["side"], e["sh"], e["px"]))
tr_pnl = {e["rs"]: (e["pnl"], e["up"], e["src"], e["pyth_up"]) for e in ev if e.get("ev") == "settle"}
pool = pol["pool0"]; n_ok = n_ev = 0; bad = []; tot_ref = tot_tr = 0.0; cap_same = 0
for rs in rounds:
    g = U[U.rs == rs]
    dec = (g.t_dec.values.astype(float).tolist(), g.k.values.astype(int).tolist(), g.p.values.tolist())
    cap = caps.get(rs, pol["cap_frac"] * pool)
    if abs(cap - pol["cap_frac"] * pool) < 1e-12: cap_same += 1
    log = []
    r = v3b_sim.sim_round(P, rs, pol["prm"], dict(model_pool="model", bot_pool=None, stop_pools=set()), model_dec=dec, cfg=pol["cfg"], liq=True,
                          log=log, stress=pol["stress"], round_cap=cap)
    ref = [(round(e["t"], 3), e["act"], e["side"], e["sh"], e["px"]) for e in log]
    trl = tr_fill.get(rs, [])
    n_ev += max(len(ref), len(trl)); n_ok += sum(1 for a, b in zip(ref, trl) if a == b)
    tp = tr_pnl.get(rs, (0.0, None, None, None))
    if ref != trl or abs(tp[0] - r["pnl"]) > 1e-9: bad.append((rs, ref, trl, r["pnl"], tp))
    tot_ref += r["pnl"]; tot_tr += tp[0]; pool += r["pnl"]
    if tp[1] is not None and tp[1] != P.rounds[rs]["up"]: lines.append("round %d: trader outcome %s (%s) vs Prep outcome %s" % (rs, tp[1], tp[2], P.rounds[rs]["up"]))
lines.append("events (fills and misses, trade by trade): %d of %d identical | rounds with any difference: %d | PnL reference %+.6f trader %+.6f USD" % (
    n_ok, n_ev, len(bad), tot_ref, tot_tr))
lines.append("caps: %d of %d rounds equal to 5 %% of the pool after the previous round settled (the simulator's compounding rule)" % (cap_same, len(rounds)))
for b in bad[:5]: lines.append("DIFF round %d: ref %s | trader %s | pnl ref %+.6f trader %s" % b)
print("\n".join(lines)); open(os.path.join(out_dir, "parity.out"), "w").write("\n".join(lines) + "\n")
