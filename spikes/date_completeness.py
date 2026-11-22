"""When is a date's data complete? Watch one trading day as it ages.

At each check time (ET), logs in once to production (ThetaDataDx 0.9.0), pulls a fixed set of requests for the target
date and records row counts, last timestamps and a checksum per request, or the error. Changes between checks show
when the data stops moving; errors show the documented 00:00-01:45 ET "previous day isn't available" window.
Runs until the last check time, sleeping in between. Usage: date_completeness.py YYYYMMDD OUT.jsonl [now]   (now: one check immediately, to test)
"""
import datetime as dt, json, os, sys, time, zoneinfo
import polars as pl
import thetadatadx as tdx

D, OUT = sys.argv[1], sys.argv[2]
ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
ET = zoneinfo.ZoneInfo("America/New_York")
T = 600_000
day = dt.datetime.strptime(D, "%Y%m%d").date()
nxt = day + dt.timedelta(1)
CHECKS = [dt.datetime.combine(day, dt.time(h, mi), ET) for h, mi in
          [(10, 30), (15, 0), (16, 20), (17, 5), (17, 30), (20, 30), (23, 45)]] + \
         [dt.datetime.combine(nxt, dt.time(h, mi), ET) for h, mi in
          [(0, 15), (0, 45), (1, 15), (1, 40), (1, 50), (2, 30), (6, 0), (9, 0), (12, 0), (18, 0)]] + \
         [dt.datetime.combine(nxt + dt.timedelta(1), dt.time(9, 0), ET)]
state = {}  # strike choices, fixed at the first check that can make them


def summary(df):
    if df.height == 0:
        return {"rows": 0}
    out = {"rows": df.height, "last_ms": int(df["ms_of_day"].max()), "first_ms": int(df["ms_of_day"].min())}
    num = [c for c in ["bid", "ask", "price", "size", "bid_size", "ask_size", "volume"] if c in df.columns]
    out["checksum"] = round(float(sum(df[c].cast(pl.Float64).sum() for c in num)), 4)
    return out


def atm(m, root, exp):
    ch = m.option_history_quote(root, exp, date=D, interval="1m", start_time="10:00:00", end_time="10:00:00",
                                timeout_ms=T).to_polars()
    piv = (ch.filter(pl.col("bid") > 0).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
             .pivot(on="right", index="strike", values="mid").drop_nulls())
    k = piv.with_columns(x=(pl.col("C") - pl.col("P")).abs()).sort("x")["strike"][0]
    return str(int(k)) if float(k).is_integer() else str(k)


def check(at):
    rec = {"check_et": at.isoformat(), "started": dt.datetime.now(ET).isoformat()}
    try:
        m = tdx.Client(api_key=KEY).market_data
    except Exception as e:
        rec["login"] = f"{type(e).__name__}: {str(e)[:200]}"; return rec
    if "spxw" not in state:
        try:
            exps = sorted(int(str(x).replace("-", "")) for x in m.option_list_expirations("SPXW", timeout_ms=T))
            spy = sorted(int(str(x).replace("-", "")) for x in m.option_list_expirations("SPY", timeout_ms=T))
            state["spxw_exp"] = str(min(e for e in exps if e > int(D)))
            state["spy_exp"] = str(min(e for e in spy if e > int(D)))
            state["spxw"] = atm(m, "SPXW", state["spxw_exp"]); state["spy"] = atm(m, "SPY", state["spy_exp"])
        except Exception as e:
            rec["setup"] = f"{type(e).__name__}: {str(e)[:200]}"
    rec["contracts"] = {k: v for k, v in state.items()}
    full = dict(start_time="00:00:00", end_time="23:59:59")
    reqs = {
        "spxw_0dte_chain_1m_rth": lambda: m.option_history_quote("SPXW", D, date=D, interval="1m"),
        "spxw_0dte_ohlc_15m_rth": lambda: m.option_history_ohlc("SPXW", D, date=D, interval="15m"),
        "list_contracts_trade": lambda: m.option_list_contracts("trade", D, symbol="SPXW"),
    }
    if "spxw" in state:
        c = dict(date=D, strike=state["spxw"], right="call")
        reqs |= {
            "spxw_next_ticks_full_day": lambda: m.option_history_quote("SPXW", state["spxw_exp"], interval="tick", **c, **full),
            "spxw_next_trades_full_day": lambda: m.option_history_trade_quote("SPXW", state["spxw_exp"], **c, **full),
            "spy_next_ticks_rth": lambda: m.option_history_quote("SPY", state["spy_exp"], interval="tick", date=D,
                                                                strike=state["spy"], right="call"),
        }
    for name, fn in reqs.items():
        t = time.perf_counter()
        try:
            r = fn()
            df = r.to_polars() if hasattr(r, "to_polars") else pl.DataFrame({"x": list(r)})
            s = summary(df) if "ms_of_day" in df.columns else {"rows": df.height}
        except tdx.NotFoundError:
            s = {"rows": 0, "error": "NotFound"}
        except Exception as e:
            s = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
        s["secs"] = round(time.perf_counter() - t, 2)
        rec[name] = s
    return rec


def main():
    if sys.argv[3:] == ["now"]:
        rec = check(dt.datetime.now(ET))
        print(json.dumps(rec, indent=1)); return
    for at in CHECKS:
        now = dt.datetime.now(ET)
        if at < now - dt.timedelta(minutes=5):
            continue
        time.sleep(max(0, (at - now).total_seconds()))
        rec = check(at)
        with open(OUT, "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(at.strftime("%a %H:%M ET"), {k: (v.get("rows"), v.get("error")) if isinstance(v, dict) and k not in
              ("contracts",) else v for k, v in rec.items() if k not in ("check_et", "started")}, flush=True)


if __name__ == "__main__":
    main()
