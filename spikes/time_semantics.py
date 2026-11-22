"""What ThetaData's bar, Greeks and end-of-day timestamps mean, checked against ticks and trades.

ThetaDataDx 0.9.0, production, one request at a time. Usage: time_semantics.py OUT_DIR
  1. option OHLC 1m: rebuild bars from trade_quote under [t, t+1m) and under (t-1m, t]; which matches?
  2. interval Greeks 1m: bid/ask against the 1m quote row at the same ts; underlying_ms_of_day against ts.
  3. option EOD and Greeks EOD: report time, last trade, and which tick quote the bid/ask equal.
"""
import os, sys
import polars as pl
import thetadatadx as tdx

OUT = sys.argv[1]; os.makedirs(OUT, exist_ok=True)
ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
T = 600_000
H = lambda h, m=0, s=0: ((h * 60 + m) * 60 + s) * 1000
# (root, trade date, expiration, strike): busy contracts, expiring and not.
CASES = [("SPY", "20240315", "20240315", "510"), ("SPXW", "20240314", "20240315", "5145"),
         ("SPXW", "20240314", "20240314", "5145")]


def fmt(ms):
    if ms is None: return "-"
    s, ms = divmod(int(ms), 1000); h, s = divmod(s, 3600); m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def get(name, fn, *a, **kw):
    path = f"{OUT}/{name}.parquet"
    if os.path.exists(path):
        return pl.read_parquet(path)
    try:
        df = fn(*a, timeout_ms=T, **kw).to_polars()
    except tdx.NotFoundError:
        df = pl.DataFrame()
    df.write_parquet(path)
    return df


def bars_from_trades(tr, label):
    """1-minute bars from trades. label 'start': [t, t+1m) labelled t. 'end': (t-1m, t] labelled t."""
    if label == "start":
        k = (pl.col("ms_of_day") // 60_000) * 60_000
    else:
        k = ((pl.col("ms_of_day") + 59_999) // 60_000) * 60_000
    return (tr.sort(["ms_of_day", "sequence"]).group_by(k.alias("ms_of_day"), maintain_order=True)
              .agg(pl.col("price").first().alias("open"), pl.col("price").max().alias("high"),
                   pl.col("price").min().alias("low"), pl.col("price").last().alias("close"),
                   pl.col("size").sum().alias("volume"), pl.len().alias("count")))


def main():
    m = tdx.Client(api_key=KEY).market_data
    for root, d, exp, k in CASES:
        tag = f"{root}_{d}_{exp}_{k}"
        print(f"\n== {root} {d} exp {exp} {k} C", flush=True)
        kw = dict(date=d, strike=k, right="call")
        full = dict(start_time="00:00:00", end_time="23:59:59")
        tr = get(f"{tag}_trades", m.option_history_trade_quote, root, exp, **kw, **full)
        ohlc = get(f"{tag}_ohlc1m", m.option_history_ohlc, root, exp, interval="1m", **kw, **full)
        q1m = get(f"{tag}_quote1m", m.option_history_quote, root, exp, interval="1m", **kw)
        g1m = get(f"{tag}_greeks1m", m.option_history_greeks_first_order, root, exp, interval="1m", **kw)
        ticks = get(f"{tag}_ticks", m.option_history_quote, root, exp, interval="tick", **kw, **full)
        eod = get(f"{tag}_eod", m.option_history_eod, root, exp, start_date=d, end_date=d, strike=k, right="call")
        geod = get(f"{tag}_greeks_eod", m.option_history_greeks_eod, root, exp, start_date=d, end_date=d,
                   strike=k, right="call")
        print(f"  rows: trades {tr.height:,}, ohlc 1m {ohlc.height:,}, quote 1m {q1m.height:,}, greeks 1m "
              f"{g1m.height:,}, ticks {ticks.height:,}, eod {eod.height}, greeks eod {geod.height}")

        # 1. OHLC labels
        o = ohlc.filter(pl.col("count") > 0)
        print(f"  ohlc: {o.height} bars with trades; first {fmt(o['ms_of_day'].min())}, last {fmt(o['ms_of_day'].max())}")
        for label in ["start", "end"]:
            b = bars_from_trades(tr, label)
            j = o.join(b, on="ms_of_day", how="left", suffix="_t")
            cols = ["open", "high", "low", "close", "volume", "count"]
            exact = j.filter(pl.all_horizontal([pl.col(c) == pl.col(f"{c}_t") for c in cols])).height
            per = {c: j.filter(pl.col(c) == pl.col(f"{c}_t")).height for c in cols}
            print(f"    trades bucketed by {label:<5}: bars matching on all of OHLCV+count {exact}/{o.height}; "
                  f"per column {per}")
        b = bars_from_trades(tr, "start")
        j = o.join(b, on="ms_of_day", how="left", suffix="_t")
        bad = j.filter(pl.any_horizontal([pl.col(c) != pl.col(f"{c}_t") for c in ["open", "close", "volume", "count"]]))
        if bad.height:
            print("    first mismatches under start-label:")
            print(bad.select(["ms_of_day", "open", "open_t", "close", "close_t", "volume", "volume_t", "count",
                              "count_t"]).head(5))

        # 2. Greeks snapshot
        if g1m.height:
            j = g1m.join(q1m, on="ms_of_day", how="inner", suffix="_q")
            same = j.filter((pl.col("bid") == pl.col("bid_q")) & (pl.col("ask") == pl.col("ask_q"))).height
            lag = (g1m["ms_of_day"] - g1m["underlying_ms_of_day"])
            print(f"  greeks 1m: bid/ask equal to the 1m quote at the same ts in {same}/{j.height} rows; "
                  f"ts - underlying_ms_of_day: min {lag.min()}, median {lag.median()}, max {lag.max()} ms; "
                  f"underlying_date differs from date in {g1m.filter(pl.col('underlying_date') != pl.col('date')).height}")
            diff = j.filter((pl.col("bid") != pl.col("bid_q")) | (pl.col("ask") != pl.col("ask_q")))
            if diff.height:
                print(diff.select(["ms_of_day", "bid", "bid_q", "ask", "ask_q"]).head(5))

        # 3. EOD
        for name, e in [("eod", eod), ("greeks eod", geod)]:
            if not e.height:
                print(f"  {name}: none"); continue
            r = e.row(0, named=True)
            times = {c: fmt(r[c]) for c in ["created_ms_of_day", "last_trade_ms_of_day", "ms_of_day"] if c in r}
            print(f"  {name}: {times}; close {r.get('close')}, bid {r['bid_size']}x{r['bid']} / ask {r['ask_size']}x{r['ask']}"
                  + (f", underlying {r.get('underlying_price')}" if 'underlying_price' in r else ""))
            match = ticks.filter((pl.col("bid") == r["bid"]) & (pl.col("ask") == r["ask"]) &
                                 (pl.col("bid_size") == r["bid_size"]) & (pl.col("ask_size") == r["ask_size"]))
            print(f"    ticks with that exact quote: {match.height}, last at {fmt(match['ms_of_day'].max()) if match.height else '-'}")
            for at in [H(16), H(16, 15), H(17), H(17, 15)]:
                last = ticks.filter(pl.col("ms_of_day") <= at).tail(1)
                if last.height:
                    l = last.row(0, named=True)
                    print(f"    last tick <= {fmt(at)[:5]}: {fmt(l['ms_of_day'])} {l['bid_size']}x{l['bid']} / {l['ask_size']}x{l['ask']}")
            if tr.height:
                lt = tr.sort(["ms_of_day", "sequence"]).tail(1).row(0, named=True)
                print(f"    last trade: {fmt(lt['ms_of_day'])} @ {lt['price']}")


if __name__ == "__main__":
    main()
