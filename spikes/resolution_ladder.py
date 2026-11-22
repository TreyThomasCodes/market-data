"""Can one quote resolution be answered from a finer one? And what does each resolution cost to fetch?

SPXW 0DTE on one day, through ThetaDataDx 0.9.0 against production. One login, one request in flight at a time.
Usage: resolution_ladder.py OUT_DIR [YYYYMMDD]

Fetches the whole chain at 5m, 1m and 1s, then one near-the-money call at 1s, tick quotes and trade_quote.
Checks:
  1. 5m chain rows   == 1m chain rows at the same timestamps?
  2. 1m chain rows   == 1s chain rows at the same timestamps?
  3. 1s contract rows == the last tick at or before (and strictly before) each second?
  4. 1s contract fetch == that contract's rows in the 1s chain fetch?
Also reports what each resolution returns at 09:30:00, without changing it.
"""
import os, sys, time
import polars as pl
import thetadatadx as tdx

OUT = sys.argv[1]; D = sys.argv[2] if len(sys.argv) > 2 else "20240314"
os.makedirs(OUT, exist_ok=True)
ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
TIMEOUT = 30 * 60 * 1000
K = ["strike", "right", "ms_of_day"]
VALS = ["bid_size", "bid_exchange", "bid", "bid_condition", "ask_size", "ask_exchange", "ask", "ask_condition"]


def fetch(name, fn, *a, **kw):
    path = f"{OUT}/{name}.parquet"
    if os.path.exists(path):  # saved by an earlier run: reuse rather than fetch again
        df = pl.read_parquet(path); print(f"{name:<22} (saved earlier)  rows {df.height:>11,}", flush=True)
        return df
    t = time.perf_counter()
    r = fn(*a, timeout_ms=TIMEOUT, **kw)
    t1 = time.perf_counter()
    df = r.to_polars()
    t2 = time.perf_counter()
    df.write_parquet(f"{OUT}/{name}.parquet")
    size = os.path.getsize(f"{OUT}/{name}.parquet")
    print(f"{name:<22} fetch {t1-t:7.2f}s  to_polars {t2-t1:5.2f}s  rows {df.height:>11,}  "
          f"parquet {size/1e6:8.1f} MB  ({size/max(df.height,1):.1f} B/row)", flush=True)
    return df


def compare(label, coarse, fine):
    """Every coarse row against the fine row with the same key."""
    j = coarse.join(fine, on=K, how="left", suffix="_f")
    missing = j.filter(pl.col("bid_f").is_null() & pl.col("ask_f").is_null()).height
    differ = j.filter(pl.any_horizontal([pl.col(c).ne_missing(pl.col(f"{c}_f")) for c in VALS]))
    print(f"\n[{label}] coarse rows {coarse.height:,}; no matching fine row {missing:,}; "
          f"rows differing on {VALS}: {differ.height:,}")
    if differ.height:
        bad = differ.select([pl.col(c).ne_missing(pl.col(f'{c}_f')).sum().alias(c) for c in VALS])
        print("  differing values per column:", bad.to_dicts()[0])
        print("  differing rows by ms_of_day:", differ.group_by("ms_of_day").len().sort("ms_of_day").head(10).rows())
        print(differ.select(K + [c for v in ["bid", "ask", "bid_size", "ask_size"] for c in (v, f"{v}_f")]).head(8))
    return differ


def at_open(label, df):
    o = df.filter(pl.col("ms_of_day") == 34_200_000)
    z = o.filter((pl.col("bid") == 0) & (pl.col("ask") == 0)).height
    first = df["ms_of_day"].min() if df.height else None
    print(f"  {label:<20} rows at 09:30:00: {o.height:>6,}  with bid=ask=0: {z:>6,}  first ms_of_day: {first}")


def main():
    c = tdx.Client(api_key=KEY)
    m = c.market_data
    q = m.option_history_quote

    m5 = fetch("chain_5m", q, "SPXW", D, date=D, interval="5m")
    print("schema:", dict(m5.schema)); print(m5.head(3))
    m1 = fetch("chain_1m", q, "SPXW", D, date=D, interval="1m")
    s1 = fetch("chain_1s", q, "SPXW", D, date=D, interval="1s")

    # Near-the-money call: the strike where call and put mids are closest at 10:00 (1m chain).
    t10 = m1.filter(pl.col("ms_of_day") == 36_000_000).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
    piv = t10.pivot(on="right", index="strike", values="mid")
    rights = [r for r in piv.columns if r != "strike"]
    strike = piv.with_columns(d=(pl.col(rights[0]) - pl.col(rights[1])).abs()).sort("d")["strike"][0]
    call = next(r for r in m1["right"].unique().to_list() if str(r).upper().startswith("C"))
    print(f"\ncontract: SPXW {D} {strike} {call}  (rights seen: {rights})")
    sk = str(int(strike)) if float(strike).is_integer() else str(strike)

    c1s = fetch("contract_1s", q, "SPXW", D, date=D, strike=sk, right=call, interval="1s")
    ct = fetch("contract_tick", q, "SPXW", D, date=D, strike=sk, right=call, interval="tick")
    tq = fetch("contract_trade_quote", m.option_history_trade_quote, "SPXW", D, date=D, strike=sk, right=call)

    compare("1: 5m chain vs 1m chain", m5, m1)
    compare("2: 1m chain vs 1s chain", m1, s1)
    compare("4: 1s contract vs 1s chain", c1s, s1)

    # 3: as-of join of ticks onto each second, inclusive (tick ms <= t) and strict (tick ms < t).
    ticks = ct.sort("ms_of_day").select(["ms_of_day"] + VALS).with_columns(tick_ms=pl.col("ms_of_day"))
    grid = c1s.sort("ms_of_day")
    for name, strict in [("<=", False), ("<", True)]:
        g = grid.with_columns((pl.col("ms_of_day") - (1 if strict else 0)).alias("probe_ms"))
        a = g.join_asof(ticks.drop("ms_of_day"), left_on="probe_ms", right_on="tick_ms", strategy="backward",
                        suffix="_f").drop("probe_ms")
        differ = a.filter(pl.any_horizontal([pl.col(c).ne_missing(pl.col(f"{c}_f")) for c in VALS]))
        print(f"\n[3: 1s contract vs last tick {name} t] seconds {a.height:,}; differing {differ.height:,}; "
              f"no tick before {a.filter(pl.col('tick_ms').is_null()).height:,}")
        if differ.height:
            print("  differing seconds by ms_of_day:", differ["ms_of_day"].head(10).to_list())
            print(differ.select(["ms_of_day", "tick_ms", "bid", "bid_f", "ask", "ask_f", "bid_size", "bid_size_f"]).head(8))

    # Ticks sharing a millisecond: an as-of join can't order them.
    dup = ct.group_by("ms_of_day").len().filter(pl.col("len") > 1)
    print(f"\ntick quotes: {ct.height:,} rows, {dup.height:,} milliseconds with more than one tick "
          f"(max {dup['len'].max() if dup.height else 1})")
    print(f"tick ms_of_day range: {ct['ms_of_day'].min()} .. {ct['ms_of_day'].max()}")
    print(f"trade_quote: {tq.height:,} trades; columns {tq.columns}")

    print("\nAt the open:")
    for label, df in [("chain 5m", m5), ("chain 1m", m1), ("chain 1s", s1), ("contract 1s", c1s),
                      ("contract tick", ct)]:
        at_open(label, df)


if __name__ == "__main__":
    main()
