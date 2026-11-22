"""Which session windows does ThetaData return option quotes and trades for?

For each (root, trade date, expiration) case, picks the near-the-money call from the 1-minute chain at 10:00, then pulls
its tick quotes and trade_quote over the whole calendar day (00:00:00-23:59:59) and counts rows per window:
overnight GTH (00:00-09:15, 09:15-09:25, 09:25-09:30), RTH, 16:00-16:15, curb 16:15-17:00, 17:00-20:15, evening GTH
20:15-24:00. Production, through ThetaDataDx 0.9.0, one request at a time.
Usage: session_windows.py OUT_DIR
"""
import os, sys
import polars as pl
import thetadatadx as tdx

OUT = sys.argv[1]; os.makedirs(OUT, exist_ok=True)
ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
H = lambda h, m=0, s=0: ((h * 60 + m) * 60 + s) * 1000
WINDOWS = [("00:00-09:15", H(0), H(9, 15)), ("09:15-09:25", H(9, 15), H(9, 25)), ("09:25-09:30", H(9, 25), H(9, 30)),
           ("09:30-16:00", H(9, 30), H(16)), ("16:00-16:15", H(16), H(16, 15)), ("16:15-17:00", H(16, 15), H(17)),
           ("17:00-20:15", H(17), H(20, 15)), ("20:15-24:00", H(20, 15), H(24))]
# (root, trade date, expiration): next-day expiry so the contract is listed all day; 0DTE to see the expiry-day close.
CASES = [("SPXW", "20240314", "20240315"), ("SPXW", "20240314", "20240314"),
         ("SPXW", "20261119", "20261120"), ("SPXW", "20261120", "20261120"),
         ("SPY", "20240314", "20240315"), ("SPY", "20240315", "20240315"),
         ("SPY", "20261119", "20261120"), ("SPY", "20261120", "20261120")]


def fmt(ms):
    if ms is None: return "-"
    s, ms = divmod(int(ms), 1000); h, s = divmod(s, 3600); m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def windows(df):
    out = []
    for name, a, b in WINDOWS:
        w = df.filter((pl.col("ms_of_day") >= a) & (pl.col("ms_of_day") < b))
        z = w.filter((pl.col("bid") == 0) & (pl.col("ask") == 0)).height if "bid" in w.columns else 0
        out.append(f"{name} {w.height:>8,}" + (f" (0/0: {z:,})" if z else "") +
                   (f" [{fmt(w['ms_of_day'].min())}..{fmt(w['ms_of_day'].max())}]" if w.height else ""))
    return out


def main():
    m = tdx.Client(api_key=KEY).market_data
    for root, d, exp in CASES:
        print(f"\n== {root} trade date {d}, expiration {exp}", flush=True)
        try:
            ch = m.option_history_quote(root, exp, date=d, interval="1m", start_time="10:00:00",
                                        end_time="10:00:00").to_polars()
        except Exception as e:
            print(f"  chain at 10:00: {type(e).__name__}: {str(e)[:200]}"); continue
        # Near the money: the strike where call and put mids are closest at 10:00.
        piv = (ch.filter(pl.col("bid") > 0).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
                 .pivot(on="right", index="strike", values="mid").drop_nulls())
        strike = piv.with_columns(d=(pl.col("C") - pl.col("P")).abs()).sort("d")["strike"][0]
        target = piv.filter(pl.col("strike") == strike)["C"][0]
        sk = str(int(strike)) if float(strike).is_integer() else str(strike)
        print(f"  contract {root} {exp} {sk} C (mid {target:.2f} at 10:00)")
        for kind, fn, kw in [("quote ticks", m.option_history_quote, dict(interval="tick")),
                             ("trade_quote", m.option_history_trade_quote, {})]:
            try:
                df = fn(root, exp, date=d, strike=sk, right="call", start_time="00:00:00", end_time="23:59:59",
                        timeout_ms=600_000, **kw).to_polars()
            except Exception as e:
                print(f"  {kind}: {type(e).__name__}: {str(e)[:200]}"); continue
            df.write_parquet(f"{OUT}/{root}_{d}_{exp}_{kind.replace(' ', '_')}.parquet")
            dates = df["date"].unique().to_list() if "date" in df.columns else []
            print(f"  {kind}: {df.height:,} rows, first {fmt(df['ms_of_day'].min() if df.height else None)}, "
                  f"last {fmt(df['ms_of_day'].max() if df.height else None)}, date column {dates}")
            for line in windows(df):
                print(f"    {line}")


if __name__ == "__main__":
    main()
