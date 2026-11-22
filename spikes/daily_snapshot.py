"""Can a multi-day interval pull with a one-minute window replace the EOD endpoints for daily chain data?

SPY monthly 2024-04-19, every strike, trade dates 2024-03-01..2024-03-28, production, ThetaDataDx 0.9.0, sequential.
Compares time and rows: greeks EOD over the range vs quote and first-order Greeks at 1m for 16:00 only over the range.
Usage: daily_snapshot.py
"""
import os, time
import polars as pl
import thetadatadx as tdx

ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
m = tdx.Client(api_key=KEY).market_data
R, E = dict(start_date="20240301", end_date="20240328"), "20240419"
W = dict(interval="1m", start_time="16:00:00", end_time="16:00:00")
out = {}
for name, fn, kw in [("greeks EOD", m.option_history_greeks_eod, {}),
                     ("quote 1m 16:00 only", m.option_history_quote, W),
                     ("greeks 1m 16:00 only", m.option_history_greeks_first_order, W),
                     ("ohlc 15m 00:00-23:59", m.option_history_ohlc, dict(interval="15m", start_time="00:00:00", end_time="23:59:59")),
                     ("ohlc 1m 00:00-23:59", m.option_history_ohlc, dict(interval="1m", start_time="00:00:00", end_time="23:59:59"))]:
    t = time.perf_counter()
    df = fn("SPY", E, timeout_ms=600_000, **R, **kw).to_polars()
    dt = time.perf_counter() - t
    per = df.group_by("date").len()
    times = sorted(df["ms_of_day"].unique().to_list())[:6] if "ms_of_day" in df.columns else []
    out[name] = df
    print(f"{name:<24} {dt:6.2f}s  rows {df.height:>7,}  dates {per.height}  contracts/date ~{per['len'].median():.0f}  "
          f"ms_of_day values {times}", flush=True)

# Volume: OHLC bars summed per contract-day over 00:00-17:15 should equal greeks EOD volume (EOD window, measured earlier).
k = ["date", "strike", "right"]
eod = out["greeks EOD"].select(k + ["volume"])
for name in ["ohlc 15m 00:00-23:59", "ohlc 1m 00:00-23:59"]:
    b = out[name].filter(pl.col("ms_of_day") < 62_100_000).group_by(k).agg(pl.col("volume").sum().alias("v"))
    j = eod.join(b, on=k, how="left").with_columns(pl.col("v").fill_null(0))
    print(f"{name}: contract-days {j.height}, volume equal to EOD {j.filter(pl.col('volume') == pl.col('v')).height}, "
          f"total EOD {j['volume'].sum():,} vs bars {j['v'].sum():,}")
