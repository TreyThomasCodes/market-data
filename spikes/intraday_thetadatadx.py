"""SPXW 0DTE on 2024-03-14, 1-minute quotes and first-order Greeks, through ThetaDataDx. Sequential, one login.
Usage: tddx_probe.py OUT_DIR [repeats]"""
import sys, time
import polars as pl
import thetadatadx as tdx

out = sys.argv[1]; reps = int(sys.argv[2]) if len(sys.argv) > 2 else 1
D = "20240314"
key = next(l.split("=", 1)[1].strip().strip('"\'') for l in open("/opt/projects/market-data/.env")
           if l.startswith("THETADATA_API_KEY="))
t = time.perf_counter(); c = tdx.Client(api_key=key); print(f"dx login {time.perf_counter()-t:.2f}s", flush=True)
m = c.market_data
for name, fn in [("quote", m.option_history_quote), ("greeks_first_order", m.option_history_greeks_first_order)]:
    for i in range(reps):
        t = time.perf_counter()
        r = fn("SPXW", D, date=D, interval="1m")
        t1 = time.perf_counter()
        df = r.to_polars()
        t2 = time.perf_counter()
        print(f"dx {name:<20} fetch {t1-t:5.2f}s  to_polars {t2-t1:5.2f}s  {df.shape}", flush=True)
    df.write_parquet(f"{out}/dx_{name}.parquet")
