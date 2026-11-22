"""Where the 09:30:00 one-minute quote comes from: one SPXW 0DTE contract (2024-03-14, 5145 C), ticks from 09:25 and
from 04:00, and 1-second rows from 09:29, against the 1-minute rows saved by resolution_ladder.py.
Usage: resolution_open.py LADDER_OUT_DIR"""
import os, polars as pl, thetadatadx as tdx
import sys; L = sys.argv[1]
KEY = next(l.split("=",1)[1].strip().strip('"\'') for l in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")) if l.startswith("THETADATA_API_KEY="))
cols = ["ms_of_day","bid_size","bid","ask_size","ask","bid_exchange","ask_exchange"]
m1 = pl.read_parquet(f"{L}/chain_1m.parquet").filter((pl.col("strike")==5145)&(pl.col("right")=="C"))
print("1m rows 09:30-09:31:\n", m1.filter(pl.col("ms_of_day")<=34_260_000).select(cols))
c = tdx.Client(api_key=KEY).market_data
for st in ["09:25:00", "04:00:00"]:
    t = c.option_history_quote("SPXW","20240314",date="20240314",strike="5145",right="C",interval="tick",start_time=st,end_time="09:30:10").to_polars()
    print(f"\nticks from {st} to 09:30:10: {t.height} rows; first {t['ms_of_day'].min() if t.height else None}")
    print(t.select(cols).head(4)); print(t.filter(pl.col("ms_of_day")<34_200_000).select(cols).tail(3))
for st in ["09:29:00"]:
    t = c.option_history_quote("SPXW","20240314",date="20240314",strike="5145",right="C",interval="1s",start_time=st,end_time="09:30:05").to_polars()
    print(f"\n1s from {st}: {t.height} rows"); print(t.select(cols).tail(8))
