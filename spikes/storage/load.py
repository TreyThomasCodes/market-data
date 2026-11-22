"""Load the SPXW one-minute whole-chain quote files into three stores. Source files: the BENCH_SRC_GLOB pattern."""
import glob, io, os, sys, time, gzip, re
import polars as pl
import psycopg

OUT = os.path.dirname(os.path.abspath(__file__))
files = sorted(glob.glob(os.environ["BENCH_SRC_GLOB"]))
print(len(files), "files", sum(os.path.getsize(f) for f in files) / 1e6, "MB gz")

SCHEMA = {"symbol": pl.Utf8, "expiration": pl.Date, "strike": pl.Float64, "right": pl.Utf8,
          "timestamp": pl.Datetime("ms"), "bid_size": pl.Int32, "bid_exchange": pl.Int16, "bid": pl.Float64,
          "bid_condition": pl.Int16, "ask_size": pl.Int32, "ask_exchange": pl.Int16, "ask": pl.Float64,
          "ask_condition": pl.Int16}

t = time.time()
parts = []
for f in files:
    d = re.search(r"date-(\d{8})", f).group(1)
    df = pl.read_csv(gzip.decompress(open(f, "rb").read()), schema=SCHEMA)
    df = df.with_columns(pl.lit(pl.Series([d]).str.strptime(pl.Date, "%Y%m%d")[0]).alias("trade_date"),
                         pl.col("right").str.slice(0, 1))
    parts.append(df.drop("symbol"))
df = pl.concat(parts)
print("null ts rows:", df["timestamp"].null_count())
df = df.filter(pl.col("timestamp").is_not_null()).sort(["trade_date", "expiration", "strike", "right", "timestamp"])
print(f"parsed {df.height:,} rows in {time.time()-t:.0f}s")

# P1: one file per fetch unit (trade_date, expiration)
t = time.time()
for (d, e), g in df.group_by(["trade_date", "expiration"]):
    p = f"{OUT}/p1/trade_date={d}"
    os.makedirs(p, exist_ok=True)
    g.write_parquet(f"{p}/exp={e}.parquet", compression="zstd")
print(f"P1 written in {time.time()-t:.0f}s")

# P2: one file per month, sorted, 128k-row row groups
t = time.time()
os.makedirs(f"{OUT}/p2", exist_ok=True)
for (m,), g in df.group_by([pl.col("trade_date").dt.strftime("%Y-%m")]):
    g.sort(["trade_date", "expiration", "strike", "right", "timestamp"]).write_parquet(
        f"{OUT}/p2/{m}.parquet", compression="zstd", row_group_size=131072, statistics=True)
print(f"P2 written in {time.time()-t:.0f}s")

# Timescale
t = time.time()
with psycopg.connect("host=127.0.0.1 port=55432 user=postgres password=bench", autocommit=True) as c:
    c.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    c.execute("DROP TABLE IF EXISTS q")
    c.execute("""CREATE TABLE q (trade_date date, expiration date, strike double precision, "right" char(1),
        ts timestamp not null, bid_size int, bid_exchange smallint, bid double precision, bid_condition smallint,
        ask_size int, ask_exchange smallint, ask double precision, ask_condition smallint)""")
    c.execute("SELECT create_hypertable('q', 'ts', chunk_time_interval => interval '7 days')")
    cols = ["trade_date", "expiration", "strike", "right", "timestamp", "bid_size", "bid_exchange", "bid",
            "bid_condition", "ask_size", "ask_exchange", "ask", "ask_condition"]
    with c.cursor().copy('COPY q FROM STDIN (FORMAT csv)') as cp:
        for i in range(0, df.height, 1_000_000):
            buf = io.BytesIO()
            df.slice(i, 1_000_000).select(cols).write_csv(buf, include_header=False)
            cp.write(buf.getvalue())
    print(f"TSDB copy {time.time()-t:.0f}s")
    t = time.time()
    c.execute('CREATE INDEX ON q (expiration, ts)')
    c.execute('CREATE INDEX ON q (expiration, strike, "right", ts)')
    c.execute("ANALYZE q")
    print(f"TSDB index {time.time()-t:.0f}s")
    print("TSDB size", c.execute("SELECT pg_size_pretty(hypertable_size('q'))").fetchone()[0])
