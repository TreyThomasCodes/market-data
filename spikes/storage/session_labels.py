"""trade_date and session: stored on every row (A) or joined from a sessions table at read time (B)?

Data: the 267M-row SPXW one-minute chain benchmark (p2/, one Parquet file per month). Run from a
scratch folder. DuckDB 2.0.1 on plain Parquet (catalog overhead is the same for both layouts, so left out).
Usage: session_labels.py P2_DIR WORK_DIR

A: ts TIMESTAMPTZ, trade_date, session stored; files per trade-date month; sorted trade_date, expiration, strike,
   right, ts.
B: ts only; files per ET calendar month; sorted the same way on the ET calendar date. Labels come from a sessions
   table (gth 20:15 prev day - 09:25, rth 09:30 - 16:15, curb 16:15 - 17:00, one set per trade date) by ASOF join,
   snapshot rule ts in (start, end].
"""
import os, statistics, sys, time
import duckdb

P2, WORK = sys.argv[1], sys.argv[2]
os.makedirs(WORK, exist_ok=True)
A, B = f"{WORK}/a", f"{WORK}/b"
REPS = 5


def con():
    c = duckdb.connect()
    c.execute(f"SET TimeZone = 'America/New_York'; SET memory_limit = '20GB'; SET threads = 16; SET temp_directory = '{WORK}/tmp'")
    return c


def build():
    c = con()
    src = f"read_parquet('{P2}/*.parquet')"
    days = c.execute(f"SELECT DISTINCT trade_date FROM {src} ORDER BY 1").fetchall()
    # Sessions: one row each per trade date. Previous-day evening for gth is approximated as the calendar day before.
    c.execute("CREATE TABLE sessions AS SELECT * FROM (VALUES (NULL::DATE, NULL::VARCHAR, NULL::TIMESTAMPTZ, "
              "NULL::TIMESTAMPTZ)) t(trade_date, session, start_ts, end_ts) WHERE false")
    c.execute(f"""INSERT INTO sessions
        SELECT d, s, (d + o_day + o_start)::TIMESTAMP::TIMESTAMPTZ, (d + e_day + o_end)::TIMESTAMP::TIMESTAMPTZ
        FROM (SELECT DISTINCT trade_date AS d FROM {src}),
             (VALUES ('gth', -1, INTERVAL 20 HOUR + INTERVAL 15 MINUTE, 0, INTERVAL 9 HOUR + INTERVAL 25 MINUTE),
                     ('rth',  0, INTERVAL 9 HOUR + INTERVAL 30 MINUTE, 0, INTERVAL 16 HOUR + INTERVAL 15 MINUTE),
                     ('curb', 0, INTERVAL 16 HOUR + INTERVAL 15 MINUTE, 0, INTERVAL 17 HOUR)) v(s, o_day, o_start, e_day, o_end)""")
    c.execute(f"COPY sessions TO '{WORK}/sessions.parquet'")
    print(f"sessions: {c.execute('SELECT count(*) FROM sessions').fetchone()[0]:,} rows for {len(days):,} trade dates")

    cols = ("expiration, strike, \"right\", bid_size, bid_exchange, bid, bid_condition, ask_size, ask_exchange, ask, "
            "ask_condition")
    for name, path, sel, part, order in [
        ("A", A, f"timestamp::TIMESTAMPTZ AS ts, trade_date, 'rth' AS session, {cols}, "
                 "year(trade_date) AS y, month(trade_date) AS m",
         "y, m", "trade_date, expiration, strike, \"right\", ts"),
        ("B", B, f"timestamp::TIMESTAMPTZ AS ts, {cols}, year(timestamp) AS y, month(timestamp) AS m",
         "y, m", "CAST(ts AS DATE), expiration, strike, \"right\", ts")]:
        if os.path.exists(path):
            print(f"{name}: exists"); continue
        t = time.perf_counter()
        c.execute(f"COPY (SELECT {sel} FROM {src} ORDER BY {order}) TO '{path}' "
                  f"(FORMAT parquet, PARTITION_BY ({part}), COMPRESSION zstd)")
        size = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(path) for f in fs)
        print(f"{name}: written in {time.perf_counter() - t:.0f}s, {size / 1e9:.2f} GB", flush=True)


def bench():
    c = con()
    c.execute(f"CREATE VIEW a AS SELECT * FROM read_parquet('{A}/**/*.parquet', hive_partitioning = true)")
    c.execute(f"CREATE VIEW b AS SELECT * FROM read_parquet('{B}/**/*.parquet', hive_partitioning = true)")
    c.execute(f"CREATE TABLE sessions AS SELECT * FROM '{WORK}/sessions.parquet'")
    # B with labels: ASOF join on start (snapshot rule: start < ts), then keep ts <= end.
    c.execute("""CREATE VIEW b_labelled AS
        SELECT s.trade_date, s.session, b.* FROM b ASOF JOIN sessions s ON b.ts > s.start_ts WHERE b.ts <= s.end_ts""")
    D = "2024-03-14"
    queries = {
        "1 lookup: one 0DTE chain at 12:00 on one day": (
            f"SELECT * FROM a WHERE trade_date = '{D}' AND expiration = '{D}' AND ts = '{D} 12:00:00'::TIMESTAMPTZ "
            f"AND y = 2024 AND m = 3",
            # B: the day's RTH bounds from sessions first (tiny), then a ts filter that prunes files and row groups.
            f"""WITH r AS (SELECT * FROM sessions WHERE trade_date = '{D}' AND session = 'rth')
                SELECT r.trade_date, r.session, b.* FROM b, r WHERE b.y = 2024 AND b.m = 3
                AND b.ts = '{D} 12:00:00'::TIMESTAMPTZ AND b.expiration = '{D}' AND b.ts > r.start_ts AND b.ts <= r.end_ts"""),
        "2 sweep: 0DTE chain at 12:00 every day, RTH": (
            "SELECT * FROM a WHERE expiration = trade_date AND session = 'rth' AND ts = (trade_date + TIME '12:00:00')::TIMESTAMPTZ",
            "SELECT * FROM b_labelled WHERE expiration = trade_date AND session = 'rth' AND ts = (trade_date + TIME '12:00:00')::TIMESTAMPTZ"),
        "3 month by day: all RTH rows for 2024-03": (
            "SELECT * FROM a WHERE y = 2024 AND m = 3 AND session = 'rth'",
            """SELECT * FROM b_labelled WHERE ts >= '2024-03-01'::TIMESTAMPTZ AND ts < '2024-04-01'::TIMESTAMPTZ
               AND session = 'rth'"""),
        "4 aggregate: rows and mean spread per trade date and session, all data": (
            "SELECT trade_date, session, count(*), avg(ask - bid) FROM a GROUP BY ALL",
            "SELECT trade_date, session, count(*), avg(ask - bid) FROM b_labelled GROUP BY ALL"),
    }
    for name, (qa, qb) in queries.items():
        out = []
        for label, q in [("A stored", qa), ("B joined", qb)]:
            n = c.sql(f"SELECT count(*) FROM ({q})").fetchall()[0][0]
            ts = []
            for _ in range(REPS):
                t = time.perf_counter(); c.sql(q).to_arrow_table(); ts.append(time.perf_counter() - t)
            out.append((label, n, statistics.median(ts), min(ts)))
        print(f"\n{name}")
        for label, n, med, lo in out:
            print(f"  {label:<9} rows {n:>12,}  median {med * 1000:9.1f} ms  best {lo * 1000:9.1f} ms", flush=True)
        if out[0][1] != out[1][1]:
            print("  ROW COUNTS DIFFER")

    # Cost of a calendar correction under A: rewrite one trade-date month.
    t = time.perf_counter()
    c.execute(f"COPY (SELECT * EXCLUDE (session), 'rth' AS session FROM a WHERE y = 2024 AND m = 3 "
              f"ORDER BY trade_date, expiration, strike, \"right\", ts) TO '{WORK}/rewrite.parquet' (COMPRESSION zstd)")
    print(f"\nA: rewriting one month (2024-03) after a calendar correction: {time.perf_counter() - t:.1f}s")


if __name__ == "__main__":
    build()
    bench()
