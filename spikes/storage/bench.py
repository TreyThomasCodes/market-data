"""Time the backtests' typical questions against each store. Usage: bench.py [parquet|tsdb]"""
import csv, gzip, io, os, statistics, sys, time
import duckdb, psycopg

D = "2024-03-14"          # a mid-sample day
SRC = os.environ.get("BENCH_CSV", "")  # one cached gzip CSV answer for D's chain, for the baseline

# {store} is replaced by the table expression
Q = {
    "Q1 one chain at 12:00 (0DTE, one day)":
        f"SELECT strike, \"right\", bid, ask FROM {{store}} WHERE trade_date = DATE '{D}' AND expiration = DATE '{D}' "
        f"AND {{ts}} = TIMESTAMP '{D} 12:00:00'",
    "Q2 one contract, whole session":
        f"SELECT {{ts}}, bid, ask FROM {{store}} WHERE trade_date = DATE '{D}' AND expiration = DATE '{D}' "
        f"AND strike = 5150 AND \"right\" = 'C'",
    "Q3 0DTE chain at 12:00, every day":
        "SELECT trade_date, count(*), avg(ask - bid) FROM {store} WHERE expiration = trade_date "
        "AND {ts}::time = TIME '12:00:00' GROUP BY 1",
    "Q4 0DTE spread by minute, all days (full scan)":
        "SELECT {ts}::time AS m, avg(ask - bid) FROM {store} WHERE expiration = trade_date AND bid > 0 GROUP BY 1",
    "Q5 one later expiry, one strike, every day at 12:00":
        "SELECT trade_date, bid, ask FROM {store} WHERE expiration = DATE '2024-06-21' AND strike = 5000 "
        "AND \"right\" = 'P' AND {ts}::time = TIME '12:00:00'",
}


def timeit(fn, n=5):
    t = time.perf_counter(); r = fn(); first = time.perf_counter() - t
    runs = []
    for _ in range(n - 1):
        t = time.perf_counter(); fn(); runs.append(time.perf_counter() - t)
    return first, statistics.median(runs), r


def show(name, first, med, rows):
    print(f"  {name:<52} first {first*1000:9.1f} ms   warm {med*1000:9.1f} ms   rows {rows}")


def baseline():
    print("a query-keyed cache (gzip CSV, one query file)")
    def q1():
        rows = list(csv.DictReader(io.StringIO(gzip.decompress(open(SRC, "rb").read()).decode())))
        return [r for r in rows if r["timestamp"].endswith("12:00:00.000")]
    f, m, r = timeit(q1)
    show("Q1 one chain at 12:00 (read whole file, filter)", f, m, len(r))


def parquet():
    for label, store in [("P1: one file per (day, expiration), glob", "read_parquet('p1/*/*.parquet', hive_partitioning=true)"),
                         ("P2: one file per month", "read_parquet('p2/*.parquet')")]:
        print(label)
        for name, sql in Q.items():
            con = duckdb.connect()  # new connection each query set: no cross-query caching
            s = sql.format(store=store, ts="timestamp")
            f, m, r = timeit(lambda: con.execute(s).fetchall())
            show(name, f, m, len(r))
    print("P1 with the client opening the unit's file directly")
    con = duckdb.connect()
    for name in list(Q)[:2]:
        s = Q[name].format(store=f"read_parquet('p1/trade_date={D}/exp={D}.parquet')", ts="timestamp")
        s = s.replace(f"trade_date = DATE '{D}' AND ", "")
        f, m, r = timeit(lambda: con.execute(s).fetchall())
        show(name, f, m, len(r))


def tsdb(label):
    print(label)
    with psycopg.connect("host=127.0.0.1 port=55432 user=postgres password=bench") as c:
        for name, sql in Q.items():
            s = sql.format(store="q", ts="ts")
            if "trade_date = DATE" in s:  # give the planner the time range, as a real client would
                s += f" AND ts >= TIMESTAMP '{D}' AND ts < TIMESTAMP '{D}' + interval '1 day'"
            f, m, r = timeit(lambda: c.execute(s).fetchall(), n=3)
            show(name, f, m, len(r))


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "parquet"
    if what == "parquet":
        baseline(); parquet()
    else:
        tsdb(sys.argv[2] if len(sys.argv) > 2 else "TimescaleDB")
