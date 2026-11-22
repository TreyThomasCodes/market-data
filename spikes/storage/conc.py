"""Concurrent readers: N separate processes, each with its own connection.
Usage: conc.py parquet|tsdb point|sweep"""
import multiprocessing as mp, random, statistics, sys, time
import duckdb, psycopg

STORE, KIND = sys.argv[1], sys.argv[2]
DAYS = [r[0] for r in duckdb.sql("SELECT DISTINCT trade_date FROM read_parquet('p2/*.parquet') WHERE year(trade_date) < 2030").fetchall()]
PER_WORKER = {"point": 100, "sweep": 3}[KIND]


def sql(day):
    ts = "timestamp" if STORE == "parquet" else "ts"
    store = "read_parquet('p2/*.parquet')" if STORE == "parquet" else "q"
    if KIND == "point":
        s = (f"SELECT strike, \"right\", bid, ask FROM {store} WHERE trade_date = DATE '{day}' "
             f"AND expiration = DATE '{day}' AND {ts} = TIMESTAMP '{day} 12:00:00'")
        if STORE == "parquet":
            s = s.replace("read_parquet('p2/*.parquet')", f"read_parquet('p2/{str(day)[:7]}.parquet')")
        return s
    return (f"SELECT trade_date, count(*), avg(ask - bid) FROM {store} WHERE expiration = trade_date "
            f"AND {ts}::time = TIME '12:00:00' GROUP BY 1")


def worker(args):
    seed, threads = args
    rnd = random.Random(seed)
    if STORE == "parquet":
        con = duckdb.connect(config={"threads": threads})
        run = lambda s: con.execute(s).fetchall()
    else:
        con = psycopg.connect("host=127.0.0.1 port=55432 user=postgres password=bench")
        run = lambda s: con.execute(s).fetchall()
    run(sql(DAYS[0]))  # connect and warm
    lat = []
    for _ in range(PER_WORKER):
        s = sql(rnd.choice(DAYS))
        t = time.perf_counter(); run(s); lat.append(time.perf_counter() - t)
    return lat


if __name__ == "__main__":
    print(f"{STORE} {KIND}")
    for n in [1, 4, 16, 32]:
        threads = max(1, 16 // n)  # DuckDB: share the 16 cores among the processes
        with mp.Pool(n) as pool:
            t = time.perf_counter()
            lats = sum(pool.map(worker, [(i, threads) for i in range(n)]), [])
            wall = time.perf_counter() - t
        lats.sort()
        p50 = statistics.median(lats) * 1000
        p95 = lats[int(len(lats) * .95) - 1] * 1000
        print(f"  {n:>2} clients: {len(lats)/wall:8.1f} queries/s   p50 {p50:8.1f} ms   p95 {p95:8.1f} ms")
