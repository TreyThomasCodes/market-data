"""Same queries direct (DuckDB in the script) and through server.py. Results land as Polars frames both ways."""
import http.client, multiprocessing as mp, os, random, statistics, time
import duckdb, polars as pl, pyarrow.ipc as ipc

os.chdir(os.path.dirname(os.path.abspath(__file__)))
D = "2024-03-14"
Q = {
    "lookup: one 0DTE chain at 12:00, all columns":
        f"SELECT * FROM q WHERE trade_date = DATE '{D}' AND expiration = DATE '{D}' AND timestamp = TIMESTAMP '{D} 12:00:00'",
    "summary sweep: 0DTE spread by minute, all days":
        "SELECT timestamp::time m, avg(ask - bid) s FROM q WHERE expiration = trade_date AND bid > 0 GROUP BY 1",
    "row sweep: 0DTE chain at 12:00, every day":
        "SELECT * FROM q WHERE expiration = trade_date AND timestamp::time = TIME '12:00:00'",
    "big pull: every 0DTE quote, Q1 2024":
        "SELECT * FROM q WHERE expiration = trade_date AND trade_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'",
}


def direct_con():
    con = duckdb.connect()
    con.execute("LOAD ducklake; LOAD postgres")
    con.execute("ATTACH 'ducklake:postgres:dbname=lake host=127.0.0.1 port=55432 user=postgres password=bench' "
                "AS lake (DATA_PATH 'lake/data/', READ_ONLY)")
    con.execute("USE lake")
    return con


class Api:
    def __init__(self, compress=None):
        self.c = http.client.HTTPConnection("127.0.0.1", 8765)
        self.path = "/sql" + (f"?compress={compress}" if compress else "")
        self.nbytes = 0

    def __call__(self, sql):
        self.c.request("POST", self.path, body=sql.encode())
        r = self.c.getresponse(); body = r.read()
        if r.status != 200:
            raise RuntimeError(body[:300])
        self.nbytes = len(body)
        return pl.from_arrow(ipc.open_stream(body).read_all())


def timeit(fn, n):
    fn()  # warm
    ts = []
    for _ in range(n):
        t = time.perf_counter(); r = fn(); ts.append(time.perf_counter() - t)
    return statistics.median(ts), r


def lookup_worker(args):
    mode, seed, n = args
    days = [f"2024-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 29)]
    rnd = random.Random(seed)
    if mode == "direct":
        con = direct_con(); run = lambda s: con.sql(s).pl()
    else:
        run = Api()
    lat = []
    for _ in range(n):
        d = rnd.choice(days)
        s = (f"SELECT * FROM q WHERE trade_date = DATE '{d}' AND expiration = DATE '{d}' "
             f"AND timestamp = TIMESTAMP '{d} 12:00:00'")
        t = time.perf_counter(); run(s); lat.append(time.perf_counter() - t)
    return lat


if __name__ == "__main__":
    con = direct_con(); api = Api(); apiz = Api("zstd")
    print(f"{'':<46} {'direct':>9} {'API':>9} {'API zstd':>9} {'rows':>10} {'Arrow MB':>9} {'zstd MB':>8}")
    for name, sql in Q.items():
        n = 3 if name.startswith("big") else 7
        td, rd = timeit(lambda: con.sql(sql).pl(), n)
        ta, ra = timeit(lambda: api(sql), n)
        tz, _ = timeit(lambda: apiz(sql), n)
        assert rd.shape == ra.shape, (rd.shape, ra.shape)
        print(f"{name:<46} {td*1000:7.0f}ms {ta*1000:7.0f}ms {tz*1000:7.0f}ms {rd.height:>10,} "
              f"{api.nbytes/1e6:9.1f} {apiz.nbytes/1e6:8.1f}")
    raise SystemExit
    print("\nconcurrent lookups (random 2024 days, all columns), 8 clients x 150")
    for mode in ["direct", "api"]:
        for n_clients in [1, 8]:
            with mp.Pool(n_clients) as p:
                t = time.perf_counter()
                lat = sorted(sum(p.map(lookup_worker, [(mode, i, 150) for i in range(n_clients)]), []))
                wall = time.perf_counter() - t
            print(f"  {mode:<6} {n_clients} clients: {len(lat)/wall:6.0f}/s  p50 {statistics.median(lat)*1000:5.1f} ms  "
                  f"p95 {lat[int(len(lat)*.95)-1]*1000:5.1f} ms")
