"""Reads while writing. Usage: rw.py setup|run naive|lake|tsdb

Held out: trade dates 2026-09-01 on (42 days). The writer lands them one (day, expiration) unit at a
time from p1/, as fast as it can, and compacts after each day. Readers: 8 point-lookup processes and
2 sweep processes. Every answer is checked against the expected row count for each day.
"""
import glob, io, json, multiprocessing as mp, os, random, shutil, statistics, sys, time, traceback
import duckdb, polars as pl, psycopg

HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE)
CUT = "2026-09-01"
PG = "host=127.0.0.1 port=55432 user=postgres password=bench"
COLS = ["trade_date", "expiration", "strike", "right", "timestamp", "bid_size", "bid_exchange", "bid",
        "bid_condition", "ask_size", "ask_exchange", "ask", "ask_condition"]
N_POINT, N_SWEEP, READ_ONLY_SECS = 8, 2, 30


def expected():
    if not os.path.exists("expected.json"):
        rows = duckdb.sql("SELECT trade_date::VARCHAR, count(*) FROM read_parquet('p2/*.parquet') "
                          "WHERE expiration = trade_date AND timestamp::time = TIME '12:00:00' "
                          "AND year(trade_date) < 2030 GROUP BY 1").fetchall()
        json.dump(dict(rows), open("expected.json", "w"))
    return json.load(open("expected.json"))


def units():
    return sorted(f for f in glob.glob("p1/trade_date=*/exp=*.parquet") if f.split("=")[1][:10] >= CUT)


def lake_attach(con, read_only=False):
    con.execute("LOAD ducklake; LOAD sqlite; LOAD postgres")
    ro = ", READ_ONLY" if read_only else ""
    cat = ("postgres:dbname=lake host=127.0.0.1 port=55432 user=postgres password=bench"
           if os.environ.get("CATALOG") == "pg" else "sqlite:lake/meta.sqlite")
    con.execute(f"ATTACH 'ducklake:{cat}' AS lake (DATA_PATH 'lake/data/'{ro})")


# ---------------------------------------------------------------- setup
def setup(v):
    expected()
    if v == "naive":
        shutil.rmtree("p_naive", ignore_errors=True); os.makedirs("p_naive")
        for f in glob.glob("p2/*.parquet"):
            if os.path.basename(f)[:7] < CUT[:7]:
                os.link(f, f"p_naive/{os.path.basename(f)}")
    elif v == "lake":
        shutil.rmtree("lake", ignore_errors=True); os.makedirs("lake/data")
        con = duckdb.connect(); lake_attach(con)
        con.execute("CREATE TABLE lake.q AS SELECT * FROM read_parquet('p1/trade_date=2022-05-09/*.parquet') LIMIT 0")
        con.execute("ALTER TABLE lake.q SET PARTITIONED BY (year(trade_date), month(trade_date))")
        t = time.time()
        con.execute(f"INSERT INTO lake.q BY NAME SELECT * FROM read_parquet('p2/*.parquet') "
                    f"WHERE trade_date < DATE '{CUT}' ORDER BY trade_date, expiration, strike, \"right\", timestamp")
        print(f"lake loaded in {time.time()-t:.0f}s")
    elif v == "tsdb":
        with psycopg.connect(PG, autocommit=True) as c:
            t = time.time()
            c.execute(f"SELECT drop_chunks('q', newer_than => TIMESTAMP '{CUT}')")
            c.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
            c.execute(f"DELETE FROM q WHERE ts >= TIMESTAMP '{CUT}'")
            c.execute("ANALYZE q")
            print(f"tsdb held-out rows removed in {time.time()-t:.0f}s; rows after cut:",
                  c.execute(f"SELECT count(*) FROM q WHERE ts >= TIMESTAMP '{CUT}'").fetchone()[0])


# ---------------------------------------------------------------- readers
def reader(v, kind, seed, stop, out):
    exp = expected(); days = sorted(exp); rnd = random.Random(seed)
    lat, errors, anomalies = [], [], []
    try:
        if v == "tsdb":
            con = psycopg.connect(PG, autocommit=True)
            run = lambda s: con.execute(s).fetchall()
        else:
            con = duckdb.connect(config={"threads": 1 if kind == "point" else 4})
            if v == "lake":
                lake_attach(con, read_only=True)
            run = lambda s: con.execute(s).fetchall()
    except Exception as e:
        out.put((kind, [], [f"connect: {e}"], [])); return

    def point_sql(d):
        if v == "naive":
            src = f"read_parquet('p_naive/{d[:7]}*.parquet')"
        elif v == "lake":
            src = "lake.q"
        else:
            return (f"SELECT count(*) FROM q WHERE expiration = DATE '{d}' AND ts = TIMESTAMP '{d} 12:00:00' "
                    f"AND ts >= TIMESTAMP '{d}' AND ts < TIMESTAMP '{d}' + interval '1 day'")
        return (f"SELECT count(*) FROM {src} WHERE trade_date = DATE '{d}' AND expiration = DATE '{d}' "
                f"AND timestamp = TIMESTAMP '{d} 12:00:00'")

    def sweep_sql():
        src = {"naive": "read_parquet('p_naive/*.parquet')", "lake": "lake.q", "tsdb": "q"}[v]
        ts = "ts" if v == "tsdb" else "timestamp"
        return (f"SELECT trade_date::VARCHAR, count(*) FROM {src} WHERE expiration = trade_date "
                f"AND {ts}::time = TIME '12:00:00' AND trade_date < DATE '2030-01-01' GROUP BY 1")

    while not stop.is_set():
        d = rnd.choice(days)
        t = time.perf_counter()
        try:
            if kind == "point":
                try:
                    n = run(point_sql(d))[0][0]
                except duckdb.IOException as e:
                    if "No files found" in str(e) and d >= CUT:
                        n = 0  # month not landed yet: an empty answer, not an error
                    else:
                        raise
                ok = n == exp[d] or (n == 0 and d >= CUT)
                if not ok:
                    anomalies.append(f"{d}: got {n}, want {exp[d]}")
            else:
                got = dict(run(sweep_sql()))
                for day, want in exp.items():
                    n = got.get(day, 0)
                    if not (n == want or (n == 0 and day >= CUT)):
                        anomalies.append(f"sweep {day}: got {n}, want {want}")
            lat.append(time.perf_counter() - t)
        except Exception as e:
            errors.append(f"{type(e).__name__}: {str(e).splitlines()[0][:160]}")
    out.put((kind, lat, errors, anomalies))


# ---------------------------------------------------------------- writers
def write_naive(log):
    for day in sorted({u.split("=")[1][:10] for u in units()}):
        m = day[:7]
        for u in [x for x in units() if x.split("=")[1][:10] == day]:
            t = time.perf_counter()
            dst = f"p_naive/{m}~{day}~{os.path.basename(u)}"
            shutil.copy(u, dst + ".tmp"); os.replace(dst + ".tmp", dst)
            log["land"].append(time.perf_counter() - t)
        t = time.perf_counter()
        parts = sorted(glob.glob(f"p_naive/{m}*.parquet"))
        duckdb.execute(f"COPY (SELECT * FROM read_parquet({parts!r}) ORDER BY trade_date, expiration, strike, "
                       f"\"right\", timestamp) TO 'p_naive/{m}.tmp' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 131072)")
        os.replace(f"p_naive/{m}.tmp", f"p_naive/{m}.parquet")
        for p in parts:
            if "~" in p:
                os.remove(p)
        log["compact"].append(time.perf_counter() - t)


def write_lake(log):
    con = duckdb.connect(); lake_attach(con)
    for day in sorted({u.split("=")[1][:10] for u in units()}):
        for u in [x for x in units() if x.split("=")[1][:10] == day]:
            t = time.perf_counter()
            con.execute(f"INSERT INTO lake.q BY NAME SELECT * FROM read_parquet('{u}') "
                        f"ORDER BY expiration, strike, \"right\", timestamp")
            log["land"].append(time.perf_counter() - t)
        t = time.perf_counter()
        con.execute("CALL ducklake_merge_adjacent_files('lake')")
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '60 seconds')")
        con.execute("CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '60 seconds')")
        log["compact"].append(time.perf_counter() - t)


def write_tsdb(log):
    with psycopg.connect(PG, autocommit=True) as c:
        for day in sorted({u.split("=")[1][:10] for u in units()}):
            for u in [x for x in units() if x.split("=")[1][:10] == day]:
                t = time.perf_counter()
                buf = io.BytesIO()
                pl.read_parquet(u).select(COLS).write_csv(buf, include_header=False)
                with c.transaction():
                    with c.cursor().copy("COPY q FROM STDIN (FORMAT csv)") as cp:
                        cp.write(buf.getvalue())
                log["land"].append(time.perf_counter() - t)
            t = time.perf_counter()
            c.execute(f"SELECT compress_chunk(ch, if_not_compressed => true) "
                      f"FROM show_chunks('q', older_than => TIMESTAMP '{day}') ch")
            log["compact"].append(time.perf_counter() - t)


# ---------------------------------------------------------------- run
def phase(v, label, write):
    stop, out = mp.Event(), mp.Queue()
    procs = [mp.Process(target=reader, args=(v, "point", i, stop, out)) for i in range(N_POINT)]
    procs += [mp.Process(target=reader, args=(v, "sweep", 100 + i, stop, out)) for i in range(N_SWEEP)]
    for p in procs: p.start()
    time.sleep(3)
    t0 = time.perf_counter(); log = {"land": [], "compact": []}; werr = None
    if write:
        try:
            {"naive": write_naive, "lake": write_lake, "tsdb": write_tsdb}[v](log)
        except Exception:
            werr = traceback.format_exc().strip().splitlines()[-1]
    else:
        time.sleep(READ_ONLY_SECS)
    wall = time.perf_counter() - t0
    stop.set()
    res = [out.get() for _ in procs]
    for p in procs: p.join()
    print(f"\n{v} — {label} ({wall:.0f}s)")
    for kind in ("point", "sweep"):
        lat = sorted(sum((r[1] for r in res if r[0] == kind), []))
        errs = sum((r[2] for r in res if r[0] == kind), []); anoms = sum((r[3] for r in res if r[0] == kind), [])
        if lat:
            p95 = lat[max(0, int(len(lat) * .95) - 1)]
            print(f"  {kind:<5}: {len(lat)/wall:7.1f}/s  p50 {statistics.median(lat)*1000:8.1f} ms  "
                  f"p95 {p95*1000:8.1f} ms  ok {len(lat)}")
        print(f"         errors {len(errs)}{'  e.g. ' + errs[0] if errs else ''}")
        print(f"         wrong answers {len(anoms)}{'  e.g. ' + anoms[0] if anoms else ''}")
    if write:
        rows = 12_434_330
        print(f"  writer: {len(log['land'])} units in {wall:.0f}s ({rows/wall/1e3:.0f}k rows/s); "
              f"land p50 {statistics.median(log['land'])*1000:.0f} ms; compact p50 "
              f"{statistics.median(log['compact'])*1000:.0f} ms max {max(log['compact'])*1000:.0f} ms"
              + (f"\n  WRITER FAILED: {werr}" if werr else ""))


if __name__ == "__main__":
    step, v = sys.argv[1], sys.argv[2]
    if step == "setup":
        setup(v)
    else:
        phase(v, "reads only", write=False)
        phase(v, "reads while writing", write=True)
