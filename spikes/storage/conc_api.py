import multiprocessing as mp, time, statistics, apibench as A

if __name__ == "__main__":
    ctx = mp.get_context("spawn")
    for mode in ["api", "direct"]:
        for n in [1, 8]:
            with ctx.Pool(n) as p:
                p.map(A.lookup_worker, [(mode, i, 2) for i in range(n)])  # start and warm the workers
                t = time.perf_counter()
                lat = sorted(sum(p.map(A.lookup_worker, [(mode, i, 60) for i in range(n)]), []))
                wall = time.perf_counter() - t
            print(f"{mode:<6} {n} clients: {len(lat)/wall:6.0f}/s  p50 {statistics.median(lat)*1000:5.1f} ms  "
                  f"p95 {lat[int(len(lat)*.95)-1]*1000:6.1f} ms", flush=True)
