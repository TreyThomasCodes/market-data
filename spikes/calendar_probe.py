"""Facts the Phase 1 calendar needs that earlier spikes didn't measure. Production, ThetaDataDx 0.9.0, one request in
flight at a time. Usage: calendar_probe.py [part ...]   parts: years starts (default both)

  years   which years `calendar_year` answers, and what it lists
  starts  per root, the first trade date with evening GTH quotes (20:15-24:00) and with curb quotes (16:16-17:00), by
          bisection over trading days between 2022-01-18 and 2026-11-19 (assumes once present, always present;
          the endpoints and a few midpoints are printed so a break in that assumption shows)
"""
import os, sys, datetime as dt
import polars as pl
import thetadatadx as tdx

ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
T = 600_000
H = lambda h, m=0: (h * 60 + m) * 60_000
_exps = {}
MIN_QUOTES = 10  # more than this many real quotes counts as a session (single stray quotes at 16:16+ exist)


def err(e): return f"{type(e).__name__}: {str(e)[:160]}"


def part_years(m):
    print("\n## calendar_year by year")
    for y in range(2008, 2030):
        try:
            rows = m.calendar_year(str(y), timeout_ms=T)
            rows = [{k: getattr(c, k) for k in ("date", "status", "open_time", "close_time")} for c in rows]
            st = {}
            for r in rows: st[r["status"]] = st.get(r["status"], 0) + 1
            odd = [r for r in rows if r["status"] not in ("full_close", "early_close")
                   or (r["status"] == "early_close" and (r["open_time"], r["close_time"]) != (34_200_000, 46_800_000))]
            print(f"  {y}: {len(rows)} rows {st} dates {[r['date'] for r in rows]}" + (f" ODD {odd}" if odd else ""))
        except Exception as e:
            print(f"  {y}: {err(e)}")


def trading_days(m, a, b):
    closed = set()
    for y in range(a.year, b.year + 1):
        closed |= {str(c.date) for c in m.calendar_year(str(y), timeout_ms=T) if c.status == "full_close"}
    out, d = [], a
    while d <= b:
        if d.weekday() < 5 and d.strftime("%Y%m%d") not in closed:
            out.append(d)
        d += dt.timedelta(1)
    return out


def near_call(m, root, d):
    if root not in _exps:
        _exps[root] = sorted(int(str(x).replace("-", "")) for x in m.option_list_expirations(root, timeout_ms=T))
    later = [e for e in _exps[root] if e > int(d)]
    if not later: return None
    exp = str(later[0])
    try:
        ch = m.option_history_quote(root, exp, date=d, interval="1m", start_time="12:00:00", end_time="12:00:00",
                                    timeout_ms=T).to_polars()
    except tdx.NotFoundError:
        return None
    piv = (ch.filter(pl.col("bid") > 0).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
             .pivot(on="right", index="strike", values="mid").drop_nulls())
    if not piv.height or not {"C", "P"} <= set(piv.columns): return None
    k = piv.with_columns(x=(pl.col("C") - pl.col("P")).abs()).sort("x")["strike"][0]
    return exp, (str(int(k)) if float(k).is_integer() else str(k))


def real_quotes(m, root, d, a, b):
    """Real (not 0/0) quote ticks in [a, b) on ThetaData date d for the near-the-money non-expiring call."""
    c = near_call(m, root, d)
    if c is None: return None
    exp, sk = c
    try:
        df = m.option_history_quote(root, exp, date=d, strike=sk, right="call", interval="tick", start_time=a,
                                    end_time=b, timeout_ms=T).to_polars()
    except tdx.NotFoundError:
        return 0
    return df.filter((pl.col("bid") > 0) | (pl.col("ask") > 0)).height


WINDOWS = {"gth": ("20:15:00", "23:59:59"), "curb": ("16:16:00", "17:00:00")}
ROOTS = {"gth": ["SPXW", "SPX", "XSP", "VIX", "RUTW", "NDXP"], "curb": ["SPXW", "SPX", "XSP", "VIX", "RUTW", "NDXP"]}


def part_starts(m):
    days = trading_days(m, dt.date(2022, 1, 18), dt.date(2026, 11, 19))
    # Evening GTH on a Friday doesn't exist, so search Mon-Thu only.
    for what, (a, b) in WINDOWS.items():
        print(f"\n## first date with real {what} quotes ({a}-{b}), near-the-money non-expiring call")
        cand = [d for d in days if d.weekday() < 4] if what == "gth" else days
        for root in ROOTS[what]:
            ds = lambda i: cand[i].strftime("%Y%m%d")
            probe = {}
            def has(i):
                if i not in probe:
                    n = real_quotes(m, root, ds(i), a, b)
                    probe[i] = n
                    print(f"    {root} {ds(i)} {cand[i]:%a}: {n}", flush=True)
                return (probe[i] or 0) > MIN_QUOTES
            lo, hi = 0, len(cand) - 1
            for i in (len(cand) // 4, len(cand) // 2, 3 * len(cand) // 4):
                has(i)  # spot checks against the monotonic assumption
            if has(lo):
                print(f"  {root}: present at the start ({ds(lo)})"); continue
            if not has(hi):
                print(f"  {root}: absent at the end ({ds(hi)})"); continue
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if has(mid): hi = mid
                else: lo = mid
            print(f"  {root}: last without {ds(lo)}, first with {ds(hi)}", flush=True)


PARTS = dict(years=part_years, starts=part_starts)
# Restrict to some roots/windows: calendar_probe.py starts gth=RUTW,NDXP curb=SPXW,...
for a in [a for a in sys.argv[1:] if "=" in a]:
    w, rs = a.split("=", 1)
    ROOTS[w] = rs.split(",") if rs else []
    sys.argv.remove(a)

if __name__ == "__main__":
    m = tdx.Client(api_key=KEY).market_data
    for p in sys.argv[1:] or list(PARTS):
        PARTS[p](m)
