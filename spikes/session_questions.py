"""The ThetaData session questions left open on 2026-11-22. Production unless stated; ThetaDataDx 0.9.0; one request
in flight at a time. Each case picks the near-the-money call (call and put mids closest at 10:00) and pulls tick quotes
and trade_quote over narrow windows only.
Usage: session_questions.py [part ...]   parts: calendar late sunday early premarket halfday open staging (default all)

  calendar  ThetaData's own early-close list, 2021-2026
  late      which roots quote/trade 16:00-16:15 and in the curb (non-expiring contract), and pre-09:30 activity
  sunday    Sunday-evening GTH under ThetaData's dates, and the Good Friday 2024 weekend
  early     GTH before and after 2022
  premarket first date with real SPY option quotes before 09:25 in 2026
  halfday   last quotes/trades on early-close days
  open      the 09:30:00 interval rows against the ticks around them, across days and roots
  staging   the same 09:30 rows from staging, whole chain
"""
import os, sys, datetime as dt
import polars as pl
import thetadatadx as tdx

ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
H = lambda h, m=0, s=0: ((h * 60 + m) * 60 + s) * 1000
T = 600_000
_exp_cache = {}
EMPTY = pl.DataFrame(schema={"ms_of_day": pl.Int32, "bid": pl.Float64, "ask": pl.Float64})


def fmt(ms):
    if ms is None: return "-"
    s, ms = divmod(int(ms), 1000); h, s = divmod(s, 3600); m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def err(e): return f"{type(e).__name__}: {str(e)[:160]}"


def expirations(m, root):
    if root not in _exp_cache:
        _exp_cache[root] = sorted(int(str(x).replace("-", "")) for x in m.option_list_expirations(root, timeout_ms=T))
    return _exp_cache[root]


def pick_exp(m, root, d, same_day):
    exps = expirations(m, root)
    if same_day:
        return d if int(d) in exps else None
    later = [e for e in exps if e > int(d)]
    return str(later[0]) if later else None


def atm(m, root, exp, d, at="10:00:00"):
    ch = m.option_history_quote(root, exp, date=d, interval="1m", start_time=at, end_time=at, timeout_ms=T).to_polars()
    piv = (ch.filter(pl.col("bid") > 0).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
             .pivot(on="right", index="strike", values="mid").drop_nulls())
    k = piv.with_columns(d=(pl.col("C") - pl.col("P")).abs()).sort("d")["strike"][0]
    return str(int(k)) if float(k).is_integer() else str(k)


def pull(m, kind, root, exp, d, sk, a, b):
    kw = dict(date=d, strike=sk, right="call", start_time=a, end_time=b, timeout_ms=T)
    try:
        if kind == "ticks":
            return m.option_history_quote(root, exp, interval="tick", **kw).to_polars()
        return m.option_history_trade_quote(root, exp, **kw).to_polars()
    except tdx.NotFoundError:  # ThetaData answers an empty window with NotFound: that's zero rows, not an error
        return EMPTY


def count(df, a, b, real=False):
    w = df.filter((pl.col("ms_of_day") >= a) & (pl.col("ms_of_day") < b))
    if real and "bid" in w.columns:
        w = w.filter((pl.col("bid") > 0) | (pl.col("ask") > 0))
    return w.height, (fmt(w["ms_of_day"].min()), fmt(w["ms_of_day"].max())) if w.height else None


def windows_report(m, root, exp, d, sk, spans, windows):
    """Pull ticks and trades over each span; report counts per window (quotes: real = not 0/0)."""
    for kind in ["ticks", "trades"]:
        try:
            df = pl.concat(how="diagonal_relaxed", items=[pull(m, kind, root, exp, d, sk, a, b) for a, b in spans])
        except Exception as e:
            print(f"    {kind}: {err(e)}"); continue
        parts = []
        for name, a, b in windows:
            n, span = count(df, a, b, real=(kind == "ticks"))
            parts.append(f"{name} {n:,}" + (f" [{span[0]}..{span[1]}]" if span else ""))
        print(f"    {kind:<6} " + " | ".join(parts), flush=True)


def case(m, root, d, same_day, label=""):
    try:
        exp = pick_exp(m, root, d, same_day)
        if exp is None:
            print(f"  {root} {d}: no {'same-day' if same_day else 'later'} expiration"); return None
        sk = atm(m, root, exp, d)
    except Exception as e:
        print(f"  {root} {d}: {err(e)}"); return None
    print(f"  {root} {d} exp {exp} {sk} C {label}", flush=True)
    return exp, sk


# ---------------------------------------------------------------- parts
def part_calendar(m):
    print("\n## ThetaData calendar: non-standard days")
    for y in range(2021, 2027):
        try:
            days = m.calendar_year(str(y), timeout_ms=T)
            rows = []
            for c in days:
                rows.append({k: getattr(c, k) for k in dir(c) if not k.startswith("_") and not callable(getattr(c, k))})
            print(f"  {y}: {rows}")
        except Exception as e:
            print(f"  {y}: {err(e)}")


LATE_ROOTS = ["SPXW", "SPX", "XSP", "NDXP", "NDX", "RUTW", "VIX", "SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "XLF",
              "EEM", "SMH", "AAPL", "TSLA", "NVDA"]


def part_late(m):
    win = [("pre 07:00-09:25", H(7), H(9, 25)), ("16:00-16:15", H(16), H(16, 15)), ("16:15-17:00", H(16, 15), H(17))]
    for d in ["20240314", "20261119"]:
        print(f"\n## After 16:00 and before 09:25, non-expiring near-the-money call, {d}")
        for root in LATE_ROOTS:
            c = case(m, root, d, same_day=False)
            if c: windows_report(m, root, *c[:1], d, c[1], [("07:00:00", "09:30:00"), ("15:55:00", "17:05:00")], win)
    print("\n## Expiry day after 16:00 (same-day expiration), 2026-11-20")
    for root in ["QQQ", "IWM", "XSP", "NDXP", "RUTW", "AAPL"]:
        c = case(m, root, "20261120", same_day=True)
        if c: windows_report(m, root, c[0], "20261120", c[1], [("15:55:00", "17:05:00")], win[1:])


def part_sunday(m):
    win = [("00:00-09:30", H(0), H(9, 30)), ("09:30-17:00", H(9, 30), H(17)), ("20:00-24:00", H(20), H(24))]
    print("\n## Sunday evening GTH and holiday weekends, SPXW")
    # Sundays (2024-03-10 is also the DST change), the Friday before, the Monday after; Good Friday 2024-03-29.
    for d, ref in [("20261113", "20261113"), ("20261115", "20261113"), ("20261116", "20261113"),
                   ("20240308", "20240308"), ("20240310", "20240308"), ("20240311", "20240308"),
                   ("20240328", "20240328"), ("20240329", "20240328"), ("20240331", "20240328"), ("20240401", "20240328")]:
        try:
            exp = str(min(e for e in expirations(m, "SPXW") if e > int(d)))  # listed on d and the days around it
            sk = atm(m, "SPXW", exp, ref)
        except Exception as e:
            print(f"  {d}: {err(e)}"); continue
        print(f"  date {d} ({dt.datetime.strptime(d, '%Y%m%d'):%a}) exp {exp} {sk} C", flush=True)
        windows_report(m, "SPXW", exp, d, sk, [("00:00:00", "23:59:59")], win)


def part_early(m):
    win = [("00:00-09:30", H(0), H(9, 30)), ("16:15-17:00", H(16, 15), H(17)), ("20:00-24:00", H(20), H(24))]
    print("\n## GTH and curb before/after 2022")
    for root, d in [("SPX", "20210315"), ("SPXW", "20210315"), ("SPXW", "20211115"), ("SPXW", "20211123"),
                    ("SPXW", "20211215"), ("SPXW", "20220118"), ("SPXW", "20220315"), ("SPXW", "20220615")]:
        c = case(m, root, d, same_day=False)
        if c: windows_report(m, root, c[0], d, c[1], [("00:00:00", "09:30:00"), ("16:00:00", "23:59:59")], win)


def part_premarket(m):
    print("\n## First 2026 date with real SPY option quotes 07:30-09:25 (bisection over weekdays)")
    days = [d for d in (dt.date(2026, 6, 1) + dt.timedelta(n) for n in range(180)) if d.weekday() < 5
            and d <= dt.date(2026, 11, 19)]

    def real(d):
        ds = d.strftime("%Y%m%d")
        try:
            exp = pick_exp(m, "SPY", ds, same_day=False); sk = atm(m, "SPY", exp, ds)
            df = pull(m, "ticks", "SPY", exp, ds, sk, "07:00:00", "09:25:00")
        except Exception as e:
            print(f"  {ds}: {err(e)}"); return None
        n, span = count(df, H(7), H(9, 25), real=True)
        print(f"  {ds}: {n:,} real quotes {span or ''}", flush=True)
        return n > 0

    lo, hi = 0, len(days) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        r = real(days[mid])
        while r is None and mid < hi:  # holiday or error: step forward
            mid += 1; r = real(days[mid])
        if r: hi = mid
        else: lo = mid + 1
    print(f"  first date with real quotes: {days[lo]}")
    for d in days[max(lo - 2, 0):lo + 3]:  # neighbours, to see it isn't a one-off
        real(d)


def part_halfday(m):
    win = [("12:45-13:00", H(12, 45), H(13)), ("13:00-13:15", H(13), H(13, 15)), ("13:15-14:00", H(13, 15), H(14)),
           ("14:00-17:00", H(14), H(17)), ("20:00-24:00", H(20), H(24))]
    print("\n## Early-close days")
    for d in ["20241129", "20241224", "20250703", "20251128", "20251224"]:
        for root, same in [("SPXW", False), ("SPXW", True), ("SPY", False), ("SPY", True)]:
            c = case(m, root, d, same_day=same, label="(expiring)" if same else "")
            if c: windows_report(m, root, c[0], d, c[1], [("12:45:00", "23:59:59")], win)


OPEN_DATES = ["20240314", "20240612", "20240829", "20250407", "20250915", "20261119", "20261120"]


def part_open(m):
    print("\n## The 09:30:00 row: 1m and 1s interval rows against the last tick before and the first tick after")
    for d in OPEN_DATES:
        for root, same in [("SPXW", True), ("SPXW", False), ("SPY", False), ("QQQ", False), ("AAPL", False)]:
            c = case(m, root, d, same_day=same, label="(0DTE)" if same else "")
            if not c: continue
            exp, sk = c
            try:
                kw = dict(date=d, strike=sk, right="call", start_time="09:30:00", end_time="09:30:00", timeout_ms=T)
                r1m = m.option_history_quote(root, exp, interval="1m", **kw).to_polars()
                r1s = m.option_history_quote(root, exp, interval="1s", **kw).to_polars()
                tk = pull(m, "ticks", root, exp, d, sk, "00:00:00", "09:31:00")
            except Exception as e:
                print(f"    {err(e)}"); continue
            q = lambda df: (f"{df['bid_size'][0]}x{df['bid'][0]:.2f} / {df['ask_size'][0]}x{df['ask'][0]:.2f}"
                            if df.height else "none")
            before = tk.filter(pl.col("ms_of_day") <= H(9, 30))
            real_before = before.filter((pl.col("bid") > 0) | (pl.col("ask") > 0))
            after = tk.filter(pl.col("ms_of_day") > H(9, 30))
            print(f"    1m 09:30 {q(r1m)} | 1s 09:30 {q(r1s)}")
            print(f"    last tick <=09:30 {fmt(before['ms_of_day'].max()) if before.height else '-'} {q(before.tail(1))}"
                  f" | last real tick {fmt(real_before['ms_of_day'].max()) if real_before.height else '-'} "
                  f"{q(real_before.tail(1))} | first tick after {fmt(after['ms_of_day'].min()) if after.height else '-'} "
                  f"{q(after.head(1))}", flush=True)


def part_staging(_m):
    print("\n## Staging vs production, whole SPXW 0DTE chain, 1m row at 09:30:00")
    clients = {"PROD": _m}  # a second production login would end the first session
    try:
        clients["STAGE"] = tdx.Client(api_key=KEY, market_data_type="STAGE").market_data
    except Exception as e:
        print(f"  STAGE login: {err(e)}")
    for d in ["20240314", "20240829", "20261119"]:
        for env, c in clients.items():
            try:
                df = c.option_history_quote("SPXW", d, date=d, interval="1m", start_time="09:30:00",
                                            end_time="09:31:00", timeout_ms=T).to_polars()
            except Exception as e:
                print(f"  {d} {env}: {err(e)}"); continue
            o = df.filter(pl.col("ms_of_day") == H(9, 30))
            z = o.filter((pl.col("bid") == 0) & (pl.col("ask") == 0)).height
            print(f"  {d} {env:<5}: 09:30 rows {o.height}, 0/0 {z}; 09:31 rows "
                  f"{df.filter(pl.col('ms_of_day') == H(9, 31)).height}", flush=True)


PARTS = dict(calendar=part_calendar, late=part_late, sunday=part_sunday, early=part_early, premarket=part_premarket,
             halfday=part_halfday, open=part_open, staging=part_staging)

if __name__ == "__main__":
    parts = sys.argv[1:] or list(PARTS)
    m = tdx.Client(api_key=KEY).market_data
    for p in parts:
        PARTS[p](m)
