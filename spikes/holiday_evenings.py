"""Evening GTH (20:15-24:00 ET) around holidays: which ThetaData calendar dates have it, and full-day counts for the
holiday itself. SPXW near-the-money call, expiration = first SPXW expiration after the date; tick quotes and
trade_quote. Production, ThetaDataDx 0.9.0, one request at a time. Usage: holiday_evenings.py [YYYYMMDD ...]
"""
import os, sys, datetime as dt
import polars as pl
import thetadatadx as tdx

ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
KEY = next(l.split("=", 1)[1].strip().strip('"\'') for l in open(ENV) if l.startswith("THETADATA_API_KEY="))
T = 600_000
H = lambda h, m=0: (h * 60 + m) * 60_000
EMPTY = pl.DataFrame(schema={"ms_of_day": pl.Int32, "bid": pl.Float64, "ask": pl.Float64})
# Holiday eves and holidays: the evening before a full close, the full close itself, the evening after.
DATES = sys.argv[1:] or ["20241127", "20241128", "20241129",   # Thanksgiving 2024 (Thu closed, Fri half day)
         "20241224", "20241225", "20241226",   # Christmas 2024 (Tue half day, Wed closed)
         "20241231", "20250101", "20250102",   # New Year 2025
         "20250117", "20250119", "20250120", "20250121",  # MLK 2025 (Mon closed): Fri, Sun, Mon, Tue
         "20250108", "20250109", "20250110",   # 2025-01-09 one-off closure
         "20250703", "20250704", "20250706",   # July 4 2025 (Thu half day, Fri closed, Sun)
         "20251126", "20251127", "20251128"]   # Thanksgiving 2025


def pull(m, kind, exp, d, sk):
    kw = dict(date=d, strike=sk, right="call", start_time="00:00:00", end_time="23:59:59", timeout_ms=T)
    try:
        if kind == "ticks":
            return m.option_history_quote("SPXW", exp, interval="tick", **kw).to_polars()
        return m.option_history_trade_quote("SPXW", exp, **kw).to_polars()
    except tdx.NotFoundError:
        return EMPTY


def main():
    m = tdx.Client(api_key=KEY).market_data
    exps = sorted(int(str(x).replace("-", "")) for x in m.option_list_expirations("SPXW", timeout_ms=T))
    for d in DATES:
        exp = str(min(e for e in exps if e > int(d)))
        # Near the money from the previous trading day's 1m chain at 15:00 (the date itself may be closed).
        ref = dt.datetime.strptime(d, "%Y%m%d")
        sk = None
        for back in range(0, 6):
            rd = (ref - dt.timedelta(back)).strftime("%Y%m%d")
            try:
                ch = m.option_history_quote("SPXW", exp, date=rd, interval="1m", start_time="12:00:00",
                                            end_time="12:00:00", timeout_ms=T).to_polars()
            except tdx.NotFoundError:
                continue
            piv = (ch.filter(pl.col("bid") > 0).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2)
                     .pivot(on="right", index="strike", values="mid").drop_nulls())
            if piv.height:
                k = piv.with_columns(x=(pl.col("C") - pl.col("P")).abs()).sort("x")["strike"][0]
                sk = str(int(k)) if float(k).is_integer() else str(k); break
        if sk is None:
            print(f"{d}: no reference chain"); continue
        parts = []
        for kind in ["ticks", "trades"]:
            df = pull(m, kind, exp, d, sk)
            dayf = df.filter(pl.col("ms_of_day") < H(20)); day = dayf.height
            fm = lambda v: f"{v // 3600000:02d}:{v // 60000 % 60:02d}"
            dspan = f" [{fm(dayf['ms_of_day'].min())}-{fm(dayf['ms_of_day'].max())}]" if day else ""
            eve = df.filter(pl.col("ms_of_day") >= H(20))
            span = f"{eve['ms_of_day'].min() // 60000 // 60:02d}:{eve['ms_of_day'].min() // 60000 % 60:02d}" if eve.height else ""
            parts.append(f"{kind} before 20:00 {day:>9,}{dspan} | 20:00-24:00 {eve.height:>7,} {('from ' + span) if span else ''}")
        print(f"{d} {dt.datetime.strptime(d, '%Y%m%d'):%a} exp {exp} {sk} C:  " + "  ||  ".join(parts), flush=True)


if __name__ == "__main__":
    main()
