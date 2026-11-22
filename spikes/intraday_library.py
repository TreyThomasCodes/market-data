"""One SPXW 0DTE day, 1-minute quotes and first-order Greeks: Theta Terminal, then the official library. Sequential."""
import io, time, urllib.request, urllib.parse, sys
from datetime import date
import polars as pl
from thetadata import ThetaClient
out = sys.argv[1]; D = "20240314"
for ep in ["quote", "greeks/first_order"]:
    q = dict(symbol="SPXW", expiration=D, date=D, interval="1m", strike="*", right="both", format="csv")
    t = time.perf_counter()
    body = urllib.request.urlopen("http://thetadata.home.thms.io:25503/v3/option/history/" + ep + "?" + urllib.parse.urlencode(q), timeout=600).read()
    df = pl.read_csv(io.BytesIO(body), infer_schema_length=100000)
    print(f"terminal {ep:<20} {time.perf_counter()-t:6.2f}s {df.shape}", flush=True); df.write_parquet(f"{out}/term_{ep.replace('/','_')}.parquet")
c = ThetaClient()
for ep, fn in [("quote", c.option_history_quote), ("greeks/first_order", c.option_history_greeks_first_order)]:
    t = time.perf_counter()
    r = fn(symbol="SPXW", expiration=date(2024,3,14), date=date(2024,3,14), interval="1m")
    df = r if isinstance(r, pl.DataFrame) else pl.from_pandas(r)
    print(f"library  {ep:<20} {time.perf_counter()-t:6.2f}s {df.shape}", flush=True); df.write_parquet(f"{out}/lib_{ep.replace('/','_')}.parquet")
