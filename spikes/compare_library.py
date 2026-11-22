"""Compare the official thetadata library's output with the Theta Terminal's CSV.

Run from the repo root (the library reads THETADATA_API_KEY from ./.env):
    .venv/bin/python spikes/compare_library.py

One request in flight at a time: each case asks the terminal, then the library.
"""
import csv
import io
import time
import urllib.parse
import urllib.request
from datetime import date

from thetadata import ThetaClient

TERMINAL = "http://thetadata.home.thms.io:25503/v3"

CASES = [
    ("stock EOD", "stock/history/eod",
     dict(symbol="F", start_date="20240603", end_date="20240607"),
     lambda c: c.stock_history_eod(symbol="F", start_date=date(2024, 6, 3), end_date=date(2024, 6, 7))),
    ("greeks EOD, chain slice", "option/history/greeks/eod",
     dict(symbol="F", expiration="20200313", right="put", strike_range="4", start_date="20200305", end_date="20200306"),
     lambda c: c.option_history_greeks_eod(symbol="F", expiration=date(2020, 3, 13), right="put", strike_range=4,
                                           start_date=date(2020, 3, 5), end_date=date(2020, 3, 6))),
    ("expirations", "option/list/expirations",
     dict(symbol="F"),
     lambda c: c.option_list_expirations(symbol="F")),
    ("contracts traded", "option/list/contracts/trade",
     dict(date="20200305"),
     lambda c: c.option_list_contracts(request_type="trade", date=date(2020, 3, 5))),
]


def terminal(endpoint, params):
    url = f"{TERMINAL}/{endpoint}?{urllib.parse.urlencode({**params, 'format': 'csv'})}"
    with urllib.request.urlopen(url, timeout=120) as r:
        return list(csv.DictReader(io.StringIO(r.read().decode())))


def same(a, b):
    if a == b:
        return True
    try:
        return abs(float(a) - float(b)) <= 1e-6 * max(1, abs(float(a)))
    except ValueError:
        return False


def norm(v):
    """Library values as the terminal would print them."""
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def main():
    client = ThetaClient()
    for name, endpoint, params, call in CASES:
        print(f"\n== {name} ({endpoint})")
        t0 = time.time(); term = terminal(endpoint, params); t_term = time.time() - t0
        t0 = time.time(); df = call(client); t_lib = time.time() - t0
        lib = [{k: norm(v) for k, v in row.items()} for row in df.to_dicts()]
        tcols, lcols = list(term[0]) if term else [], list(df.columns)
        print(f"rows: terminal {len(term)} ({t_term:.1f}s), library {len(lib)} ({t_lib:.1f}s)")
        print(f"terminal columns: {tcols}")
        print(f"library columns:  {lcols}")
        print(f"library dtypes:   {dict(zip(df.columns, map(str, df.dtypes)))}")
        if set(tcols) - set(lcols) or set(lcols) - set(tcols):
            print(f"only terminal: {sorted(set(tcols) - set(lcols))}; only library: {sorted(set(lcols) - set(tcols))}")
        common = [c for c in tcols if c in lcols]
        # Compare as sorted row sets on the common columns, so order differences don't count.
        key = lambda r: tuple(r[c] for c in common)
        mism = 0
        for t, l in zip(sorted(term, key=key), sorted(lib, key=key)):
            bad = [c for c in common if not same(t[c], l[c])]
            if bad:
                mism += 1
                if mism <= 3:
                    print("  differs:", {c: (t[c], l[c]) for c in bad})
        print(f"rows differing on common columns: {mism}")
        if term:
            print(f"first terminal row: {term[0]}")
            print(f"first library row:  {lib[0]}")


if __name__ == "__main__":
    main()
