"""Find how the library behaves with more than 4 requests in flight.

Run from the repo root:  .venv/bin/python spikes/slot_limit.py N [N ...]
For each N, starts N identical slow requests at once (one shared client: a second login invalidates the first) and
prints each one's start, finish and error. With a 4-slot limit, request 5 either
fails or waits for a slot (finishes about one request-length later).
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from thetadata import ThetaClient


def request(client):
    return client.option_history_greeks_eod(symbol="SPY", expiration=date(2024, 12, 20), right="put",
                                            start_date=date(2024, 6, 3), end_date=date(2024, 6, 7))


def run(n, clients):
    t0 = time.time()

    def one(i):
        s = time.time() - t0
        try:
            rows, err = len(request(clients[i])), ""
        except Exception as e:
            rows, err = 0, f"{type(e).__name__}: {str(e)[:150]}"
        return i, s, time.time() - t0, rows, err

    with ThreadPoolExecutor(n) as pool:
        results = list(pool.map(one, range(n)))
    print(f"\n== {n} at once")
    for i, s, f, rows, err in results:
        print(f"  #{i + 1}: start {s:5.1f}s  finish {f:5.1f}s  rows {rows}  {err}")


def main():
    ns = [int(a) for a in sys.argv[1:]] or [1]
    client = ThetaClient()  # one session per account: a second login kills the first
    clients = [client] * max(ns)
    for n in ns:
        run(n, clients)


if __name__ == "__main__":
    main()
