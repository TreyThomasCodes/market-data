"""Reads while writing (the Phase 2 check; `spikes/storage/rw.py` as a real test).

A writer lands fetch units through `write_unit` as fast as it can, refetches units already stored (replacing their
rows), and compacts after each day with a short grace period, so merged files are retired and deleted while readers
run. Reader threads, each on its own cursor like the service's request handlers, check every answer:

- point: in one transaction, a unit's coverage row and its chain at 12:00. A covered unit has its whole chain at one
  bid; an uncovered one has no rows.
- sweep: in one statement, rows per unit against coverage across the whole table. Every covered unit has exactly
  `n_rows` rows, and no rows exist outside a covered unit.

Any error, wrong answer, or a writer failure fails the test.
"""
import random
import threading
import time as clock
from datetime import date, datetime, time, timedelta

import pytest

from market_data.calendar import ET
from market_data.store import LAKE, Store
from market_data.tables import ensure_tables
from market_data.units import Unit
from market_data.writer import compact, write_unit

from test_writer import quotes

STRIKES = tuple(5000.0 + 5 * i for i in range(8))
CHAIN = 2 * len(STRIKES)                     # contracts per unit
PER_UNIT = CHAIN * 406                       # 09:30-16:15 at one minute
GRACE = timedelta(seconds=2)
N_POINT, N_SWEEP = 6, 2


@pytest.fixture
def lake(settings):
    s = Store(settings)
    s.attach()
    ensure_tables(s)
    yield s
    s.close()


def _units(cal, days):
    out = []
    for d in days:
        nxt = cal.next_trading(d)
        for exp in (d, nxt):
            out.append(Unit("option_quote", "SPXW", d, expiration=exp, interval_ms=60_000))
    return out


def _point(store, units, stop, out):
    rnd, errors, wrong, n = random.Random(), [], [], 0
    try:
        with store.cursor() as cur:
            while not stop.is_set():
                u = rnd.choice(units)
                at = datetime.combine(u.date, time(12, 0), ET)
                try:
                    cur.execute("BEGIN")
                    covered = cur.execute(f"SELECT count(*) FROM {LAKE}.coverage WHERE unit_id = ?",
                                          [u.id]).fetchone()[0]
                    rows, bids = cur.execute(
                        f"SELECT count(*), count(DISTINCT bid) FROM {LAKE}.option_quote "
                        "WHERE root = ? AND expiration = ? AND trade_date = ? AND ts = ?",
                        [u.root, u.expiration, u.date, at]).fetchone()
                    cur.execute("COMMIT")
                except Exception as e:
                    errors.append(f"{type(e).__name__}: {str(e).splitlines()[0][:200]}")
                    try:
                        cur.execute("ROLLBACK")
                    except Exception:
                        pass
                    continue
                n += 1
                want = CHAIN if covered else 0
                if covered > 1 or rows != want or (rows and bids != 1):
                    wrong.append(f"{u.key}: covered {covered}, rows {rows} (want {want}), distinct bids {bids}")
    finally:
        out.append(("point", n, errors, wrong))


SWEEP = f"""
    WITH r AS (SELECT unit_id, count(*) AS n FROM {LAKE}.option_quote GROUP BY 1),
         c AS (SELECT unit_id, n_rows, count(*) OVER (PARTITION BY unit_id) AS dup FROM {LAKE}.coverage)
    SELECT coalesce(r.unit_id, c.unit_id), coalesce(r.n, 0), c.n_rows, c.dup
    FROM r FULL OUTER JOIN c ON r.unit_id = c.unit_id
    WHERE c.unit_id IS NULL OR coalesce(r.n, 0) <> c.n_rows OR c.dup > 1
"""


def _sweep(store, stop, out):
    errors, wrong, n = [], [], 0
    try:
        with store.cursor() as cur:
            while not stop.is_set():
                try:
                    bad = cur.execute(SWEEP).fetchall()
                except Exception as e:
                    errors.append(f"{type(e).__name__}: {str(e).splitlines()[0][:200]}")
                    continue
                n += 1
                wrong += [f"unit {u}: rows {r}, coverage n_rows {c}, coverage rows {dup}" for u, r, c, dup in bad]
    finally:
        out.append(("sweep", n, errors, wrong))


def test_reads_while_writing(lake, cal):
    days = cal.trading_days(date(2024, 3, 1), date(2024, 3, 28))
    preload, live = days[:4], days[4:]
    units = _units(cal, days)
    for u in _units(cal, preload):
        write_unit(lake, cal, u, quotes(u.root, u.expiration, u.date, STRIKES))

    stop, out, werr = threading.Event(), [], []
    readers = [threading.Thread(target=_point, args=(lake, units, stop, out)) for _ in range(N_POINT)]
    readers += [threading.Thread(target=_sweep, args=(lake, stop, out)) for _ in range(N_SWEEP)]
    for t in readers:
        t.start()
    clock.sleep(0.5)
    written, refetched, compactions = 0, 0, 0
    try:
        for i, d in enumerate(live):
            for u in _units(cal, [d]):
                write_unit(lake, cal, u, quotes(u.root, u.expiration, u.date, STRIKES, bid=1.0))
                written += 1
            # Refetch an earlier unit with new prices: its old rows must vanish in the same snapshot.
            old = units[(2 * i) % len(units[: 2 * (4 + i)])]
            write_unit(lake, cal, old, quotes(old.root, old.expiration, old.date, STRIKES, bid=2.0 + i))
            refetched += 1
            compact(lake, GRACE)
            compactions += 1
        clock.sleep(GRACE.total_seconds() + 0.5)
        compact(lake, GRACE)   # past the grace period: retired files are deleted while readers still run
        clock.sleep(0.5)
    except Exception as e:
        werr.append(f"{type(e).__name__}: {e}")
    finally:
        stop.set()
        for t in readers:
            t.join(timeout=60)

    assert not werr, werr
    assert not any(t.is_alive() for t in readers)
    for kind in ("point", "sweep"):
        res = [r for r in out if r[0] == kind]
        checks = sum(r[1] for r in res)
        errors = [e for r in res for e in r[2]]
        wrong = [w for r in res for w in r[3]]
        assert not errors, f"{kind}: {len(errors)} error(s), e.g. {errors[:3]}"
        assert not wrong, f"{kind}: {len(wrong)} wrong answer(s), e.g. {wrong[:3]}"
        assert checks >= (50 if kind == "point" else 5), f"{kind}: only {checks} checks ran"

    # Final state: every unit stored once, with its rows, and files were merged and deleted.
    with lake.cursor() as cur:
        assert cur.execute(SWEEP).fetchall() == []
        assert cur.execute(f"SELECT count(*), sum(n_rows) FROM {LAKE}.coverage").fetchone() == \
            (len(units), len(units) * PER_UNIT)
        data, deletes = cur.execute(f"SELECT count(data_file), count(delete_file) "
                                    f"FROM ducklake_list_files('{LAKE}', 'option_quote')").fetchone()
    # Refetches leave delete files beside the data files they apply to; merging adjacent files doesn't fold them
    # in (rewriting files with deletes is upkeep for after the merge spike).
    on_disk = list((lake.data_path / "main" / "option_quote").rglob("*.parquet"))
    assert written + refetched + len(_units(cal, preload)) > data, "nothing was merged"
    assert len(on_disk) == data + deletes, \
        f"{len(on_disk)} files on disk, {data} + {deletes} in the table: cleanup didn't run"
