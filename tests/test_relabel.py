"""A calendar correction relabels stored rows (README: Decisions, trade_date and session stored on every row)."""
from datetime import date, datetime, time, timedelta, timezone

import polars as pl
import pytest

from market_data.calendar import ET, Day, from_rows
from market_data.calendar_store import CalendarService, ensure_tables as ensure_calendar_tables, save_year
from market_data.store import LAKE, Store
from market_data.tables import ensure_tables
from market_data.units import Unit
from market_data.writer import check_labels, relabel_pending, write_unit

from conftest import calendar_days, fake_fetch
from test_writer import quotes

D = date(2024, 3, 14)       # Thursday
CLOSED = date(2024, 3, 15)  # made a surprise closure below (like 2025-01-09)


@pytest.fixture
def lake(settings):
    s = Store(settings)
    s.attach()
    ensure_tables(s)
    ensure_calendar_tables(s)
    yield s
    s.close()


def closed_on(cal_days: dict, d: date):
    """The fixture calendar with `d` turned into a full close."""
    days = [x for rows in cal_days.values() for x in rows if x.date != d] + [Day(d, "full_close", None, None)]
    return from_rows(days, list(cal_days))


def ticks(d: date, exp: date, times) -> pl.DataFrame:
    ts = [datetime.combine(d, t, ET).astimezone(timezone.utc) for t in times]
    return pl.DataFrame({"ts": ts, "root": "SPXW", "expiration": exp, "strike": 5100.0, "right": "C",
                         "seq": list(range(len(ts))), "bid_size": 1, "bid_exchange": 5, "bid": 1.0,
                         "bid_condition": 50, "ask_size": 1, "ask_exchange": 5, "ask": 1.1, "ask_condition": 50})


def labels(store, table, unit_id):
    with store.cursor() as cur:
        order = "ts, seq" if table == "option_quote_tick" else 'ts, strike, "right"'
        return cur.execute(f"SELECT trade_date, session FROM {LAKE}.{table} WHERE unit_id = ? ORDER BY {order}",
                           [unit_id]).fetchall()


def pending(store):
    with store.cursor() as cur:
        return [r[0] for r in cur.execute(f"SELECT date FROM {LAKE}.relabel_pending ORDER BY 1").fetchall()]


def test_closure_relabels_nearby_units_only(lake, cal):
    exp = date(2024, 3, 22)
    # Thursday evening GTH rolls to Friday; Friday's RTH is ordinary.
    thu = Unit("option_quote_tick", "SPXW", D, expiration=exp)
    fri = Unit("option_quote", "SPXW", CLOSED, expiration=exp, interval_ms=60_000)
    far = Unit("option_quote", "SPXW", date(2024, 2, 1), expiration=date(2024, 2, 1), interval_ms=60_000)
    write_unit(lake, cal, thu, ticks(D, exp, [time(12, 0), time(21, 0)]))
    write_unit(lake, cal, fri, quotes("SPXW", exp, CLOSED))
    write_unit(lake, cal, far, quotes("SPXW", far.expiration, far.date))
    assert labels(lake, "option_quote_tick", thu.id) == [(D, "rth"), (CLOSED, "gth")]

    new = closed_on(calendar_days(), CLOSED)
    with lake.cursor() as cur:
        cur.execute(f"INSERT INTO {LAKE}.relabel_pending VALUES (?, now())", [CLOSED])
        before = cur.execute(f"SELECT * FROM {LAKE}.coverage ORDER BY unit_id").fetchall()
        far_files = cur.execute(f"SELECT data_file FROM ducklake_list_files('{LAKE}', 'option_quote')").fetchall()
    got = relabel_pending(lake, new)

    monday = date(2024, 3, 18)
    assert labels(lake, "option_quote_tick", thu.id) == [(D, "rth"), (monday, "gth")]
    assert check_labels(lake, new, "option_quote", CLOSED, monday)["mismatched"] == 0
    assert check_labels(lake, new, "option_quote_tick", D, monday)["mismatched"] == 0
    assert got == {"dates": 1, "units": 2, "rows": 1 + 4 * 406}  # `far` isn't in reach of the change
    assert pending(lake) == []
    with lake.cursor() as cur:
        assert cur.execute(f"SELECT * FROM {LAKE}.coverage ORDER BY unit_id").fetchall() == before
        assert cur.execute(f"SELECT count(*) FROM {LAKE}.option_quote WHERE unit_id = ?", [fri.id]).fetchone()[0] \
            == 4 * 406
        # Friday's rows now sit on Monday's trade date, outside any session.
        assert cur.execute(f"SELECT DISTINCT trade_date, session FROM {LAKE}.option_quote WHERE unit_id = ?",
                           [fri.id]).fetchall() == [(monday, "closed")]
    assert relabel_pending(lake, new) == {"dates": 0, "units": 0, "rows": 0}


def test_unchanged_labels_rewrite_nothing(lake, cal):
    u = Unit("option_quote", "SPXW", D, expiration=D, interval_ms=60_000)
    write_unit(lake, cal, u, quotes("SPXW", D, D))
    with lake.cursor() as cur:
        cur.execute(f"INSERT INTO {LAKE}.relabel_pending VALUES (?, now())", [D])
        snap = cur.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{LAKE}')").fetchone()[0]
    assert relabel_pending(lake, cal) == {"dates": 1, "units": 1, "rows": 0}
    with lake.cursor() as cur:
        # Only the pending row's deletion was written.
        assert cur.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{LAKE}')").fetchone()[0] == snap + 1


def test_save_year_queues_changed_dates(lake):
    days = calendar_days()[2024]
    save_year(lake, 2024, days, datetime(2026, 11, 24, tzinfo=timezone.utc), changed=[CLOSED, CLOSED, D])
    assert pending(lake) == [D, CLOSED]


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_service_queues_a_change_and_relabels(lake):
    now = Clock(datetime(2026, 11, 22, 20, tzinfo=timezone.utc))
    real = calendar_days()
    calls = []

    def relabel(c):
        calls.append(c)
        return relabel_pending(lake, c)

    svc = CalendarService(lake, fake_fetch(), now=now, on_change=relabel)
    svc.start(background=False)
    u = Unit("option_quote", "SPXW", date(2026, 11, 27), expiration=date(2026, 11, 27), interval_ms=60_000)
    write_unit(lake, svc.calendar, u, quotes("SPXW", u.expiration, u.date))
    with lake.cursor() as cur:
        assert cur.execute(f"SELECT DISTINCT session FROM {LAKE}.option_quote WHERE ts = ?",
                           [datetime.combine(u.date, time(12, 0), ET)]).fetchall() == [("rth",)]

    # A day later ThetaData's 2026 calendar shows a closure on 2026-11-27.
    closed = date(2026, 11, 27)
    changed = {y: ([d for d in rows if d.date != closed] + [Day(closed, "full_close", None, None)]
                   if y == 2026 else rows) for y, rows in real.items()}
    svc.fetch = lambda years: {y: changed.get(y) for y in years}
    now.t += timedelta(days=1, hours=1)
    svc._refresh_logged()
    assert svc.calendar.is_trading(closed) is False
    assert pending(lake) == []
    assert {s for _, s in labels(lake, "option_quote", u.id)} == {"closed"}
    assert svc.health()["relabel_error"] is None and len(calls) == 2


def test_failed_relabel_is_kept_and_reported(lake, cal):
    with lake.cursor() as cur:
        cur.execute(f"INSERT INTO {LAKE}.relabel_pending VALUES (?, now())", [D])

    def boom(c):
        raise RuntimeError("disk full")

    svc = CalendarService(lake, fake_fetch(), now=Clock(datetime(2026, 11, 22, 20, tzinfo=timezone.utc)),
                          on_change=boom)
    svc.start(background=False)
    assert svc.health()["relabel_error"] == "RuntimeError: disk full"
    assert pending(lake) == [D]
    svc.on_change = lambda c: relabel_pending(lake, c)
    svc._refresh_logged()
    assert svc.health()["relabel_error"] is None and pending(lake) == []
