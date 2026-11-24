"""The stored calendar and GET /calendar, against the real catalog instance in a throwaway database."""
import logging
from datetime import date, datetime, time, timedelta, timezone

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from starlette.testclient import TestClient

from market_data.app import create_app
from market_data.calendar import CalendarError, Day
from market_data.calendar_store import CalendarService, load, save_year
from market_data.store import Store

from conftest import calendar_days, fake_fetch

NOW = datetime(2026, 11, 22, 20, tzinfo=timezone.utc)


@pytest.fixture
def store(settings):
    s = Store(settings)
    s.attach()
    yield s
    s.close()


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_save_and_load_round_trip(store):
    from market_data.calendar_store import ensure_tables
    ensure_tables(store)
    rows = calendar_days()[2024]
    save_year(store, 2024, rows, NOW)
    save_year(store, 2024, rows, NOW + timedelta(1))  # replaces, doesn't add
    days, years = load(store)
    assert days == rows and years == {2024: NOW + timedelta(1)}
    with pytest.raises(CalendarError):
        save_year(store, 2025, rows, NOW)


def test_refresh_fetches_once_then_only_current_and_next_year(store):
    calls, clock = [], Clock(NOW)
    svc = CalendarService(store, fake_fetch(calls), now=clock)
    svc.start(background=False)
    assert calls == [list(range(2012, 2028))]
    assert svc.calendar.first == date(2012, 1, 1) and svc.calendar.last == date(2027, 12, 31)
    assert svc.refresh() == [] and len(calls) == 1
    clock.t = NOW + timedelta(hours=25)
    assert svc.refresh() == [2026, 2027] and calls[-1] == [2026, 2027]
    # A fresh service on the same store loads without fetching.
    calls2 = []
    svc2 = CalendarService(store, fake_fetch(calls2), now=clock)
    svc2.start(background=False)
    assert calls2 == [] and svc2.calendar.last == date(2027, 12, 31) and svc2.health()["ok"]


def test_next_year_not_published_yet(store):
    """Asked for once a day, not on every hourly check: each ask is a production login."""
    calls, clock = [], Clock(NOW)
    svc = CalendarService(store, fake_fetch(calls, missing={2027}), now=clock)
    svc.start(background=False)
    assert svc.calendar.last == date(2026, 12, 31) and svc.health()["ok"]
    clock.t = NOW + timedelta(hours=1)
    assert svc.refresh() == [] and len(calls) == 1
    clock.t = NOW + timedelta(hours=25)
    svc.refresh()
    assert calls[-1] == [2026, 2027]


def test_missing_current_year_stores_nothing(store):
    svc = CalendarService(store, fake_fetch(missing={2026}), now=Clock(NOW))
    svc.start(background=False)
    h = svc.health()
    assert not h["ok"] and "2026" in h["error"]
    assert load(store) == ([], {})  # all or nothing: 2012-2025 weren't stored either


def test_failed_refresh_keeps_serving_the_stored_calendar(store):
    clock = Clock(NOW)
    svc = CalendarService(store, fake_fetch(), now=clock)
    svc.start(background=False)

    def down(years):
        raise ConnectionError("ThetaData is down")
    svc.fetch = down
    clock.t = NOW + timedelta(hours=25)
    assert not svc._refresh_logged()
    h = svc.health()
    assert h["ok"] and h["error"] == "ConnectionError" and svc.calendar.last == date(2027, 12, 31)


def test_no_key(store):
    svc = CalendarService(store, None, now=Clock(NOW))
    svc.start(background=False)
    assert svc.calendar is None and not svc.health()["ok"]
    CalendarService(store, fake_fetch(), now=Clock(NOW)).start(background=False)
    later = CalendarService(store, None, now=Clock(NOW + timedelta(days=3)))
    later.start(background=False)  # stale current year, no key: serves what's stored
    h = later.health()
    assert h["ok"] and h["error"] is None and not h["can_fetch"]


def test_stale_calendar_isnt_ok(store):
    CalendarService(store, fake_fetch(missing={2027}), now=Clock(NOW)).start(background=False)
    svc = CalendarService(store, None, now=Clock(datetime(2027, 1, 2, 12, tzinfo=timezone.utc)))
    svc.start(background=False)
    assert svc.calendar.last == date(2026, 12, 31) and not svc.health()["ok"]


def test_a_change_is_logged(store, caplog):
    clock = Clock(NOW)
    days = calendar_days()
    svc = CalendarService(store, fake_fetch(), now=clock)
    svc.start(background=False)
    extra = Day(date(2026, 12, 28), "full_close")

    def changed(years):
        return {y: days[y] + ([extra] if y == 2026 else []) for y in years}
    svc.fetch = changed
    clock.t = NOW + timedelta(hours=25)
    with caplog.at_level(logging.WARNING):
        svc.refresh()
    assert "calendar 2026 changed" in caplog.text and "2026-12-28" in caplog.text
    assert not svc.calendar.is_trading(date(2026, 12, 28))


def _arrow(r):
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/vnd.apache.arrow.stream"
    return ipc.open_stream(pa.py_buffer(r.content)).read_all()


def test_get_calendar(settings):
    with TestClient(create_app(settings, calendar_fetch=fake_fetch())) as client:
        t = _arrow(client.get("/calendar", params={"root": "SPXW", "start": "2024-11-27", "end": "2024-11-29"}))
        assert t.schema.field("start").type == pa.timestamp("us", tz="America/New_York")
        rows = [(r["trade_date"].isoformat(), r["session"], r["start"].strftime("%m-%d %H:%M"), r["end"].strftime("%m-%d %H:%M"))
                for r in t.to_pylist()]
        assert rows == [
            ("2024-11-27", "gth", "11-26 20:15", "11-27 09:25"), ("2024-11-27", "rth", "11-27 09:30", "11-27 16:15"),
            ("2024-11-27", "curb", "11-27 16:15", "11-27 17:00"),
            ("2024-11-29", "gth", "11-27 20:15", "11-28 11:30"), ("2024-11-29", "gth", "11-28 20:15", "11-29 09:25"),
            ("2024-11-29", "rth", "11-29 09:30", "11-29 13:15")]
        exp = _arrow(client.get("/calendar", params={"root": "spxw", "start": "2024-11-29", "expiration": "2024-11-29"}))
        assert exp.to_pylist()[-1]["end"].time() == time(13)
        stock = _arrow(client.get("/calendar", params={"root": "AAPL", "asset": "stock", "start": "2024-03-14"}))
        assert stock["session"].to_pylist() == ["pre", "rth", "post"]
        assert _arrow(client.get("/calendar", params={"root": "SPXW", "start": "2024-11-28"})).num_rows == 0
        for params, msg in [({"start": "2024-11-27"}, "root"), ({"root": "SPXW"}, "start"),
                            ({"root": "SPXW", "start": "2024-13-01"}, "start"),
                            ({"root": "SPXW", "start": "2024-11-29", "end": "2024-11-27"}, "before"),
                            ({"root": "SPXW", "start": "2030-01-02"}, "outside the calendar"),
                            ({"root": "SPXW", "start": "2024-11-29", "asset": "bond"}, "asset")]:
            r = client.get("/calendar", params=params)
            assert r.status_code == 400 and msg in r.json()["error"], (params, r.text)
