"""Fetching missing units from ThetaData and storing them (README: Fetch units, not queries; Build plan, Phase 3).

- A unit is fetched only if `coverage` doesn't have it, and two requests needing the same missing unit share one
  fetch (single flight).
- The ThetaData call covers the unit's window: regular hours from the calendar for the root, date and expiration
  (16:15 or 16:00, 13:00/13:15 on half days), since ThetaDataDx's own default is 09:30-16:00.
- NotFound is an empty answer and is stored as one, so it isn't asked again.
- A date that may still be incomplete isn't stored. When a date is complete is still open (README: Open questions;
  the date-completeness monitor); until it's settled, only dates before yesterday are stored before 02:00 ET and
  before today after that (ThetaData: previous-day data can be missing 00:00-01:45 ET).
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone

from .calendar import ET, Asset, Calendar, CalendarError
from .ingest import convert
from .store import Store
from .theta_client import SLOTS, Priority, ThetaClient
from .units import Unit, split
from .writer import write_unit

log = logging.getLogger(__name__)

COMPLETE_AFTER = time(2, 0)  # provisional; see the module docstring


class NotStorable(Exception):
    """The unit's date may still be incomplete, so it can't be stored yet."""


def last_complete(now: datetime) -> date:
    et = now.astimezone(ET)
    return et.date() - timedelta(days=1 if et.time() >= COMPLETE_AFTER else 2)


def interval_str(ms: int) -> str:
    if ms % 60_000 == 0:
        return f"{ms // 60_000}m"
    if ms % 1000 == 0:
        return f"{ms // 1000}s"
    return f"{ms}ms"


def _hms(t: datetime) -> str:
    return t.astimezone(ET).strftime("%H:%M:%S")


def request_for(cal: Calendar, unit: Unit) -> tuple[str, tuple, dict]:
    """The ThetaDataDx call for a unit: (method, args, kwargs)."""
    if unit.kind == "option_contracts":
        return "option_list_contracts", (unit.list_type, unit.date), {"symbol": unit.root}
    kw: dict = {"date": unit.date}
    if unit.strike is not None:
        kw["strike"] = f"{unit.strike:g}"
        kw["right"] = unit.right
    if unit.kind != "option_open_interest":
        rth = [s for s in cal.sessions(unit.root, unit.date, unit.expiration, Asset.OPTION) if s.name == "rth"]
        if len(rth) != 1:
            raise CalendarError(f"{unit.key}: no regular session on {unit.date}")
        kw["start_time"], kw["end_time"] = _hms(rth[0].start), _hms(rth[0].end)
    method = {
        "option_quote": "option_history_quote",
        "option_quote_tick": "option_history_quote",
        "option_trade_quote": "option_history_trade_quote",
        "option_greeks": "option_history_greeks_first_order",
        "option_ohlc": "option_history_ohlc",
        "option_open_interest": "option_history_open_interest",
    }[unit.kind]
    if unit.kind == "option_quote_tick":
        kw["interval"] = "tick"
    elif unit.interval_ms is not None:
        kw["interval"] = interval_str(unit.interval_ms)
    return method, (unit.root, unit.expiration), kw


class Fetcher:
    def __init__(self, store: Store, calendar: Callable[[], Calendar | None], theta: ThetaClient,
                 now: Callable[[], datetime] | None = None):
        self.store = store
        self.calendar = calendar
        self.theta = theta
        self.now = now or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()
        self._inflight: dict[int, Future] = {}
        self._pool = ThreadPoolExecutor(max_workers=SLOTS, thread_name_prefix="fetch")
        self.fetched = 0
        self.shared = 0

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _cal(self) -> Calendar:
        cal = self.calendar()
        if cal is None:
            raise CalendarError("the calendar isn't loaded yet")
        return cal

    def ensure(self, units: Iterable[Unit], priority: Priority = Priority.INTERACTIVE) -> dict:
        """Fetch and store every unit that isn't covered; returns counts. Raises the first failure after the others
        finish (stored units stay stored)."""
        units = list(units)
        limit = last_complete(self.now())
        late = [u for u in units if u.date > limit]
        if late:
            raise NotStorable(f"{len(late)} unit(s) after {limit} may still be incomplete, e.g. {late[0].key}")
        covered, missing = split(self.store, units)
        futures = [self._pool.submit(self.fetch_one, u, priority) for u in missing]
        errors = []
        for f in futures:
            try:
                f.result()
            except Exception as e:
                errors.append(e)
        if errors:
            raise errors[0]
        return {"units": len(units), "covered": len(covered), "fetched": len(missing)}

    def fetch_one(self, unit: Unit, priority: Priority = Priority.INTERACTIVE) -> int:
        """Fetch and store one unit, or wait for the fetch already running for it. Returns its row count (or -1
        when it turned out to be covered already)."""
        with self._lock:
            running = self._inflight.get(unit.id)
            if running is None:
                mine = self._inflight[unit.id] = Future()
        if running is not None:
            self.shared += 1
            return running.result()
        try:
            n = self._fetch(unit, priority)
            mine.set_result(n)
            return n
        except BaseException as e:
            mine.set_exception(e)
            raise
        finally:
            with self._lock:
                self._inflight.pop(unit.id, None)

    def _fetch(self, unit: Unit, priority: Priority) -> int:
        if unit.date > last_complete(self.now()):
            raise NotStorable(f"{unit.key}: {unit.date} may still be incomplete")
        if split(self.store, [unit])[0]:
            return -1  # stored while this request waited
        cal = self._cal()
        method, args, kw = request_for(cal, unit)
        got = self.theta.call(method, *args, priority=priority, **kw)
        raw = None if got is None else got.to_polars()
        df = convert(unit, raw)
        n = write_unit(self.store, cal, unit, df)
        self.fetched += 1
        log.info("stored %s: %d row(s)", unit.key, n)
        return n
