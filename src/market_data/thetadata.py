"""ThetaData access. Phase 1 needs only `calendar_year`; Phase 3 replaces this with the shared production client
(4 slots, bulk limited to 3, retries, outage pause).

Production only (README: Fetching). A login replaces any other session on the account in the same environment, so
each call here logs in, fetches and closes; nothing holds a session between refreshes.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, time

from .calendar import Day
from .config import Secret

log = logging.getLogger(__name__)
TIMEOUT_MS = 60_000


def _ms_time(ms: int) -> time:
    s, ms = divmod(int(ms), 1000)
    return time(s // 3600, s // 60 % 60, s % 60, ms * 1000)


def _int_date(v: int) -> date:
    v = int(v)
    return date(v // 10000, v // 100 % 100, v % 100)


def to_day(c) -> Day:
    """A ThetaData CalendarDay: integer date, status, open_time/close_time in ms of day (0 on a full close)."""
    status = c.status
    if status == "early_close":
        return Day(_int_date(c.date), status, _ms_time(c.open_time), _ms_time(c.close_time))
    return Day(_int_date(c.date), status)


def fetch_calendar_years(api_key: Secret, years: Iterable[int]) -> dict[int, list[Day] | None]:
    """Each year's non-standard days, or None where ThetaData has no calendar for the year (NotFound: a year not
    published yet, or before 2012)."""
    import thetadatadx as tdx  # imported here so tests and the rest of the service don't need a login

    client = tdx.Client(api_key=api_key.reveal())
    try:
        m = client.market_data
        out: dict[int, list[Day] | None] = {}
        for y in years:
            try:
                out[y] = [to_day(c) for c in m.calendar_year(str(y), timeout_ms=TIMEOUT_MS)]
            except tdx.NotFoundError:
                out[y] = None
        return out
    finally:
        client.close()
