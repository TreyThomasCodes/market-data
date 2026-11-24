"""ThetaData calendar answers, fetched through the service's shared client (`theta_client`), so the calendar refresh
never logs in on its own and knocks out the service's session.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, time

from .calendar import Day
from .theta_client import ThetaClient

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


def fetch_calendar_years(client: ThetaClient, years: Iterable[int]) -> dict[int, list[Day] | None]:
    """Each year's non-standard days, or None where ThetaData has no calendar for the year (NotFound: a year not
    published yet, or before 2012)."""
    out: dict[int, list[Day] | None] = {}
    for y in years:
        got = client.call("calendar_year", str(y), timeout_ms=TIMEOUT_MS)
        out[y] = None if got is None else [to_day(c) for c in got]
    return out
