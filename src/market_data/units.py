"""Fetch units: what the service fetches and records in `coverage`, and the read path's first step.

A request is answered from the store when every unit it touches is covered (README: Fetch units, not queries). A
unit is keyed on ThetaData's own date, since that's what its API takes (README: Open questions, session semantics).

Only regular-hours windows are planned for now. RTH is the decided default; what a unit's window is for the other
sessions is still open in the README, so asking for one raises.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from .calendar import Calendar, CalendarError
from .store import LAKE, Store
from .tables import TABLES

WINDOWS = ("rth",)


@dataclass(frozen=True)
class Unit:
    kind: str                       # a table name in TABLES
    root: str
    date: date                      # ThetaData's date
    window: str = "rth"
    expiration: date | None = None
    interval_ms: int | None = None
    strike: float | None = None     # with right: a one-contract unit; both None: the whole chain
    right: str | None = None
    list_type: str | None = None    # option_contracts only: trade or quote

    def __post_init__(self):
        if self.kind not in TABLES:
            raise ValueError(f"unknown kind {self.kind!r}")
        if self.root != self.root.upper() or not self.root:
            raise ValueError(f"root must be upper case: {self.root!r}")
        if (self.strike is None) != (self.right is None):
            raise ValueError("strike and right go together")
        if self.right not in (None, "C", "P"):
            raise ValueError(f"right must be C or P: {self.right!r}")
        if self.window not in WINDOWS:
            raise CalendarError(f"fetch window {self.window!r} isn't settled (README: Open questions); only rth")
        cols = TABLES[self.kind].names
        if ("interval_ms" in cols) != (self.interval_ms is not None):
            raise ValueError(f"{self.kind}: interval_ms is {'required' if 'interval_ms' in cols else 'not used'}")
        if (self.kind == "option_contracts") != (self.list_type is not None):
            raise ValueError("list_type is for option_contracts only, and required there")

    @property
    def key(self) -> str:
        v = lambda x: "" if x is None else x.isoformat() if isinstance(x, date) else f"{x:.3f}" \
            if isinstance(x, float) else str(x)
        return "|".join(v(x) for x in (self.kind, self.root, self.date, self.window, self.expiration,
                                       self.interval_ms, self.strike, self.right, self.list_type))

    @property
    def id(self) -> int:
        """A stable 64-bit id from the key; stored on the unit's rows and its coverage row."""
        return int.from_bytes(hashlib.blake2b(self.key.encode(), digest_size=8).digest(), "big", signed=True)


def plan(cal: Calendar, kind: str, root: str, start: date, end: date, *, expiration: date | None = None,
         interval_ms: int | None = None, strike: float | None = None, right: str | None = None,
         list_type: str | None = None, window: str = "rth") -> list[Unit]:
    """The units a request for start..end touches: one per trading day (RTH exists only on trading days), and for
    an option no later than its expiration."""
    if window not in WINDOWS:
        raise CalendarError(f"fetch window {window!r} isn't settled (README: Open questions); only rth")
    if end < start:
        raise ValueError("end is before start")
    if expiration is not None:
        end = min(end, expiration)
    if end < start:
        return []
    return [Unit(kind, root.upper(), d, window, expiration, interval_ms, strike, right, list_type)
            for d in cal.trading_days(start, end)]


def split(store: Store, units: Iterable[Unit]) -> tuple[list[Unit], list[Unit]]:
    """(covered, missing), each in the order given."""
    units = list(units)
    if not units:
        return [], []
    with store.cursor() as cur:
        have = {r[0] for r in cur.execute(
            f"SELECT unit_id FROM {LAKE}.coverage WHERE unit_id IN (SELECT unnest(?::BIGINT[]))",
            [[u.id for u in units]]).fetchall()}
    return [u for u in units if u.id in have], [u for u in units if u.id not in have]
