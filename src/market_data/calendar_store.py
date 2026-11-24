"""The stored calendar and the service that keeps it current.

ThetaData's `calendar_year` answers are stored in two DuckLake tables: `calendar_days` (the non-standard days) and
`calendar_years` (when each year was fetched, and how many rows). A year and its rows are written in one transaction.

Past years are kept forever, like every other past date (README: Freshness). The current and next year are
refetched once a day, because closures can be announced at short notice (2025-01-09 was). A change is logged, and
the changed dates go into `relabel_pending` in the same transaction as the new year, so stored rows near them are
relabelled (`writer.relabel_pending`) even if the service stops before that runs.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta, timezone

from .calendar import ET, FIRST_YEAR, Calendar, CalendarError, Day, from_rows
from .store import LAKE, Store

log = logging.getLogger(__name__)

REFETCH_AFTER = timedelta(hours=24)  # also how long an unpublished next year waits before it's asked for again
CHECK_EVERY_SECS = 3600
RETRY_AFTER_SECS = 300
STOP_WAIT_SECS = 90  # longer than one calendar_year call's timeout

DDL = [
    f"CREATE TABLE IF NOT EXISTS {LAKE}.calendar_days (date DATE NOT NULL, status VARCHAR NOT NULL, "
    "open_time TIME, close_time TIME)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.calendar_years (year INTEGER NOT NULL, fetched_at TIMESTAMPTZ NOT NULL, "
    "n_rows INTEGER NOT NULL)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.relabel_pending (date DATE NOT NULL, noted_at TIMESTAMPTZ NOT NULL)",
]

Fetch = Callable[[list[int]], dict[int, list[Day] | None]]


def ensure_tables(store: Store) -> None:
    with store.cursor() as cur:
        for sql in DDL:
            cur.execute(sql)


def load(store: Store) -> tuple[list[Day], dict[int, datetime]]:
    with store.cursor() as cur:
        days = [Day(d, s, o, c) for d, s, o, c in cur.execute(
            f"SELECT date, status, open_time, close_time FROM {LAKE}.calendar_days ORDER BY date").fetchall()]
        years = dict(cur.execute(f"SELECT year, fetched_at FROM {LAKE}.calendar_years").fetchall())
    return days, years


def save_year(store: Store, year: int, days: list[Day], fetched_at: datetime,
              changed: Iterable[date] = ()) -> None:
    """Store a year's calendar_year answer; `changed` dates (where it differs from what was stored) are queued for
    relabelling in the same transaction."""
    for d in days:
        if d.date.year != year:
            raise CalendarError(f"calendar_year {year} returned {d.date}")
    with store.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(f"DELETE FROM {LAKE}.calendar_days WHERE year(date) = ?", [year])
            cur.execute(f"DELETE FROM {LAKE}.calendar_years WHERE year = ?", [year])
            if days:
                cur.executemany(f"INSERT INTO {LAKE}.calendar_days VALUES (?, ?, ?, ?)",
                                [(d.date, d.status, d.open, d.close) for d in days])
            cur.execute(f"INSERT INTO {LAKE}.calendar_years VALUES (?, ?, ?)", [year, fetched_at, len(days)])
            changed = sorted(set(changed))
            if changed:
                cur.executemany(f"INSERT INTO {LAKE}.relabel_pending VALUES (?, ?)", [(d, fetched_at) for d in changed])
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise


class CalendarService:
    """Holds the current `Calendar`; loads it from the store and refreshes it from ThetaData in a thread."""

    def __init__(self, store: Store, fetch: Fetch | None, now: Callable[[], datetime] | None = None,
                 on_change: Callable[[Calendar], object] | None = None):
        """`on_change` runs after every refresh check with the current calendar (the service relabels pending
        dates there); it's cheap when nothing is pending, and a failure is retried at the next check."""
        self.store = store
        self.fetch = fetch
        self.on_change = on_change
        self.last_relabel_error: str | None = None
        self.now = now or (lambda: datetime.now(timezone.utc))
        self._cal: Calendar | None = None
        self._fetched: dict[int, datetime] = {}
        self._unpublished: dict[int, datetime] = {}  # next year, asked for and not published yet
        self._lock = threading.Lock()      # one refresh at a time
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_refresh: datetime | None = None
        self.last_error: str | None = None

    @property
    def calendar(self) -> Calendar | None:
        return self._cal

    def start(self, background: bool = True) -> None:
        """Load the stored calendar and refresh it once before returning, so the service starts with a calendar
        whenever ThetaData answers; then keep it fresh in a thread. A failed refresh doesn't stop the start.
        Blocking (it may fetch): call it from a worker thread, not the event loop."""
        ensure_tables(self.store)
        self.reload()
        self._refresh_logged()
        if background:
            self._thread = threading.Thread(target=self._run, name="calendar-refresh", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop the refresh thread. A refresh in flight finishes its fetch but stores nothing after this."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=STOP_WAIT_SECS)
            if self._thread.is_alive():
                log.warning("calendar refresh thread still running at stop")

    def reload(self) -> None:
        days, fetched = load(self.store)
        self._fetched = fetched
        # The calendar covers consecutive years from FIRST_YEAR; a gap ends it.
        years, y = [], FIRST_YEAR
        while y in fetched:
            years.append(y)
            y += 1
        if not years:
            self._cal = None
            return
        self._cal = from_rows([d for d in days if d.date.year <= years[-1]], years)

    def due(self) -> list[int]:
        """Years to fetch now: those never fetched (FIRST_YEAR to next year), and the current and next year when their
        last fetch is older than REFETCH_AFTER. A next year ThetaData hasn't published is asked for again only after
        REFETCH_AFTER, since every fetch is a production login."""
        now = self.now()
        this = now.astimezone(ET).year
        out = []
        for y in range(FIRST_YEAR, this + 2):
            at = self._fetched.get(y) or self._unpublished.get(y)
            if at is None or (y >= this and now - at > REFETCH_AFTER):
                out.append(y)
        return out

    def refresh(self) -> list[int]:
        """Fetch the due years and store them. Returns the years stored. Without a fetch (no ThetaData key) it
        stores nothing and the service serves what's stored."""
        with self._lock:
            due = self.due()
            if not due or self.fetch is None:
                return []
            got = self.fetch(due)
            now = self.now()
            this = now.astimezone(ET).year
            missing = [y for y in due if got.get(y) is None and y <= this]
            if missing:  # checked before anything is stored, so a refresh stores all its years or none
                raise CalendarError(f"ThetaData has no calendar for {missing}")
            stored = []
            try:
                old_days, _ = load(self.store)
                for y in due:
                    rows = got.get(y)
                    if rows is None:
                        self._unpublished[y] = now
                        continue
                    if self._stop.is_set():
                        break
                    old = {d for d in old_days if d.date.year == y}
                    changed = set()
                    if y in self._fetched and old != set(rows):
                        log.warning("calendar %d changed: removed %s, added %s", y,
                                    sorted(d.date.isoformat() for d in old - set(rows)),
                                    sorted(d.date.isoformat() for d in set(rows) - old))
                        changed = {d.date for d in old ^ set(rows)}
                    save_year(self.store, y, rows, now, changed)
                    self._unpublished.pop(y, None)
                    stored.append(y)
            finally:
                if stored:
                    self.reload()
            self.last_refresh = now
            return stored

    def _refresh_logged(self) -> bool:
        ok = True
        try:
            stored = self.refresh()
            if stored:
                log.info("calendar years stored: %s", stored)
            self.last_error = None
        except Exception as e:
            # Our own errors are shown; anything else by type only, in case it carries account details.
            self.last_error = str(e) if isinstance(e, CalendarError) else type(e).__name__
            log.warning("calendar refresh failed: %s", self.last_error)
            ok = False
        self._relabel_logged()
        return ok

    def _relabel_logged(self) -> None:
        if self.on_change is None or self._cal is None:
            return
        try:
            self.on_change(self._cal)
            self.last_relabel_error = None
        except Exception as e:
            self.last_relabel_error = f"{type(e).__name__}: {e}"
            log.warning("relabelling failed (retried at the next check): %s", self.last_relabel_error)

    def _run(self) -> None:
        ok = self.last_error is None
        while not self._stop.wait(CHECK_EVERY_SECS if ok else RETRY_AFTER_SECS):
            ok = self._refresh_logged()

    def health(self) -> dict:
        """ok: a calendar is loaded and covers today. A failed refresh shows in `error` without failing it, since
        the stored calendar is still served."""
        cal = self._cal
        today = self.now().astimezone(ET).date()
        return {
            "ok": cal is not None and cal.last >= today,
            "can_fetch": self.fetch is not None,
            "first": cal.first.isoformat() if cal else None,
            "last": cal.last.isoformat() if cal else None,
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "error": self.last_error,
            "relabel_error": self.last_relabel_error,
        }
