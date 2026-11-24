"""The one ThetaData production client the service shares (README: Fetching).

- One login. A new login replaces any other session on the account in the same environment, so everything that
  talks to ThetaData, the calendar refresh included, goes through this client.
- Slots: 4 requests in flight at most, bulk work at most 3 of them, so a long job never locks out a client that's
  waiting. Interactive requests waiting go first.
- Errors, by ThetaDataDx's typed exceptions:
  - `NotFoundError`: an empty answer (ThetaData answers an empty window that way). Returned as None.
  - `RateLimitError` (`RESOURCE_EXHAUSTED`): retry shortly; never counted as an outage. ThetaDataDx retries these
    itself first.
  - `UnavailableError`, `NetworkError`, `DeadlineExceededError`: outage-type. The request is retried, not dropped.
    When they come together (OUTAGE_FAILURES within OUTAGE_WINDOW), every request waits while one cheap probe runs
    every PROBE_EVERY until ThetaData answers.
  - Anything else (authentication, subscription, invalid parameters, schema mismatch): raised to the caller.

The thresholds are first guesses, not measured: how ThetaData fails during an outage hasn't been observed through
ThetaDataDx yet (README: Open questions).
"""
from __future__ import annotations

import contextlib
import logging
import random
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from .calendar import ET
from .config import Secret

log = logging.getLogger(__name__)

SLOTS = 4                       # the account's limit (Options Standard)
BULK_SLOTS = 3
OUTAGE_FAILURES = 3
OUTAGE_WINDOW = timedelta(seconds=60)
PROBE_EVERY = 90.0              # seconds between probes while paused (README: every minute or two)
RETRY_BACKOFF = (1.0, 30.0)     # first and longest wait between retries of one request, seconds
RATE_LIMIT_WAIT = 2.0           # when RateLimitError carries no retry_after


class Priority(str, Enum):
    INTERACTIVE = "interactive"
    BULK = "bulk"


class Slots:
    """At most `total` holders, at most `bulk` of them bulk; waiting interactive requests go before bulk."""

    def __init__(self, total: int = SLOTS, bulk: int = BULK_SLOTS):
        if not 0 < bulk <= total:
            raise ValueError("need 0 < bulk <= total")
        self.total, self.bulk = total, bulk
        self._cond = threading.Condition()
        self.in_use = 0
        self.bulk_in_use = 0
        self._interactive_waiting = 0

    @contextlib.contextmanager
    def take(self, priority: Priority) -> Iterator[None]:
        bulk = priority is Priority.BULK
        with self._cond:
            if not bulk:
                self._interactive_waiting += 1
            try:
                self._cond.wait_for(lambda: self.in_use < self.total and (
                    not bulk or (self.bulk_in_use < self.bulk and not self._interactive_waiting)))
            finally:
                if not bulk:
                    self._interactive_waiting -= 1
            self.in_use += 1
            self.bulk_in_use += bulk
        try:
            yield
        finally:
            with self._cond:
                self.in_use -= 1
                self.bulk_in_use -= bulk
                self._cond.notify_all()


class ThetaUnavailable(Exception):
    """The client was closed, or no API key is configured."""


def _tdx():
    import thetadatadx as tdx  # imported lazily: tests and a key-less service don't need it loaded
    return tdx


def connect_production(api_key: Secret):
    """A ThetaDataDx client on production with the pool fixed at our slot count and ThetaDataDx's own bulk
    sharding off, so every unit is one request in the server's own order."""
    tdx = _tdx()
    cfg = tdx.Config.production()
    cfg.max_concurrent_requests = SLOTS
    cfg.bulk_fetch = "off"
    return tdx.Client(api_key=api_key.reveal(), config=cfg)


class ThetaClient:
    def __init__(self, connect: Callable[[], Any] | None, errors: Any = None,
                 now: Callable[[], datetime] | None = None, sleep: Callable[[float], None] = time.sleep):
        """`connect` makes the ThetaDataDx client (None: no key, every call raises ThetaUnavailable). `errors` is the
        module holding ThetaDataDx's exception classes (tests pass a stand-in)."""
        self._connect = connect
        self._errors = errors
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep
        self.slots = Slots()
        self._client = None
        self._lock = threading.Lock()           # connecting and closing
        self._closed = False
        self._state = threading.Condition()     # outage state
        self._failures: list[datetime] = []
        self.paused_since: datetime | None = None
        self._prober: threading.Thread | None = None
        self.requests = 0
        self.last_error: str | None = None

    # -- connection

    @property
    def errors(self):
        if self._errors is None:
            self._errors = _tdx()
        return self._errors

    def _market_data(self):
        with self._lock:
            if self._closed or self._connect is None:
                raise ThetaUnavailable("ThetaData client closed" if self._closed else "no ThetaData API key")
            if self._client is None:
                self._client = self._connect()
                log.info("ThetaData: logged in to production")
            return self._client.market_data

    def close(self) -> None:
        with self._lock:
            self._closed = True
            client, self._client = self._client, None
        with self._state:
            self._state.notify_all()
        if client is not None:
            client.close()

    # -- outages

    def _outage_error(self, e: Exception) -> bool:
        x = self.errors
        return isinstance(e, (x.UnavailableError, x.NetworkError, x.DeadlineExceededError))

    def _note_failure(self, e: Exception) -> None:
        now = self.now()
        with self._state:
            self._failures = [t for t in self._failures if now - t < OUTAGE_WINDOW] + [now]
            if self.paused_since is None and len(self._failures) >= OUTAGE_FAILURES:
                self.paused_since = now
                log.warning("ThetaData: %d failures within %ss (last %s); pausing requests until a probe answers",
                            len(self._failures), OUTAGE_WINDOW.seconds, type(e).__name__)
                self._prober = threading.Thread(target=self._probe_loop, name="thetadata-probe", daemon=True)
                self._prober.start()

    def _note_success(self) -> None:
        with self._state:
            self._failures.clear()

    def _probe_loop(self) -> None:
        while True:
            self.sleep(PROBE_EVERY)
            if self._closed:
                return
            try:
                with self.slots.take(Priority.INTERACTIVE):
                    self._market_data().calendar_year(str(self.now().astimezone(ET).year), timeout_ms=30_000)
            except Exception as e:
                if isinstance(e, ThetaUnavailable):
                    return
                log.info("ThetaData probe failed (%s); still paused", type(e).__name__)
                continue
            with self._state:
                log.warning("ThetaData answered again after %s; resuming", self.now() - self.paused_since)
                self.paused_since = None
                self._failures.clear()
                self._state.notify_all()
            return

    def _wait_until_open(self) -> None:
        with self._state:
            self._state.wait_for(lambda: self.paused_since is None or self._closed)

    # -- calls

    def call(self, method: str, *args, priority: Priority = Priority.INTERACTIVE, **kwargs):
        """Run one ThetaDataDx market-data call under the rules above. Returns its result, or None for NotFound."""
        attempt = 0
        while True:
            self._wait_until_open()
            md = self._market_data()
            try:
                with self.slots.take(priority):
                    self.requests += 1
                    result = getattr(md, method)(*args, **kwargs)
            except self.errors.NotFoundError:
                self._note_success()
                return None
            except self.errors.RateLimitError as e:
                wait = e.retry_after if getattr(e, "retry_after", None) else RATE_LIMIT_WAIT
                log.info("ThetaData %s: rate limited, retrying in %.1fs", method, wait)
                self.sleep(wait)
                continue
            except Exception as e:
                if not self._outage_error(e):
                    self.last_error = type(e).__name__
                    raise
                attempt += 1
                self.last_error = type(e).__name__
                self._note_failure(e)
                lo, hi = RETRY_BACKOFF
                wait = min(hi, lo * 2 ** (attempt - 1)) * (0.5 + random.random() / 2)
                log.info("ThetaData %s failed (%s, attempt %d); retrying in %.1fs", method, type(e).__name__,
                         attempt, wait)
                self.sleep(wait)
                continue
            self._note_success()
            return result

    def health(self) -> dict:
        return {
            "connected": self._client is not None,
            "can_fetch": self._connect is not None and not self._closed,
            "slots_in_use": self.slots.in_use,
            "bulk_in_use": self.slots.bulk_in_use,
            "paused_since": self.paused_since.isoformat() if self.paused_since else None,
            "requests": self.requests,
            "last_error": self.last_error,
        }
