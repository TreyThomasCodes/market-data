"""The DuckLake store: one DuckDB connection, one attachment, a cursor per caller.

Phase 0 only attaches the catalog; the tables come in Phase 2.

Errors from DuckDB's postgres extension can carry connection details, so nothing from them is
shown to clients, and what is logged has the password scrubbed out first.
"""
from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Iterator
from pathlib import Path

import duckdb

from .config import Settings

log = logging.getLogger(__name__)

CATALOG_FORMAT = "1.1"  # pinned in the README's build plan
LAKE = "lake"
CLOSE_WAIT_SECS = 10


class StoreError(Exception):
    pass


def _quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_path: Path = settings.data_dir / "lake"
        self._con: duckdb.DuckDBPyConnection | None = None
        self._cond = threading.Condition()
        self._active = 0  # cursors handed out and not yet closed

    def scrub(self, text: str) -> str:
        pw = self.settings.catalog_password.reveal()
        return text.replace(pw, "<redacted>") if pw else text

    def attach(self) -> None:
        try:
            self._attach()
        except Exception as e:
            # `from None`: the original exception's text isn't scrubbed, so it isn't chained.
            raise StoreError(f"attach failed: {type(e).__name__}: {self.scrub(str(e))}") from None

    def _attach(self) -> None:
        s = self.settings
        self.data_path.mkdir(parents=True, exist_ok=True)
        # ts is UTC inside (README: Time semantics). Set at connect so every cursor inherits it;
        # a session SET doesn't carry over to cursors.
        con = duckdb.connect(config={"TimeZone": "UTC"})
        try:
            con.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres")
            # An unnamed secret is the default for postgres: ATTACHes. Keeps the password out of
            # the ATTACH string; duckdb_secrets() shows it redacted.
            con.execute(
                "CREATE SECRET (TYPE postgres, "
                f"HOST {_quote(s.catalog_host)}, PORT {s.catalog_port}, USER {_quote(s.catalog_user)}, "
                f"PASSWORD {_quote(s.catalog_password.reveal())}, DATABASE {_quote(s.catalog_db)})"
            )
            con.execute(f"ATTACH 'ducklake:postgres:' AS {LAKE} (DATA_PATH {_quote(str(self.data_path) + '/')})")
            fmt = self._catalog_format(con)
            if fmt != CATALOG_FORMAT:
                raise StoreError(f"catalog format is {fmt}, expected {CATALOG_FORMAT}")
        except Exception:
            con.close()
            raise
        with self._cond:
            self._con = con
        log.info("store attached: catalog %s@%s:%s/%s format %s, data %s",
                 s.catalog_user, s.catalog_host, s.catalog_port, s.catalog_db, fmt, self.data_path)

    @staticmethod
    def _catalog_format(con: duckdb.DuckDBPyConnection) -> str | None:
        row = con.execute(
            f"SELECT value FROM __ducklake_metadata_{LAKE}.ducklake_metadata WHERE key = 'version'"
        ).fetchone()
        return row[0] if row else None

    @contextlib.contextmanager
    def cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """A cursor for one caller; close() waits for it to be returned."""
        with self._cond:
            if self._con is None:
                raise StoreError("store isn't attached")
            cur = self._con.cursor()
            self._active += 1
        try:
            yield cur
        finally:
            cur.close()
            with self._cond:
                self._active -= 1
                self._cond.notify_all()

    def health(self) -> dict:
        """Catalog reachable, its format, and the latest snapshot. Errors name only their type."""
        try:
            with self.cursor() as cur:
                fmt = self._catalog_format(cur)
                snap = cur.execute(
                    f"SELECT snapshot_id, snapshot_time FROM ducklake_snapshots('{LAKE}') "
                    "ORDER BY snapshot_id DESC LIMIT 1"
                ).fetchone()
        except Exception as e:
            log.warning("store health check failed: %s: %s", type(e).__name__, self.scrub(str(e)))
            return {"ok": False, "error": type(e).__name__}
        return {
            "ok": True,
            "catalog_format": fmt,
            "latest_snapshot": snap[0] if snap else None,
            "latest_snapshot_time": snap[1].isoformat() if snap else None,
            "data_path": str(self.data_path),
        }

    def close(self) -> None:
        """Stop handing out cursors, wait for those in use, then close the connection."""
        with self._cond:
            con, self._con = self._con, None
            if con is None:
                return
            if not self._cond.wait_for(lambda: self._active == 0, timeout=CLOSE_WAIT_SECS):
                log.warning("closing the store with %d cursor(s) still in use", self._active)
            con.close()
