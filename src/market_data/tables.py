"""The store's tables: one DuckLake table per kind of data, plus `coverage`.

Every data row carries `trade_date` and `session`, computed from its time and the calendar when written (README:
Decisions), and `unit_id`, the fetch unit it came from, so a refetched unit replaces its rows. Times are
time-zone aware (TIMESTAMPTZ, UTC inside): `ts` for events and snapshots, `bar_start` and `bar_end` for bars. Tables
are partitioned by trade-date year and month; rows are written sorted by root, expiration, strike, right and time.

Columns follow ThetaDataDx 0.9.0's records (its type stubs), converted at ingest in Phase 3: `date` + `ms_of_day`
become `ts`, rights become `C`/`P`, integer dates become DATE.
"""
from __future__ import annotations

from dataclasses import dataclass

from .calendar import Kind
from .store import LAKE, Store

CONTRACT = [("root", "VARCHAR NOT NULL"), ("expiration", "DATE NOT NULL"), ("strike", "DOUBLE NOT NULL"),
            ("right", "VARCHAR NOT NULL")]
QUOTE = [("bid_size", "INTEGER"), ("bid_exchange", "SMALLINT"), ("bid", "DOUBLE"), ("bid_condition", "SMALLINT"),
         ("ask_size", "INTEGER"), ("ask_exchange", "SMALLINT"), ("ask", "DOUBLE"), ("ask_condition", "SMALLINT")]
TRADE = [("sequence", "BIGINT"), ("condition", "SMALLINT"), ("size", "INTEGER"), ("exchange", "SMALLINT"),
         ("price", "DOUBLE"), ("condition_flags", "INTEGER"), ("price_flags", "INTEGER"), ("volume_type", "INTEGER"),
         ("records_back", "INTEGER")]
LABELS = [("trade_date", "DATE NOT NULL"), ("session", "VARCHAR NOT NULL")]
TS = [("ts", "TIMESTAMPTZ NOT NULL")]
BAR = [("bar_start", "TIMESTAMPTZ NOT NULL"), ("bar_end", "TIMESTAMPTZ NOT NULL")]
INTERVAL = [("interval_ms", "INTEGER NOT NULL")]
UNIT = [("unit_id", "BIGINT NOT NULL")]


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[tuple[str, str], ...]
    label: Kind | None     # how trade_date and session are assigned; None: the table has no time column
    time: str | None       # the column labels are computed from: ts, or bar_start for bars

    @property
    def names(self) -> list[str]:
        return [c for c, _ in self.columns]

    @property
    def computed(self) -> set[str]:
        """Columns the writer adds; incoming frames must not carry them."""
        return {"unit_id"} | ({"trade_date", "session"} if self.label else set())

    @property
    def sort(self) -> list[str]:
        keys = [c for c in ("root", "expiration", "strike", "right", "interval_ms") if c in self.names]
        if self.time:
            keys.append(self.time)
        if "seq" in self.names:
            keys.append("seq")
        return keys

    @property
    def partition_col(self) -> str:
        return "trade_date" if self.label else "date"

    def ddl(self) -> list[str]:
        cols = ", ".join(f'"{c}" {t}' for c, t in self.columns)
        p = self.partition_col
        return [f"CREATE TABLE {LAKE}.{self.name} ({cols})",
                f"ALTER TABLE {LAKE}.{self.name} SET PARTITIONED BY (year({p}), month({p}))"]


def _t(name, label, time, *groups) -> Table:
    return Table(name, tuple(c for g in groups for c in g), label, time)


TABLES: dict[str, Table] = {t.name: t for t in [
    # Interval quotes: the quote in effect at each interval's end (a snapshot).
    _t("option_quote", Kind.SNAPSHOT, "ts", TS, LABELS, CONTRACT, INTERVAL, QUOTE, UNIT),
    # Quote ticks in ThetaData's order; seq numbers them per contract and ThetaData date (no source sequence).
    _t("option_quote_tick", Kind.EVENT, "ts", TS, LABELS, CONTRACT, [("seq", "INTEGER NOT NULL")], QUOTE, UNIT),
    # Each trade with the quote in effect when it printed (quote_ts: when that quote was reported).
    _t("option_trade_quote", Kind.EVENT, "ts", TS, LABELS, CONTRACT, TRADE, [("quote_ts", "TIMESTAMPTZ")], QUOTE,
       UNIT),
    # First-order Greeks at each interval's end (a snapshot), with the underlying they were computed from.
    _t("option_greeks", Kind.SNAPSHOT, "ts", TS, LABELS, CONTRACT, INTERVAL,
       [("bid", "DOUBLE"), ("ask", "DOUBLE"), ("delta", "DOUBLE"), ("theta", "DOUBLE"), ("vega", "DOUBLE"),
        ("rho", "DOUBLE"), ("epsilon", "DOUBLE"), ("lambda", "DOUBLE"), ("implied_volatility", "DOUBLE"),
        ("iv_error", "DOUBLE"), ("implied_vol_source", "VARCHAR"), ("underlying_ts", "TIMESTAMPTZ"),
        ("underlying_price", "DOUBLE")], UNIT),
    # Trade bars [bar_start, bar_end); bars with no trades are dropped at ingest (README: Time semantics).
    _t("option_ohlc", Kind.EVENT, "bar_start", BAR, LABELS, CONTRACT, INTERVAL,
       [("open", "DOUBLE NOT NULL"), ("high", "DOUBLE NOT NULL"), ("low", "DOUBLE NOT NULL"),
        ("close", "DOUBLE NOT NULL"), ("volume", "BIGINT NOT NULL"), ("count", "INTEGER NOT NULL"),
        ("vwap", "DOUBLE")], UNIT),
    # Open interest as reported (a snapshot at its report time).
    _t("option_open_interest", Kind.SNAPSHOT, "ts", TS, LABELS, CONTRACT, [("open_interest", "BIGINT NOT NULL")],
       UNIT),
    # Contract lists: what ThetaData lists for a date (`list_type` trade or quote). No time of day, so no labels.
    _t("option_contracts", None, None, [("date", "DATE NOT NULL"), ("list_type", "VARCHAR NOT NULL")], CONTRACT,
       UNIT),
]}

# What was fetched. Written in the same DuckLake transaction as the unit's rows, so the two always agree
# (README: Decisions). Empty answers are rows with n_rows 0. Deleting a row makes the unit fetch again.
COVERAGE = Table("coverage", (
    ("unit_id", "BIGINT NOT NULL"), ("kind", "VARCHAR NOT NULL"), ("root", "VARCHAR NOT NULL"),
    ("date", "DATE NOT NULL"), ("expiration", "DATE"), ("interval_ms", "INTEGER"), ("strike", "DOUBLE"),
    ("right", "VARCHAR"), ("window", "VARCHAR NOT NULL"), ("list_type", "VARCHAR"),
    ("fetched_at", "TIMESTAMPTZ NOT NULL"), ("n_rows", "BIGINT NOT NULL")), None, None)


def ensure_tables(store: Store) -> list[str]:
    """Create the tables that don't exist yet, in one transaction. Returns the names created."""
    with store.cursor() as cur:
        have = {r[0] for r in cur.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ?", [LAKE]).fetchall()}
        todo = [t for t in [*TABLES.values(), COVERAGE] if t.name not in have]
        if not todo:
            return []
        cur.execute("BEGIN")
        try:
            for t in todo:
                stmts = t.ddl() if t is not COVERAGE else t.ddl()[:1]
                for s in stmts:
                    cur.execute(s)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    return [t.name for t in todo]
