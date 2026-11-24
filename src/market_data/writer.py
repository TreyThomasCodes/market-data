"""Writing fetch units into the store, checking stored labels, and compacting.

`write_unit` is the only way data gets in. In one DuckLake transaction it replaces the unit's rows (if it was
fetched before) and its coverage row, so a reader sees a unit either fully there and covered, or not at all
(README: Decisions, coverage in the same transaction; Why DuckLake).
"""
from __future__ import annotations

import itertools
import logging
from datetime import date, datetime, time, timedelta, timezone

import polars as pl

from .calendar import ET, ET_NAME, Calendar, CalendarError
from .store import LAKE, Store
from .tables import TABLES, Table
from .units import Unit

log = logging.getLogger(__name__)
_names = itertools.count()


class WriteError(Exception):
    pass


def _utc(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    out = []
    for c in cols:
        dt = df.schema[c]
        if not isinstance(dt, pl.Datetime) or dt.time_zone is None:
            raise WriteError(f"{c} must be a time-zone aware datetime column, got {dt}")
        out.append(pl.col(c).dt.convert_time_zone("UTC").dt.cast_time_unit("us"))
    return df.with_columns(out) if out else df


def _check_belongs(t: Table, unit: Unit, df: pl.DataFrame) -> None:
    """Every row must be inside the unit: a mismatch is a bug in the fetch layer, and would put rows under a unit
    that a refetch doesn't replace."""
    def bad(expr: pl.Expr, what: str):
        n = df.filter(~expr.fill_null(False)).height
        if n:
            raise WriteError(f"{unit.key}: {n} row(s) with {what}")

    bad(pl.col("root") == unit.root, f"root other than {unit.root}")
    bad(pl.col("right").is_in(["C", "P"]), "right not C or P")
    if unit.expiration is not None:
        bad(pl.col("expiration") == unit.expiration, f"expiration other than {unit.expiration}")
    if unit.interval_ms is not None:
        bad(pl.col("interval_ms") == unit.interval_ms, f"interval_ms other than {unit.interval_ms}")
    if unit.strike is not None:
        bad((pl.col("strike") == unit.strike) & (pl.col("right") == unit.right),
            f"a contract other than {unit.strike} {unit.right}")
    if t.time:
        bad(pl.col(t.time).dt.convert_time_zone(ET_NAME).dt.date() == unit.date,
            f"{t.time} on a date other than {unit.date} (ET)")
    else:
        bad(pl.col("date") == unit.date, f"date other than {unit.date}")
        bad(pl.col("list_type") == unit.list_type, f"list_type other than {unit.list_type}")


def prepare(cal: Calendar, unit: Unit, df: pl.DataFrame) -> pl.DataFrame:
    """The unit's frame as it's stored: checked, labelled, with unit_id, in the table's column order and sort."""
    t = TABLES[unit.kind]
    want = [c for c in t.names if c not in t.computed]
    missing, extra = set(want) - set(df.columns), set(df.columns) - set(want)
    if missing or extra:
        raise WriteError(f"{unit.kind}: missing columns {sorted(missing)}, unexpected {sorted(extra)}")
    df = _utc(df, [c for c, typ in t.columns if typ.startswith("TIMESTAMPTZ") and c in df.columns])
    _check_belongs(t, unit, df)
    if t.label:
        frame = df.select(pl.col(t.time).alias("ts"), "root", "expiration")
        try:
            labels = cal.label_frame(frame, t.label).select("trade_date", "session")
        except CalendarError as e:
            raise WriteError(f"{unit.key}: can't label: {e}") from None
        df = df.with_columns(labels)
    df = df.with_columns(pl.lit(unit.id, pl.Int64).alias("unit_id"))
    return df.select(t.names).sort(t.sort, maintain_order=True)


def write_unit(store: Store, cal: Calendar, unit: Unit, df: pl.DataFrame,
               fetched_at: datetime | None = None) -> int:
    """Store a fetched unit: its rows (none for an empty answer) and its coverage row, in one transaction. A unit
    stored before is replaced. Returns the row count."""
    t = TABLES[unit.kind]
    df = prepare(cal, unit, df)
    fetched_at = fetched_at or datetime.now(timezone.utc)
    # Where an earlier copy's rows can be, for partition pruning: ThetaData's date, or the trade date GTH rows
    # after 20:15 (or on a non-trading day) roll to.
    lo, hi = unit.date, (cal.next_trading(unit.date) if t.label else unit.date)
    name = f"incoming_{next(_names)}"
    with store.cursor() as cur:
        cur.register(name, df.to_arrow())
        try:
            cur.execute("BEGIN")
            try:
                if cur.execute(f"SELECT count(*) FROM {LAKE}.coverage WHERE unit_id = ?", [unit.id]).fetchone()[0]:
                    cur.execute(f'DELETE FROM {LAKE}.{t.name} WHERE unit_id = ? AND "{t.partition_col}" BETWEEN ? AND ?',
                                [unit.id, lo, hi])
                    cur.execute(f"DELETE FROM {LAKE}.coverage WHERE unit_id = ?", [unit.id])
                if df.height:
                    cur.execute(f"INSERT INTO {LAKE}.{t.name} BY NAME SELECT * FROM {name}")
                cur.execute(
                    f"INSERT INTO {LAKE}.coverage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [unit.id, unit.kind, unit.root, unit.date, unit.expiration, unit.interval_ms, unit.strike,
                     unit.right, unit.window, unit.list_type, fetched_at, df.height])
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        finally:
            cur.unregister(name)
    return df.height


def check_labels(store: Store, cal: Calendar, table: str, start: date, end: date, examples: int = 5) -> dict:
    """Recompute trade_date and session for the rows of trade dates start..end (and any row whose time falls in
    those dates, in case its stored trade_date is wrong) and compare with what's stored."""
    t = TABLES[table]
    if not t.label:
        raise ValueError(f"{table} has no labels")
    t0 = datetime.combine(start, time(0), ET) - timedelta(days=1)
    t1 = datetime.combine(end, time(0), ET) + timedelta(days=1)
    with store.cursor() as cur:
        df = cur.sql(
            f'SELECT "{t.time}" AS ts, root, expiration, trade_date, session FROM {LAKE}.{t.name} '
            f'WHERE trade_date BETWEEN ? AND ? OR ("{t.time}" >= ? AND "{t.time}" < ?)',
            params=[start, end, t0, t1]).pl()
    if not df.height:
        return {"rows": 0, "mismatched": 0, "examples": []}
    again = cal.label_frame(df.drop("trade_date", "session"), t.label)
    df = df.with_columns(again.select(pl.col("trade_date").alias("want_trade_date"),
                                      pl.col("session").alias("want_session")))
    bad = df.filter((pl.col("trade_date") != pl.col("want_trade_date")) | (pl.col("session") != pl.col("want_session")))
    return {"rows": df.height, "mismatched": bad.height, "examples": bad.head(examples).to_dicts()}


def compact(store: Store, grace: timedelta) -> None:
    """Merge small files, expire snapshots older than `grace`, and delete files no snapshot needs that were
    retired more than `grace` ago. `grace` must be longer than the longest query, so a running query never loses a
    file (README: Store layout). The schedule and grace period wait for the merge spike."""
    if grace <= timedelta(0):
        raise ValueError("grace must be positive")
    cutoff = datetime.now(timezone.utc) - grace
    with store.cursor() as cur:
        cur.execute(f"CALL ducklake_merge_adjacent_files('{LAKE}')")
        cur.execute(f"CALL ducklake_expire_snapshots('{LAKE}', older_than => ?::TIMESTAMPTZ)", [cutoff])
        cur.execute(f"CALL ducklake_cleanup_old_files('{LAKE}', older_than => ?::TIMESTAMPTZ)", [cutoff])


# A calendar change on date c can move the labels of rows dated a few days before it (their next trading day) and
# after it (sessions of the trade dates around it). Ten days either side covers a weekend plus a holiday run.
RELABEL_REACH = timedelta(days=10)


def relabel_unit(store: Store, cal: Calendar, kind: str, unit_id: int, d: date) -> int:
    """Recompute a stored unit's labels; if any changed, replace its rows with relabelled ones in one transaction
    (coverage is untouched: the unit and its row count are the same). Returns the number of rows whose labels
    changed."""
    t = TABLES[kind]
    lo, hi = d, d + RELABEL_REACH  # where its rows can be under any calendar
    where = f'unit_id = ? AND "{t.partition_col}" BETWEEN ? AND ?'
    name = f"relabel_{next(_names)}"
    with store.cursor() as cur:
        cur.execute("BEGIN")
        try:
            df = cur.sql(f"SELECT * FROM {LAKE}.{t.name} WHERE {where}", params=[unit_id, lo, hi]).pl()
            if not df.height:
                cur.execute("COMMIT")
                return 0
            frame = df.select(pl.col(t.time).alias("ts"), "root", "expiration")
            new = cal.label_frame(frame, t.label).select("trade_date", "session")
            moved = df.filter((df["trade_date"] != new["trade_date"]) | (df["session"] != new["session"])).height
            if not moved:
                cur.execute("COMMIT")
                return 0
            out = df.with_columns(new).select(t.names).sort(t.sort, maintain_order=True)
            cur.register(name, out.to_arrow())
            try:
                cur.execute(f"DELETE FROM {LAKE}.{t.name} WHERE {where}", [unit_id, lo, hi])
                cur.execute(f"INSERT INTO {LAKE}.{t.name} BY NAME SELECT * FROM {name}")
            finally:
                cur.unregister(name)
            cur.execute("COMMIT")
            return moved
        except Exception:
            cur.execute("ROLLBACK")
            raise


def relabel_pending(store: Store, cal: Calendar) -> dict:
    """Relabel the stored units near every date in `relabel_pending`, then clear those dates. Dates noted while
    this runs stay for the next call. A failure leaves everything pending (units already done are just checked
    again next time). Returns counts."""
    with store.cursor() as cur:
        pending = cur.execute(f"SELECT date, max(noted_at) FROM {LAKE}.relabel_pending GROUP BY 1").fetchall()
    if not pending:
        return {"dates": 0, "units": 0, "rows": 0}
    lo = min(d for d, _ in pending) - RELABEL_REACH
    hi = max(d for d, _ in pending) + RELABEL_REACH
    reach = {d + timedelta(days=i) for d, _ in pending
             for i in range(-RELABEL_REACH.days, RELABEL_REACH.days + 1)}
    kinds = [k for k, t in TABLES.items() if t.label]
    with store.cursor() as cur:
        units = cur.execute(
            f"SELECT unit_id, kind, date FROM {LAKE}.coverage WHERE date BETWEEN ? AND ? AND n_rows > 0 "
            "AND kind IN (SELECT unnest(?::VARCHAR[])) ORDER BY date, kind", [lo, hi, kinds]).fetchall()
    units = [u for u in units if u[2] in reach]
    rows = sum(relabel_unit(store, cal, kind, uid, d) for uid, kind, d in units)
    with store.cursor() as cur:
        cur.executemany(f"DELETE FROM {LAKE}.relabel_pending WHERE date = ? AND noted_at <= ?", pending)
    log.info("relabelled %d row(s) in %d unit(s) near %d changed calendar date(s)", rows, len(units), len(pending))
    return {"dates": len(pending), "units": len(units), "rows": rows}
