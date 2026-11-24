"""Phase 2: the tables, fetch units, coverage, the write path and the label check, against the real catalog instance
in a throwaway database. Data is synthetic: nothing is fetched in Phase 2."""
from datetime import date, datetime, time, timedelta, timezone

import polars as pl
import pytest

from market_data.calendar import ET, CalendarError
from market_data.store import LAKE, Store
from market_data.tables import COVERAGE, TABLES, ensure_tables
from market_data.units import Unit, plan, split
from market_data.writer import WriteError, check_labels, prepare, write_unit

D = date(2024, 3, 14)  # a Thursday; SPXW expiring that day ends regular hours at 16:00


@pytest.fixture
def lake(settings):
    s = Store(settings)
    s.attach()
    ensure_tables(s)
    yield s
    s.close()


def quotes(root: str, exp: date, d: date, strikes=(5100.0, 5150.0), start=time(9, 30), end=time(16, 15),
           step_min: int = 1, bid: float = 1.0) -> pl.DataFrame:
    """A synthetic interval-quote chain: every contract at every interval end from start to end (ET)."""
    t0, t1 = datetime.combine(d, start, ET), datetime.combine(d, end, ET)
    n = int((t1 - t0).total_seconds() // (step_min * 60)) + 1
    ts = [(t0 + timedelta(minutes=step_min * i)).astimezone(timezone.utc) for i in range(n)]
    rows = [(t, root, exp, k, r) for k in strikes for r in ("C", "P") for t in ts]
    df = pl.DataFrame(rows, orient="row", schema={"ts": pl.Datetime("us", "UTC"), "root": pl.String,
                                                  "expiration": pl.Date, "strike": pl.Float64, "right": pl.String})
    return df.with_columns(interval_ms=pl.lit(step_min * 60_000, pl.Int32), bid_size=pl.lit(10, pl.Int32),
                           bid_exchange=pl.lit(5, pl.Int16), bid=pl.lit(bid), bid_condition=pl.lit(50, pl.Int16),
                           ask_size=pl.lit(12, pl.Int32), ask_exchange=pl.lit(5, pl.Int16), ask=pl.lit(bid + 0.1),
                           ask_condition=pl.lit(50, pl.Int16))


def unit(d=D, exp=D, **kw) -> Unit:
    return Unit("option_quote", "SPXW", d, expiration=exp, interval_ms=60_000, **kw)


def count(store, table, unit_id=None) -> int:
    with store.cursor() as cur:
        q = f"SELECT count(*) FROM {LAKE}.{table}" + (" WHERE unit_id = ?" if unit_id is not None else "")
        return cur.execute(q, [unit_id] if unit_id is not None else []).fetchone()[0]


# ---------------------------------------------------------------- tables

def test_tables_created_once_and_partitioned(lake):
    assert ensure_tables(lake) == []  # the fixture created them
    with lake.cursor() as cur:
        have = {r[0] for r in cur.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ?", [LAKE]).fetchall()}
        assert set(TABLES) | {COVERAGE.name} <= have
        parts = cur.execute(
            f"SELECT count(DISTINCT table_id) FROM __ducklake_metadata_{LAKE}.ducklake_partition_info").fetchone()[0]
    assert parts == len(TABLES)


def test_every_labelled_table_has_its_columns():
    for t in TABLES.values():
        names = t.names
        assert "unit_id" in names
        if t.label:
            assert {"trade_date", "session", t.time} <= set(names)
        assert ("seq" in names) == (t.name == "option_quote_tick")
    assert {"bar_start", "bar_end"} <= set(TABLES["option_ohlc"].names)


# ---------------------------------------------------------------- units

def test_unit_id_is_stable_and_distinct():
    a, b = unit(), unit()
    assert a.id == b.id and a.key == b.key
    assert unit(strike=5100.0, right="C").id != a.id
    assert Unit("option_quote", "SPXW", D, expiration=D, interval_ms=1000).id != a.id
    assert a.key == "option_quote|SPXW|2024-03-14|rth|2024-03-14|60000|||"


def test_unit_rejects_bad_fields():
    with pytest.raises(ValueError):
        Unit("option_quote", "spxw", D, expiration=D, interval_ms=60_000)
    with pytest.raises(ValueError):
        Unit("option_quote", "SPXW", D, expiration=D)  # interval_ms required
    with pytest.raises(ValueError):
        Unit("option_quote_tick", "SPXW", D, expiration=D, interval_ms=1000)  # ticks have no interval
    with pytest.raises(ValueError):
        unit(strike=5100.0)
    with pytest.raises(CalendarError):
        unit(window="gth")  # other sessions' windows aren't settled


def test_plan_trading_days_up_to_expiration(cal):
    us = plan(cal, "option_quote", "spxw", date(2024, 3, 28), date(2024, 4, 5), expiration=date(2024, 4, 2),
              interval_ms=60_000)
    # Good Friday 2024-03-29 is closed; the weekend has no RTH; nothing after the expiration.
    assert [u.date for u in us] == [date(2024, 3, 28), date(2024, 4, 1), date(2024, 4, 2)]
    assert all(u.root == "SPXW" and u.expiration == date(2024, 4, 2) for u in us)
    assert plan(cal, "option_quote", "SPXW", date(2024, 4, 3), date(2024, 4, 5), expiration=date(2024, 4, 2),
                interval_ms=60_000) == []
    with pytest.raises(CalendarError):
        plan(cal, "option_quote", "SPXW", D, D, expiration=D, interval_ms=60_000, window="curb")


# ---------------------------------------------------------------- writing

def test_write_labels_and_covers(lake, cal):
    df = quotes("SPXW", D, D)
    u = unit()
    assert split(lake, [u]) == ([], [u])
    n = write_unit(lake, cal, u, df)
    assert n == df.height == 2 * 2 * 406
    assert split(lake, [u]) == ([u], [])
    with lake.cursor() as cur:
        got = cur.sql(
            f"SELECT ts, trade_date, session FROM {LAKE}.option_quote WHERE strike = 5100 AND \"right\" = 'C' "
            "ORDER BY ts").pl()
        cov = cur.execute(f"SELECT kind, root, date, n_rows, \"window\" FROM {LAKE}.coverage").fetchall()
    assert cov == [("option_quote", "SPXW", D, n, "rth")]
    assert got["trade_date"].unique().to_list() == [D]
    by_time = dict(zip(got["ts"].dt.convert_time_zone("America/New_York").dt.time(), got["session"]))
    assert by_time[time(9, 30)] == "closed"   # the 09:30:00 snapshot is pre-open state
    assert by_time[time(9, 31)] == "rth"
    assert by_time[time(16, 0)] == "rth"      # the close snapshot is inside (start, end]
    assert by_time[time(16, 1)] == "closed"   # expiring SPXW ends at 16:00


def test_rows_are_stored_sorted(lake, cal):
    df = quotes("SPXW", D, D).sample(fraction=1.0, shuffle=True, seed=1)
    write_unit(lake, cal, unit(), df)
    with lake.cursor() as cur:
        got = cur.sql(f"SELECT strike, \"right\", ts FROM {LAKE}.option_quote").pl()
    assert got.equals(got.sort("strike", "right", "ts"))


def test_empty_answer_is_covered(lake, cal):
    u = unit(d=date(2024, 3, 15), exp=date(2024, 3, 15))
    assert write_unit(lake, cal, u, quotes("SPXW", D, D).clear()) == 0
    assert split(lake, [u]) == ([u], [])
    assert count(lake, "option_quote") == 0


def test_refetch_replaces_rows(lake, cal):
    u = unit()
    write_unit(lake, cal, u, quotes("SPXW", D, D, bid=1.0))
    other = Unit("option_quote", "SPXW", D, expiration=date(2024, 3, 15), interval_ms=60_000)
    write_unit(lake, cal, other, quotes("SPXW", date(2024, 3, 15), D))
    n = write_unit(lake, cal, u, quotes("SPXW", D, D, strikes=(5100.0,), bid=2.0))
    assert count(lake, "option_quote", u.id) == n == 2 * 406
    assert count(lake, "option_quote", other.id) == 4 * 406  # untouched
    with lake.cursor() as cur:
        assert cur.execute(f"SELECT DISTINCT bid FROM {LAKE}.option_quote WHERE unit_id = ?", [u.id]).fetchall() \
            == [(2.0,)]
        assert cur.execute(f"SELECT count(*), max(n_rows) FROM {LAKE}.coverage WHERE unit_id = ?",
                           [u.id]).fetchone() == (1, n)


def test_failed_insert_stores_nothing(lake, cal):
    u = Unit("option_ohlc", "SPXW", D, expiration=D, interval_ms=900_000)
    start = datetime.combine(D, time(9, 30), ET).astimezone(timezone.utc)
    bars = pl.DataFrame({
        "bar_start": [start, start + timedelta(minutes=15)], "bar_end": [start + timedelta(minutes=15),
                                                                        start + timedelta(minutes=30)],
        "root": "SPXW", "expiration": D, "strike": 5100.0, "right": "C", "interval_ms": 900_000,
        "open": [1.0, None], "high": 1.2, "low": 0.9, "close": 1.1, "volume": 10, "count": 3, "vwap": 1.05,
    }).with_columns(pl.col("bar_start", "bar_end").dt.replace_time_zone("UTC"))
    with pytest.raises(Exception):
        write_unit(lake, cal, u, bars)  # open is NOT NULL
    assert count(lake, "option_ohlc") == 0
    assert split(lake, [u]) == ([], [u])


def test_bars_labelled_by_bar_start(cal):
    u = Unit("option_ohlc", "SPXW", D, expiration=D, interval_ms=900_000)
    starts = [datetime.combine(D, t, ET) for t in (time(9, 15), time(9, 30), time(15, 45), time(16, 0))]
    bars = pl.DataFrame({
        "bar_start": starts, "bar_end": [s + timedelta(minutes=15) for s in starts], "root": "SPXW",
        "expiration": D, "strike": 5100.0, "right": "C", "interval_ms": 900_000, "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 1.0, "volume": 1, "count": 1, "vwap": 1.0})
    got = prepare(cal, u, bars.with_columns(pl.col("bar_start", "bar_end").dt.convert_time_zone("UTC")))
    assert got["session"].to_list() == ["closed", "rth", "rth", "closed"]


@pytest.mark.parametrize("change, message", [
    (lambda df: df.with_columns(root=pl.lit("SPX")), "root other than"),
    (lambda df: df.with_columns(expiration=pl.lit(date(2024, 3, 15))), "expiration other than"),
    (lambda df: df.with_columns(interval_ms=pl.lit(1000, pl.Int32)), "interval_ms other than"),
    (lambda df: df.with_columns(right=pl.lit("call")), "right not C or P"),
    (lambda df: df.with_columns(pl.col("ts") + timedelta(days=1)), "on a date other than"),
    (lambda df: df.drop("bid"), "missing columns ['bid']"),
    (lambda df: df.with_columns(session=pl.lit("rth")), "unexpected ['session']"),
    (lambda df: df.with_columns(pl.col("ts").dt.replace_time_zone(None)), "time-zone aware"),
])
def test_rejects_rows_outside_the_unit(cal, change, message):
    with pytest.raises(WriteError, match=message.replace("[", r"\[").replace("]", r"\]")):
        prepare(cal, unit(), change(quotes("SPXW", D, D)))


def test_evening_rows_roll_to_the_next_trade_date(cal):
    """A unit on ThetaData's date keeps its 20:15+ rows; they're labelled with the next trading day. (Planning
    such units waits for the GTH window to be settled, but the write path already handles them.)"""
    u = Unit("option_quote_tick", "SPXW", D, expiration=date(2024, 3, 15))
    ts = [datetime.combine(D, time(20, 14, 59), ET), datetime.combine(D, time(20, 15), ET)]
    df = pl.DataFrame({"ts": ts, "root": "SPXW", "expiration": date(2024, 3, 15), "strike": 5100.0, "right": "C",
                       "seq": [0, 0], "bid_size": 1, "bid_exchange": 5, "bid": 1.0, "bid_condition": 50,
                       "ask_size": 1, "ask_exchange": 5, "ask": 1.1, "ask_condition": 50})
    got = prepare(cal, u, df)
    assert got["trade_date"].to_list() == [D, date(2024, 3, 15)]
    assert got["session"].to_list() == ["closed", "gth"]


# ---------------------------------------------------------------- label check

def test_label_check_finds_a_wrong_label(lake, cal):
    write_unit(lake, cal, unit(), quotes("SPXW", D, D))
    ok = check_labels(lake, cal, "option_quote", D, D)
    assert ok["rows"] == 4 * 406 and ok["mismatched"] == 0
    with lake.cursor() as cur:
        cur.execute(f"UPDATE {LAKE}.option_quote SET session = 'gth' WHERE ts = ?",
                    [datetime.combine(D, time(12, 0), ET)])
        cur.execute(f"UPDATE {LAKE}.option_quote SET trade_date = DATE '2024-03-13' WHERE ts = ?",
                    [datetime.combine(D, time(13, 0), ET)])
    bad = check_labels(lake, cal, "option_quote", D, D)
    assert bad["mismatched"] == 8
    assert {e["want_session"] for e in bad["examples"]} == {"rth"}
