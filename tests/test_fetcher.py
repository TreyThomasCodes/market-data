"""Phase 3 without a login: ThetaDataDx-shaped answers from a fake client, converted, stored once, shared between
requests. What only production can confirm is listed in the README (Phase 3)."""
import threading
import time as clock
from datetime import date, datetime, time, timedelta, timezone

import polars as pl
import pytest
import thetadatadx as tdx

from market_data.calendar import ET
from market_data.fetcher import Fetcher, NotStorable, interval_str, last_complete, request_for
from market_data.ingest import IngestError, convert
from market_data.store import LAKE, Store
from market_data.tables import ensure_tables
from market_data.theta_client import ThetaClient
from market_data.units import Unit, plan, split
from market_data.writer import check_labels

D = date(2024, 3, 14)
NOW = datetime(2026, 11, 24, 15, tzinfo=timezone.utc)


def ms(t: time) -> int:
    return ((t.hour * 60 + t.minute) * 60 + t.second) * 1000


def raw_quotes(d=D, exp=D, strikes=(5100.0, 5150.0), start=time(9, 30), end=time(16, 0), right=("C", "P")):
    """What ThetaDataDx's QuoteTickList.to_polars() gives, per its stubs: integer dates, ms_of_day, no root."""
    t0, n = ms(start), (ms(end) - ms(start)) // 60_000 + 1
    rows = [(t0 + i * 60_000, int(d.strftime("%Y%m%d")), int(exp.strftime("%Y%m%d")), k, r)
            for k in strikes for r in right for i in range(n)]
    df = pl.DataFrame(rows, orient="row", schema={"ms_of_day": pl.Int32, "date": pl.Int32, "expiration": pl.Int32,
                                                  "strike": pl.Float64, "right": pl.String})
    return df.with_columns(bid_size=pl.lit(10, pl.Int32), bid_exchange=pl.lit(5), bid=pl.lit(1.0),
                           bid_condition=pl.lit(50), ask_size=pl.lit(12, pl.Int32), ask_exchange=pl.lit(5),
                           ask=pl.lit(1.1), ask_condition=pl.lit(50))


class Result:
    def __init__(self, df):
        self.df = df

    def to_polars(self):
        return self.df


class FakeMD:
    def __init__(self, answer=None, delay=0.0):
        self.calls = []
        self.answer = answer
        self.delay = delay
        self.lock = threading.Lock()

    def option_history_quote(self, symbol, expiration, **kw):
        with self.lock:
            self.calls.append((symbol, expiration, kw))
        clock.sleep(self.delay)
        if self.answer is None:
            raise tdx.NotFoundError("nothing")
        return Result(self.answer(kw))


class FakeClient:
    def __init__(self, md):
        self.market_data = md

    def close(self):
        pass


@pytest.fixture
def lake(settings):
    s = Store(settings)
    s.attach()
    ensure_tables(s)
    yield s
    s.close()


def fetcher(lake, cal, md):
    theta = ThetaClient(lambda: FakeClient(md), errors=tdx, sleep=lambda s: None)
    return Fetcher(lake, lambda: cal, theta, now=lambda: NOW)


def unit(**kw):
    return Unit("option_quote", "SPXW", kw.pop("d", D), expiration=kw.pop("exp", D), interval_ms=60_000, **kw)


# ---------------------------------------------------------------- requests

def test_request_covers_regular_hours_from_the_calendar(cal):
    m, args, kw = request_for(cal, unit())
    assert (m, args) == ("option_history_quote", ("SPXW", D))
    assert kw == {"date": D, "start_time": "09:30:00", "end_time": "16:00:00", "interval": "1m"}  # expiring: 16:00
    _, _, kw = request_for(cal, unit(exp=date(2024, 3, 15)))
    assert kw["end_time"] == "16:15:00"                                          # not expiring: 16:15
    _, _, kw = request_for(cal, unit(d=date(2024, 11, 29), exp=date(2024, 12, 6)))
    assert kw["end_time"] == "13:15:00"                                          # half day
    _, _, kw = request_for(cal, Unit("option_quote_tick", "SPXW", D, expiration=D, strike=5145.0, right="C"))
    assert kw["strike"] == "5145" and kw["right"] == "C" and kw["interval"] == "tick"
    m, args, kw = request_for(cal, Unit("option_contracts", "SPXW", D, list_type="trade"))
    assert (m, args, kw) == ("option_list_contracts", ("trade", D), {"symbol": "SPXW"})
    assert [interval_str(x) for x in (1000, 60_000, 900_000, 250)] == ["1s", "1m", "15m", "250ms"]


def test_last_complete_date_is_conservative():
    at = lambda h, m: datetime.combine(date(2026, 11, 24), time(h, m), ET)
    assert last_complete(at(1, 59)) == date(2026, 11, 22)
    assert last_complete(at(2, 0)) == date(2026, 11, 23)


# ---------------------------------------------------------------- conversions

def test_quotes_convert_to_stored_columns():
    df = convert(unit(), raw_quotes(right=("call", "put")))
    assert df["root"].unique().to_list() == ["SPXW"] and set(df["right"]) == {"C", "P"}
    assert df["expiration"].dtype == pl.Date and df["interval_ms"].unique().to_list() == [60_000]
    assert df["ts"].dtype == pl.Datetime("us", "UTC")
    first = df["ts"].min().astimezone(ET)
    assert (first.date(), first.time()) == (D, time(9, 30))


def test_tick_seq_numbers_each_contract_in_returned_order():
    raw = pl.concat([raw_quotes(start=time(10, 0), end=time(10, 0))] * 3)  # same ms three times per contract
    u = Unit("option_quote_tick", "SPXW", D, expiration=D)
    df = convert(u, raw)
    assert df.filter((pl.col("strike") == 5100) & (pl.col("right") == "C"))["seq"].to_list() == [0, 1, 2]


def test_empty_trade_bars_are_dropped_and_bars_get_their_end():
    raw = pl.DataFrame({"ms_of_day": [ms(time(9, 30)), ms(time(9, 45))], "date": [20240314] * 2,
                        "expiration": [20240314] * 2, "strike": [5100.0] * 2, "right": ["C"] * 2,
                        "open": [1.0, 0.0], "high": [1.2, 0.0], "low": [0.9, 0.0], "close": [1.1, 0.0],
                        "volume": [10, 0], "count": [3, 0], "vwap": [1.05, 0.0]})
    df = convert(Unit("option_ohlc", "SPXW", D, expiration=D, interval_ms=900_000), raw)
    assert df.height == 1
    assert df["bar_end"][0] - df["bar_start"][0] == timedelta(minutes=15)


def test_not_found_converts_to_an_empty_frame():
    df = convert(unit(), None)
    assert df.height == 0 and "ts" in df.columns


@pytest.mark.parametrize("change, message", [
    (lambda df: df.drop("bid"), "lacks"),
    (lambda df: df.with_columns(right=pl.lit("X")), "unknown right"),
    (lambda df: df.with_columns(strike=pl.col("strike") * 1000), "1/1000ths"),
])
def test_unexpected_answers_raise(change, message):
    with pytest.raises(IngestError, match=message):
        convert(unit(), change(raw_quotes()))


# ---------------------------------------------------------------- fetching

def test_fetched_once_then_answered_from_the_store(lake, cal):
    md = FakeMD(lambda kw: raw_quotes())
    f = fetcher(lake, cal, md)
    units = plan(cal, "option_quote", "SPXW", D, D, expiration=D, interval_ms=60_000)
    assert f.ensure(units) == {"units": 1, "covered": 0, "fetched": 1}
    assert f.ensure(units) == {"units": 1, "covered": 1, "fetched": 0}
    assert len(md.calls) == 1
    assert check_labels(lake, cal, "option_quote", D, D) == {"rows": 4 * 391, "mismatched": 0, "examples": []}
    with lake.cursor() as cur:
        assert cur.execute(f"SELECT n_rows FROM {LAKE}.coverage").fetchall() == [(4 * 391,)]


def test_duplicate_requests_share_one_fetch(lake, cal):
    md = FakeMD(lambda kw: raw_quotes(), delay=0.5)
    f = fetcher(lake, cal, md)
    u = unit()
    out = []
    threads = [threading.Thread(target=lambda: out.append(f.fetch_one(u))) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert len(md.calls) == 1 and out.count(4 * 391) == 5 and f.shared == 4


def test_not_found_is_stored_as_empty_and_not_asked_again(lake, cal):
    md = FakeMD(None)
    f = fetcher(lake, cal, md)
    u = unit()
    f.ensure([u])
    f.ensure([u])
    assert len(md.calls) == 1
    assert split(lake, [u]) == ([u], [])


def test_recent_dates_are_not_stored(lake, cal):
    md = FakeMD(lambda kw: raw_quotes())
    f = fetcher(lake, cal, md)
    with pytest.raises(NotStorable):
        f.ensure([unit(d=date(2026, 11, 24), exp=date(2026, 11, 24))])
    assert md.calls == []


def test_a_bad_answer_stores_nothing(lake, cal):
    md = FakeMD(lambda kw: raw_quotes().drop("ask"))
    f = fetcher(lake, cal, md)
    with pytest.raises(IngestError):
        f.ensure([unit()])
    assert split(lake, [unit()]) == ([], [unit()])
