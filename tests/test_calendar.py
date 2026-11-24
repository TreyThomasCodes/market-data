"""The calendar against what the probes measured (spikes/session_*.py, holiday_evenings.py, calendar_probe.py)."""
from datetime import date, datetime, time, timedelta, timezone

import polars as pl
import pytest

from market_data.calendar import (ET, CalendarError, exchange_holidays, holiday_kind)

from conftest import calendar_days

D = date.fromisoformat


def et(s: str) -> datetime:
    """'2024-03-14 09:15:00.764' in ET."""
    return datetime.fromisoformat(s).replace(tzinfo=ET)


def spans(cal, root, td, exp=None, asset="option"):
    return [(s.name, s.start.strftime("%m-%d %H:%M"), s.end.strftime("%m-%d %H:%M"))
            for s in cal.sessions(root, D(td), D(exp) if exp else None, asset)]


def lab(cal, ts, root="SPXW", kind="event", exp=None, asset="option"):
    td, s = cal.label(et(ts), root, kind, D(exp) if exp else None, asset)
    return td.isoformat(), s


# ---------------------------------------------------------------- trading days

def test_every_full_close_is_a_known_holiday():
    special = {D("2012-10-29"), D("2012-10-30"), D("2018-12-05"), D("2025-01-09")}
    for y, rows in calendar_days().items():
        closes = {r.date for r in rows if r.status == "full_close"}
        named = set(exchange_holidays(y))
        assert closes - named == {d for d in special if d.year == y}, y
        assert named <= closes, (y, sorted(named - closes))


def test_early_closes_are_1300(cal):
    early = [r for rows in calendar_days().values() for r in rows if r.status == "early_close"]
    assert early and all((r.open, r.close) == (time(9, 30), time(13)) for r in early)


def test_holiday_kinds():
    assert holiday_kind(D("2022-12-26")) == "christmas"      # Sunday Christmas, observed Monday
    assert holiday_kind(D("2023-01-02")) == "new_year"
    assert holiday_kind(D("2027-12-24")) == "christmas"      # Saturday Christmas, observed Friday
    assert holiday_kind(D("2026-07-03")) == "july4"
    assert holiday_kind(D("2024-03-29")) == "good_friday"
    assert holiday_kind(D("2025-01-09")) == "special"
    assert D("2021-12-31") not in exchange_holidays(2021)    # Saturday New Year isn't observed


def test_trading_days(cal):
    assert not cal.is_trading(D("2024-11-28")) and cal.is_trading(D("2024-11-29"))
    assert cal.next_trading(D("2024-11-27")) == D("2024-11-29")
    assert cal.next_trading(D("2025-01-17")) == D("2025-01-21")
    assert cal.prev_trading(D("2025-01-10")) == D("2025-01-08")
    assert cal.hours(D("2025-07-03")) == (time(9, 30), time(13))
    with pytest.raises(CalendarError):
        cal.is_trading(D("2028-01-03"))
    with pytest.raises(CalendarError):
        cal.trade_date(et("2027-12-31 21:00"), "event")


# ---------------------------------------------------------------- SPXW sessions (the Phase 1 done list)

def test_2024_03_14_gth_ends_0915(cal):
    assert spans(cal, "SPXW", "2024-03-14") == [
        ("gth", "03-13 20:15", "03-14 09:15"), ("rth", "03-14 09:30", "03-14 16:15"), ("curb", "03-14 16:15", "03-14 17:00")]
    assert spans(cal, "SPXW", "2024-03-14", exp="2024-03-14") == [
        ("gth", "03-13 20:15", "03-14 09:15"), ("rth", "03-14 09:30", "03-14 16:00")]
    assert lab(cal, "2024-03-14 09:14:59.999") == ("2024-03-14", "gth")
    assert lab(cal, "2024-03-14 09:15:00.764") == ("2024-03-14", "closed")  # the closing tick after the bell
    assert lab(cal, "2024-03-14 21:00") == ("2024-03-15", "gth")            # filed by ThetaData under 03-14


def test_gth_end_moves_to_0925_from_2024_08_26(cal):
    assert spans(cal, "SPXW", "2024-08-23")[0] == ("gth", "08-22 20:15", "08-23 09:15")
    assert spans(cal, "SPXW", "2024-08-26")[0] == ("gth", "08-25 20:15", "08-26 09:25")
    assert lab(cal, "2024-08-26 09:20") == ("2024-08-26", "gth")
    assert lab(cal, "2024-08-23 09:20") == ("2024-08-23", "closed")


def test_thanksgiving_2024(cal):
    assert spans(cal, "SPXW", "2024-11-29") == [
        ("gth", "11-27 20:15", "11-28 11:30"), ("gth", "11-28 20:15", "11-29 09:25"), ("rth", "11-29 09:30", "11-29 13:15")]
    assert spans(cal, "SPXW", "2024-11-29", exp="2024-11-29")[-1] == ("rth", "11-29 09:30", "11-29 13:00")
    assert lab(cal, "2024-11-27 22:00") == ("2024-11-29", "gth")
    assert lab(cal, "2024-11-28 11:29:59") == ("2024-11-29", "gth")
    assert lab(cal, "2024-11-28 12:00") == ("2024-11-29", "closed")
    assert lab(cal, "2024-11-29 13:14") == ("2024-11-29", "rth")
    assert lab(cal, "2024-11-29 16:10") == ("2024-11-29", "closed")         # half day: no curb
    assert cal.evening(D("2024-11-29")) is None                              # Friday


def test_sunday_before_mlk_2025(cal):
    assert spans(cal, "SPXW", "2025-01-21")[:2] == [("gth", "01-19 20:15", "01-20 11:30"), ("gth", "01-20 20:15", "01-21 09:25")]
    assert lab(cal, "2025-01-19 21:00") == ("2025-01-21", "gth")
    assert lab(cal, "2025-01-20 10:00") == ("2025-01-21", "gth")
    assert cal.evening(D("2025-01-17")) is None


def test_half_day_with_evening_2025_07_03(cal):
    assert spans(cal, "SPXW", "2025-07-03") == [("gth", "07-02 20:15", "07-03 09:25"), ("rth", "07-03 09:30", "07-03 13:15")]
    assert spans(cal, "SPXW", "2025-07-07")[:2] == [("gth", "07-03 20:15", "07-04 11:30"), ("gth", "07-06 20:15", "07-07 09:25")]
    assert lab(cal, "2025-07-03 21:00") == ("2025-07-07", "gth")


def test_0930_snapshot_is_outside_rth(cal):
    assert lab(cal, "2024-03-14 09:30:00", kind="snapshot") == ("2024-03-14", "closed")
    assert lab(cal, "2024-03-14 09:30:01", kind="snapshot") == ("2024-03-14", "rth")
    assert lab(cal, "2024-03-14 09:30:00", kind="event") == ("2024-03-14", "rth")
    assert lab(cal, "2026-11-19 09:25:00", kind="snapshot") == ("2026-11-19", "gth")  # last GTH state


def test_close_snapshots_are_inside(cal):
    assert lab(cal, "2024-03-14 16:15:00", kind="snapshot") == ("2024-03-14", "rth")
    assert lab(cal, "2024-03-14 16:15:00", kind="event") == ("2024-03-14", "curb")
    assert lab(cal, "2024-03-14 16:00:00", kind="snapshot", exp="2024-03-14") == ("2024-03-14", "rth")
    assert lab(cal, "2024-03-14 16:00:00.001", exp="2024-03-14") == ("2024-03-14", "closed")
    assert lab(cal, "2024-03-14 17:00:00", kind="snapshot") == ("2024-03-14", "curb")


def test_roll_at_2015(cal):
    assert lab(cal, "2024-03-14 20:15:00", kind="snapshot") == ("2024-03-14", "closed")
    assert lab(cal, "2024-03-14 20:15:00", kind="event") == ("2024-03-15", "gth")
    assert lab(cal, "2024-03-14 20:15:00.001", kind="snapshot") == ("2024-03-15", "gth")
    assert lab(cal, "2024-03-15 21:00") == ("2024-03-18", "closed")         # Friday evening: nothing
    assert lab(cal, "2024-03-16 12:00") == ("2024-03-18", "closed")         # Saturday
    assert lab(cal, "2024-03-17 20:15") == ("2024-03-18", "gth")            # Sunday evening


# ---------------------------------------------------------------- holidays (spikes/holiday_evenings.py)

@pytest.mark.parametrize("eve, expected", [
    # no evening session before Christmas, New Year, Good Friday
    ("2024-12-24", None), ("2024-12-31", None), ("2022-12-25", None), ("2023-01-01", None), ("2023-12-31", None),
    ("2024-03-28", None), ("2025-04-17", None), ("2022-04-14", None),
    # an evening session on the holiday itself
    ("2024-12-25", ("12-25 20:15", "12-26 09:25")), ("2025-01-01", ("01-01 20:15", "01-02 09:25")),
    ("2022-12-26", ("12-26 20:15", "12-27 09:15")), ("2024-03-31", ("03-31 20:15", "04-01 09:15")),
    # the evening before runs to 11:30 on the holiday
    ("2024-11-27", ("11-27 20:15", "11-28 11:30")), ("2025-01-19", ("01-19 20:15", "01-20 11:30")),
    ("2025-02-16", ("02-16 20:15", "02-17 11:30")), ("2024-05-26", ("05-26 20:15", "05-27 11:30")),
    ("2024-06-18", ("06-18 20:15", "06-19 11:30")), ("2024-07-03", ("07-03 20:15", "07-04 11:30")),
    ("2024-09-01", ("09-01 20:15", "09-02 11:30")), ("2022-06-19", ("06-19 20:15", "06-20 11:30")),
    # the 2025-01-09 closure: the overnight session ends as usual
    ("2025-01-08", ("01-08 20:15", "01-09 09:25")), ("2025-01-09", ("01-09 20:15", "01-10 09:25")),
])
def test_holiday_evenings(cal, eve, expected):
    ev = cal.evening(D(eve))
    assert (ev and (ev[0].strftime("%m-%d %H:%M"), ev[1].strftime("%m-%d %H:%M"))) == expected


def test_closure_2025_01_09_trade_date(cal):
    assert lab(cal, "2025-01-09 09:00") == ("2025-01-10", "gth")
    assert lab(cal, "2025-01-09 12:00") == ("2025-01-10", "closed")


# ---------------------------------------------------------------- other products

def test_spy(cal):
    assert spans(cal, "SPY", "2024-03-15", exp="2024-03-15") == [("rth", "03-15 09:30", "03-15 16:15")]
    assert spans(cal, "SPY", "2026-10-16") == [("rth", "10-16 09:30", "10-16 16:15")]
    assert spans(cal, "SPY", "2026-10-19") == [("pre", "10-19 07:30", "10-19 09:25"), ("rth", "10-19 09:30", "10-19 16:15")]
    assert spans(cal, "SPY", "2024-11-29") == [("rth", "11-29 09:30", "11-29 13:15")]
    assert lab(cal, "2024-03-15 16:15:00.000", root="SPY") == ("2024-03-15", "closed")


def test_stock_options_and_unknown_roots(cal):
    assert spans(cal, "AAPL", "2024-03-14") == [("rth", "03-14 09:30", "03-14 16:00")]
    assert spans(cal, "AAPL", "2026-11-19")[0] == ("pre", "11-19 07:30", "11-19 09:25")
    assert spans(cal, "ZZZZ", "2026-11-19") == [("rth", "11-19 09:30", "11-19 16:00")]
    assert spans(cal, "NDXP", "2024-03-14") == [("rth", "03-14 09:30", "03-14 16:15")]
    assert spans(cal, "NDXP", "2024-03-14", exp="2024-03-14") == [("rth", "03-14 09:30", "03-14 16:00")]


def test_stocks(cal):
    assert spans(cal, "AAPL", "2024-03-14", asset="stock") == [
        ("pre", "03-14 04:00", "03-14 09:30"), ("rth", "03-14 09:30", "03-14 16:00"), ("post", "03-14 16:00", "03-14 20:00")]
    assert spans(cal, "AAPL", "2024-11-29", asset="stock")[1] == ("rth", "11-29 09:30", "11-29 13:00")
    assert lab(cal, "2024-03-14 19:59", root="AAPL", asset="stock") == ("2024-03-14", "post")


def test_dst_is_et_wall_clock(cal):
    # 2024-03-10 is the spring-forward Sunday: the evening session still opens 20:15 ET (00:15 UTC, not 01:15)
    s = cal.sessions("SPXW", D("2024-03-11"))[0]
    assert s.start.astimezone(ET).time() == time(20, 15) and s.start.utcoffset() == timedelta(hours=-4)


def test_naive_timestamps_are_refused(cal):
    with pytest.raises(CalendarError):
        cal.label(datetime(2024, 3, 14, 10), "SPXW", "event")


# ---------------------------------------------------------------- vectorized labels match the scalar ones

def _grid(cal, start, end, roots):
    """Every 7 minutes, plus 1 ms either side of and exactly at every session boundary."""
    # stepped in UTC: an ET wall-clock step would make times that don't exist (02:00 on spring-forward day)
    a = datetime.combine(D(start), time(), ET).astimezone(timezone.utc)
    b = datetime.combine(D(end), time(), ET).astimezone(timezone.utc)
    ts = []
    t = a
    while t < b:
        ts.append(t)
        t += timedelta(minutes=7)
    for root in roots:
        for td in cal.trading_days(D(start) + timedelta(3), D(end) - timedelta(1)):
            for exp in (None, td):
                for s in cal.sessions(root, td, exp):
                    for edge in (s.start, s.end):
                        ts += [edge - timedelta(milliseconds=1), edge, edge + timedelta(milliseconds=1)]
    return sorted({t.astimezone(timezone.utc) for t in ts})


@pytest.mark.parametrize("start, end", [("2024-11-22", "2024-12-03"), ("2025-01-05", "2025-01-23"),
                                        ("2025-06-29", "2025-07-09"), ("2024-08-20", "2024-08-28"),
                                        ("2026-10-14", "2026-10-21"), ("2024-03-06", "2024-03-13")])
@pytest.mark.parametrize("kind", ["event", "snapshot"])
def test_label_frame_matches_label(cal, start, end, kind):
    roots = ["SPXW", "SPY", "AAPL", "NDXP", "ZZZZ"]
    ts = _grid(cal, start, end, roots)
    exps = [None, D(end) - timedelta(2), D(start) + timedelta(5)]
    rows = [(t, r, e) for t in ts for r in roots for e in exps]
    df = pl.DataFrame({"ts": [r[0] for r in rows], "root": [r[1] for r in rows], "expiration": [r[2] for r in rows]},
                      schema={"ts": pl.Datetime("ms", "UTC"), "root": pl.String, "expiration": pl.Date})
    df = df.with_columns(pl.col("ts").dt.convert_time_zone(ET.key)).sample(fraction=1.0, shuffle=True, seed=1)  # order must be kept, whatever it is
    out = cal.label_frame(df, kind)
    assert out.select("ts", "root", "expiration").equals(df.select("ts", "root", "expiration"))
    want = [cal.label(t, r, kind, e) for t, r, e in df.iter_rows()]
    got = list(zip(out["trade_date"], out["session"]))
    bad = [(row, g, w) for row, g, w in zip(df.iter_rows(), got, want) if g != w]
    assert not bad, bad[:5]
    assert set(out["session"]) >= {"rth", "closed"}


def test_label_frame_stocks_and_fixed_root(cal):
    df = pl.DataFrame({"ts": [et("2024-03-14 05:00"), et("2024-03-14 12:00"), et("2024-03-14 17:00")]})
    out = cal.label_frame(df, "event", "stock", root="AAPL")
    assert out["session"].to_list() == ["pre", "rth", "post"]


def test_label_frame_refuses_naive_ts(cal):
    with pytest.raises(CalendarError):
        cal.label_frame(pl.DataFrame({"ts": [datetime(2024, 3, 14, 10)], "root": ["SPXW"]}), "event")


@pytest.mark.parametrize("root, before, after, names", [
    ("VIX", "2022-03-14", "2022-03-15", ["gth", "rth"]),         # GTH from 2022-03-15
    ("SPXW", "2022-04-22", "2022-04-25", ["gth", "rth", "curb"]),  # curb from 2022-04-25 (SPX, VIX too)
    ("XSP", "2022-12-09", "2022-12-12", ["gth", "rth"]),
    ("XSP", "2023-01-10", "2023-01-11", ["gth", "rth", "curb"]),
    ("RUTW", "2026-02-10", "2026-02-11", ["gth", "rth", "curb"]),
])
def test_session_start_dates(cal, root, before, after, names):
    """First trade dates with real GTH / curb quotes (spikes/calendar_probe.py)."""
    assert [s.name for s in cal.sessions(root, D(after))] == names
    assert len(cal.sessions(root, D(before))) < len(names)
    assert [s.name for s in cal.sessions("NDXP", D("2026-11-19"))] == ["rth"]


def test_roots_are_case_insensitive(cal):
    assert cal.label(et("2024-03-14 16:30"), "spxw", "event") == (D("2024-03-14"), "curb")
    df = pl.DataFrame({"ts": [et("2024-03-14 03:00"), et("2024-03-14 16:30")]})
    assert cal.label_frame(df, "event", root="spxw")["session"].to_list() == ["gth", "curb"]


def test_label_frame_refuses_null_ts(cal):
    df = pl.DataFrame({"ts": [et("2024-03-14 10:00"), None], "root": ["SPXW", "SPXW"]},
                      schema={"ts": pl.Datetime("us", ET.key), "root": pl.String})
    with pytest.raises(CalendarError, match="null"):
        cal.label_frame(df, "event")
