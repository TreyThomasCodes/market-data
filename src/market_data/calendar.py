"""The trading calendar: trading days, sessions per product, trade dates and session labels.

Everything here follows the README (Time semantics; Open questions: session semantics) and the probes in `spikes/`.
Trading days and early closes come from ThetaData's `calendar_year` (stored, see `calendar_store`); the session rules
below are kept by hand, each with the date it takes effect.

Labels, for a timestamp `ts` (time-zone aware) of a row for a product:

- `trade_date`: the exchange's trade date. It rolls when the evening session opens (20:15 ET): from then on, and on
  any day that isn't a trading day, a row belongs to the next trading day. Curb rows keep the regular session's date.
  This doesn't depend on the product, so a stock row at 19:00 keeps its calendar date.
- `session`: `gth`, `pre`, `rth`, `curb` for options; `pre`, `rth`, `post` for stocks; `closed` for a time in no
  session (between sessions, or after a session's last instant, such as the 09:30:00 snapshot or a closing quote a
  few milliseconds after the bell).

Events (ticks, trades) and bars (by `bar_start`) belong to the session whose [start, end) holds them. Snapshots
(interval quotes, Greeks) belong to the session whose (start, end] holds them: a snapshot is the state as of `ts`, so
the 09:30:00 row is pre-open state and the 16:15:00 row is the close. The same rule decides the trade date at the
20:15 roll: an event at 20:15:00 is in the evening session, a snapshot at 20:15:00 is the state before it.
"""
from __future__ import annotations

import bisect
import calendar as _cal
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

import polars as pl

ET = ZoneInfo("America/New_York")
ET_NAME = "America/New_York"
FIRST_YEAR = 2012  # ThetaData's calendar_year starts here (probed 2026-11-22: 2008-2011 NotFound)

EVENING_OPEN = time(20, 15)          # Cboe GTH opens; the trade date rolls here
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)          # options on stocks and expiring PM-settled index options
CURB_END = time(17, 0)
OPTION_PRE = (time(7, 30), time(9, 25))   # equity-option pre-market (Cboe), from 2026-10-19
STOCK_PRE_START = time(4, 0)         # Alpaca SIP minute bars run 04:00-20:00 (probed 2026-11-21)
STOCK_POST_END = time(20, 0)
GTH_END_BEFORE = time(9, 15)
GTH_END = time(9, 25)
GTH_END_CHANGE = date(2024, 8, 26)   # ThetaData docs: effective trade date 2024-08-26 (Sunday 08-25 evening on)

SESSIONS = ("gth", "pre", "rth", "curb", "post", "closed")


class Kind(str, Enum):
    EVENT = "event"        # ticks, trades, and bars by bar_start: [start, end)
    SNAPSHOT = "snapshot"  # interval quotes and Greeks: (start, end]


class Asset(str, Enum):
    OPTION = "option"
    STOCK = "stock"


class CalendarError(Exception):
    pass


# ---------------------------------------------------------------- trading days

@dataclass(frozen=True)
class Day:
    """A day ThetaData's calendar lists: a full close, or an early close with its hours."""
    date: date
    status: str               # "full_close" | "early_close"
    open: time | None = None
    close: time | None = None

    def __post_init__(self):
        if self.status not in ("full_close", "early_close"):
            raise CalendarError(f"{self.date}: unknown calendar status {self.status!r}")
        if self.status == "early_close" and (self.open is None or self.close is None):
            raise CalendarError(f"{self.date}: early close without hours")


def _nth_weekday(y: int, month: int, weekday: int, n: int) -> date:
    first = date(y, month, 1)
    return first + timedelta((weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(y: int, month: int, weekday: int) -> date:
    last = date(y, month, _cal.monthrange(y, month)[1])
    return last - timedelta((last.weekday() - weekday) % 7)


def _easter(y: int) -> date:
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(y, month, day + 1)


def _observed(d: date) -> date:
    return d - timedelta(1) if d.weekday() == 5 else d + timedelta(1) if d.weekday() == 6 else d


def exchange_holidays(y: int) -> dict[date, str]:
    """The regular US exchange holidays of a year, by kind, on the day observed. Used to tell which holiday a full
    close is, since `calendar_year` gives no names; closures not here (2012-10-29/30, 2018-12-05, 2025-01-09) are
    `special`."""
    h = {
        _nth_weekday(y, 1, 0, 3): "mlk",
        _nth_weekday(y, 2, 0, 3): "presidents",
        _easter(y) - timedelta(2): "good_friday",
        _last_weekday(y, 5, 0): "memorial",
        _observed(date(y, 7, 4)): "july4",
        _nth_weekday(y, 9, 0, 1): "labor",
        _nth_weekday(y, 11, 3, 4): "thanksgiving",
        _observed(date(y, 12, 25)): "christmas",
    }
    ny = date(y, 1, 1)
    if ny.weekday() != 5:  # a Saturday New Year's Day isn't observed on the Friday before
        h[_observed(ny)] = "new_year"
    if y >= 2022:
        h[_observed(date(y, 6, 19))] = "juneteenth"
    return h


def holiday_kind(d: date) -> str:
    return exchange_holidays(d.year).get(d, "special")


# How the evening GTH session before a full close ends (spikes/holiday_evenings.py, probed 2026-11-22, every holiday
# 2022-05 to 2026-09). None: there's no evening session before it. A time: the session runs on to that time on the
# holiday. NORMAL: it ends at the usual GTH end. An evening session opens at 20:15 on the holiday itself as on any
# other weekday.
NORMAL = "normal"
EVENING_BEFORE: dict[str, time | str | None] = {
    "new_year": None,
    "christmas": None,
    "good_friday": None,
    "mlk": time(11, 30),
    "presidents": time(11, 30),
    "memorial": time(11, 30),
    "juneteenth": time(11, 30),
    "july4": time(11, 30),
    "labor": time(11, 30),
    "thanksgiving": time(11, 30),
    "special": NORMAL,  # 2025-01-09: overnight session ended at 09:25 as usual
}


# ---------------------------------------------------------------- product rules

@dataclass(frozen=True)
class OptionRules:
    """Sessions of one option root from a given trade date on.

    rth_extra / rth_extra_expiring: minutes the regular session runs past the day's regular close (16:00, or 13:00 on
    a half day) for a non-expiring and an expiring contract. 15 means it ends 16:15 (13:15 on a half day).
    """
    rth_extra: int = 0
    rth_extra_expiring: int = 0
    gth: bool = False
    curb: bool = False
    pre: bool = False


def _rules(*changes: tuple[date, OptionRules]) -> list[tuple[date, OptionRules]]:
    return sorted(changes, key=lambda c: c[0])


_START = date(FIRST_YEAR, 1, 1)
_PRE_START = date(2026, 10, 19)  # first date with real SPY pre-market quotes (spikes/session_questions.py)
_INDEX = OptionRules(rth_extra=15, rth_extra_expiring=0)
_INDEX_GTH = replace(_INDEX, gth=True)
_INDEX_PM = replace(_INDEX, gth=True, curb=True)
_ETF_1615 = OptionRules(rth_extra=15, rth_extra_expiring=15)

# Option roots whose sessions differ from a stock option's (09:30-16:00, nothing else). Measured on 2024-03-14 and
# 2026-11-19 unless noted (spikes/session_questions.py). GTH and curb start on the first trade date ThetaData has
# real quotes in them (spikes/calendar_probe.py, 2026-11-22: bisection over 2022-01-18 to 2026-11-19); the exchange
# may have started them a little earlier, with no data to label. SPX/SPXW GTH is present where ThetaData's GTH data
# starts (2022-01), so it's listed from the calendar's start. Not final for ETFs: when each ETF joined the 16:15
# group, and whether expiring ETF options trade after 16:00, are open (README: Open questions).
OPTION_ROOTS: dict[str, list[tuple[date, OptionRules]]] = {
    "SPXW": _rules((_START, _INDEX_GTH), (date(2022, 4, 25), _INDEX_PM)),
    "SPX": _rules((_START, _INDEX_GTH), (date(2022, 4, 25), _INDEX_PM)),
    "VIX": _rules((_START, _INDEX), (date(2022, 3, 15), _INDEX_GTH), (date(2022, 4, 25), _INDEX_PM)),
    "XSP": _rules((_START, _INDEX), (date(2022, 12, 12), _INDEX_GTH), (date(2023, 1, 11), _INDEX_PM)),
    "RUTW": _rules((_START, _INDEX), (date(2026, 2, 11), _INDEX_PM)),
    "NDX": _rules((_START, OptionRules(rth_extra=15))),
    "NDXP": _rules((_START, OptionRules(rth_extra=15))),
    **{r: _rules((_START, _ETF_1615), (_PRE_START, replace(_ETF_1615, pre=True)))
       for r in ("SPY", "QQQ", "IWM", "GLD", "XLF")},
    **{r: _rules((_START, _ETF_1615)) for r in ("DIA", "EEM")},
    **{r: _rules((_START, OptionRules()), (_PRE_START, OptionRules(pre=True))) for r in ("AAPL", "TSLA", "NVDA")},
}
_DEFAULT = OptionRules()


def option_rules(root: str, trade_date: date) -> OptionRules:
    changes = OPTION_ROOTS.get(root.upper())
    if not changes:
        return _DEFAULT
    i = bisect.bisect_right([c[0] for c in changes], trade_date) - 1
    return changes[i][1] if i >= 0 else _DEFAULT


# ---------------------------------------------------------------- the calendar

@dataclass(frozen=True)
class Session:
    trade_date: date
    name: str
    start: datetime  # time-zone aware (ET)
    end: datetime

    def holds(self, ts: datetime, kind: Kind) -> bool:
        return self.start <= ts < self.end if kind is Kind.EVENT else self.start < ts <= self.end


def _at(d: date, t: time) -> datetime:
    return datetime.combine(d, t, tzinfo=ET)


def _plus(t: time, minutes: int) -> time:
    return (datetime.combine(date(2000, 1, 1), t) + timedelta(minutes=minutes)).time()


@dataclass
class Calendar:
    """Trading days for whole years [first_year, last_year], from ThetaData's non-standard days."""
    days: dict[date, Day]
    first_year: int
    last_year: int
    _trading: list[date] = field(init=False, repr=False)

    def __post_init__(self):
        if self.first_year > self.last_year:
            raise CalendarError("empty calendar")
        for d in self.days:
            if not self.first_year <= d.year <= self.last_year:
                raise CalendarError(f"{d} is outside the calendar's years")
        out, d, end = [], date(self.first_year, 1, 1), date(self.last_year, 12, 31)
        while d <= end:
            day = self.days.get(d)
            if d.weekday() < 5 and not (day and day.status == "full_close"):
                out.append(d)
            d += timedelta(1)
        self._trading = out

    @property
    def first(self) -> date:
        return date(self.first_year, 1, 1)

    @property
    def last(self) -> date:
        return date(self.last_year, 12, 31)

    def _check(self, d: date) -> None:
        if not self.first <= d <= self.last:
            raise CalendarError(f"{d} is outside the calendar ({self.first} to {self.last})")

    # -- days

    def is_trading(self, d: date) -> bool:
        self._check(d)
        i = bisect.bisect_left(self._trading, d)
        return i < len(self._trading) and self._trading[i] == d

    def next_trading(self, d: date) -> date:
        """The first trading day after d."""
        self._check(d)
        i = bisect.bisect_right(self._trading, d)
        if i == len(self._trading):
            raise CalendarError(f"no trading day after {d} in the calendar (ends {self.last})")
        return self._trading[i]

    def _next_or_none(self, d: date) -> date | None:
        i = bisect.bisect_right(self._trading, d)
        return self._trading[i] if i < len(self._trading) else None

    def prev_trading(self, d: date) -> date:
        """The last trading day before d."""
        self._check(d)
        i = bisect.bisect_left(self._trading, d)
        if i == 0:
            raise CalendarError(f"no trading day before {d} in the calendar (starts {self.first})")
        return self._trading[i - 1]

    def trading_days(self, start: date, end: date) -> list[date]:
        self._check(start)
        self._check(end)
        return self._trading[bisect.bisect_left(self._trading, start):bisect.bisect_right(self._trading, end)]

    def hours(self, d: date) -> tuple[time, time]:
        """The day's regular open and close (09:30-16:00, or an early close's hours)."""
        if not self.is_trading(d):
            raise CalendarError(f"{d} isn't a trading day")
        day = self.days.get(d)
        return (day.open, day.close) if day else (REGULAR_OPEN, REGULAR_CLOSE)

    def is_half_day(self, d: date) -> bool:
        day = self.days.get(d)
        return bool(day and day.status == "early_close")

    # -- sessions

    def gth_end(self, d: date) -> time:
        return GTH_END if d >= GTH_END_CHANGE else GTH_END_BEFORE

    def evening(self, c: date) -> tuple[datetime, datetime] | None:
        """The GTH session opening at 20:15 on calendar day c, if there is one, as (start, end)."""
        if c.weekday() in (4, 5):  # nothing opens Friday or Saturday evening
            return None
        n = c + timedelta(1)
        if self.is_trading(n):
            return _at(c, EVENING_OPEN), _at(n, self.gth_end(n))
        rule = EVENING_BEFORE[holiday_kind(n)]
        if rule is None:
            return None
        return _at(c, EVENING_OPEN), _at(n, self.gth_end(n) if rule == NORMAL else rule)

    def sessions(self, root: str, trade_date: date, expiration: date | None = None,
                 asset: Asset | str = Asset.OPTION) -> list[Session]:
        """Sessions of a trade date for one product, in time order. Empty if it isn't a trading day.

        For options, `expiration` decides whether the contract expires on this trade date (its regular session may
        end earlier and it has no curb); None means a contract that doesn't.
        """
        asset = Asset(asset)
        if not self.is_trading(trade_date):
            return []
        open_, close = self.hours(trade_date)
        S = lambda name, a, b: Session(trade_date, name, a, b)
        if asset is Asset.STOCK:
            return [S("pre", _at(trade_date, STOCK_PRE_START), _at(trade_date, open_)),
                    S("rth", _at(trade_date, open_), _at(trade_date, close)),
                    S("post", _at(trade_date, close), _at(trade_date, STOCK_POST_END))]
        r = option_rules(root, trade_date)
        expiring = expiration == trade_date
        out = []
        if r.gth:
            c = self.prev_trading(trade_date) if trade_date > self._trading[0] else self.first
            while c < trade_date:
                ev = self.evening(c)
                if ev:
                    out.append(S("gth", *ev))
                c += timedelta(1)
        if r.pre:
            out.append(S("pre", _at(trade_date, OPTION_PRE[0]), _at(trade_date, OPTION_PRE[1])))
        rth_end = _plus(close, r.rth_extra_expiring if expiring else r.rth_extra)
        out.append(S("rth", _at(trade_date, open_), _at(trade_date, rth_end)))
        if r.curb and not expiring and not self.is_half_day(trade_date) and rth_end < CURB_END:
            out.append(S("curb", _at(trade_date, rth_end), _at(trade_date, CURB_END)))
        for a, b in zip(out, out[1:]):
            if a.end > b.start:
                raise CalendarError(f"{root} {trade_date}: sessions overlap ({a.name}, {b.name})")
        return out

    # -- labels

    def trade_date(self, ts: datetime, kind: Kind | str) -> date:
        kind = Kind(kind)
        if ts.tzinfo is None:
            raise CalendarError("ts must be time-zone aware")
        et = ts.astimezone(ET)
        d, t = et.date(), et.time()
        rolled = t >= EVENING_OPEN if kind is Kind.EVENT else t > EVENING_OPEN
        if not rolled and self.is_trading(d):
            return d
        return self.next_trading(d)

    def label(self, ts: datetime, root: str, kind: Kind | str, expiration: date | None = None,
              asset: Asset | str = Asset.OPTION) -> tuple[date, str]:
        kind = Kind(kind)
        td = self.trade_date(ts, kind)
        for s in self.sessions(root, td, expiration, asset):
            if s.holds(ts, kind):
                return td, s.name
        return td, "closed"

    def sessions_frame(self, roots: Iterable[str], start: date, end: date, *, expiring: bool | None = None,
                       asset: Asset | str = Asset.OPTION) -> pl.DataFrame:
        """Sessions for trade dates start..end (inclusive), one row each: root, trade_date, expiring, session,
        start, end (UTC). expiring None gives both the non-expiring and the expiring rows for options."""
        asset = Asset(asset)
        flags = [False] if asset is Asset.STOCK else [False, True] if expiring is None else [expiring]
        rows = []
        for root in roots:
            for td in self.trading_days(start, end):
                for e in flags:
                    for s in self.sessions(root, td, td if e else None, asset):
                        rows.append((root, td, e, s.name, s.start, s.end))
        return pl.DataFrame(rows, orient="row", schema={
            "root": pl.String, "trade_date": pl.Date, "expiring": pl.Boolean, "session": pl.String,
            "start": pl.Datetime("us", "UTC"), "end": pl.Datetime("us", "UTC")})

    def label_frame(self, df: pl.DataFrame, kind: Kind | str, asset: Asset | str = Asset.OPTION, *,
                    root: str | None = None) -> pl.DataFrame:
        """`df` with `trade_date` and `session` added; same rows, same order. Needs `ts` (time-zone aware), `root`
        unless `root` is given, and for options optionally `expiration` (a date; null = not expiring).

        The same rules as `label`, done as joins so it scales to a fetch unit's millions of rows.
        """
        kind, asset = Kind(kind), Asset(asset)
        if df.height == 0:
            return df.with_columns(trade_date=pl.lit(None, pl.Date), session=pl.lit(None, pl.String))
        tz = df.schema["ts"]
        if not isinstance(tz, pl.Datetime) or tz.time_zone is None:
            raise CalendarError("ts must be a time-zone aware datetime column")
        if df["ts"].null_count():
            raise CalendarError(f"ts has {df['ts'].null_count()} null value(s)")
        work = df.select(
            pl.int_range(pl.len(), dtype=pl.UInt32).alias("_i"),
            pl.col("ts").dt.convert_time_zone("UTC").dt.cast_time_unit("us").alias("_ts"),
            (pl.lit(root) if root is not None else pl.col("root")).str.to_uppercase().alias("_root"),
            (pl.col("expiration") if asset is Asset.OPTION and "expiration" in df.columns
             else pl.lit(None, pl.Date)).alias("_exp"),
        )
        et = pl.col("_ts").dt.convert_time_zone(ET_NAME)
        work = work.with_columns(_d=et.dt.date(), _t=et.dt.time())
        lo, hi = work["_d"].min(), work["_d"].max()
        self._check(lo)
        self._check(hi)
        cal_days = pl.date_range(lo, hi, "1d", eager=True)
        days = pl.DataFrame({"_d": cal_days,
                             "_trading": [self.is_trading(d) for d in cal_days],
                             "_next": [self._next_or_none(d) for d in cal_days]})
        rolled = pl.col("_t") >= EVENING_OPEN if kind is Kind.EVENT else pl.col("_t") > EVENING_OPEN
        work = (work.join(days, on="_d", how="left")
                    .with_columns(trade_date=pl.when(pl.col("_trading") & ~rolled)
                                  .then(pl.col("_d")).otherwise(pl.col("_next")))
                    .with_columns(_expiring=(pl.col("_exp") == pl.col("trade_date")).fill_null(False)))
        if work["trade_date"].null_count():
            raise CalendarError(f"no trading day after {hi} in the calendar (ends {self.last})")
        sess = self.sessions_frame(work["_root"].unique().to_list(), work["trade_date"].min(),
                                   work["trade_date"].max(), asset=asset)
        sess = sess.rename({"root": "_root", "expiring": "_expiring", "trade_date": "_std", "session": "_name",
                            "start": "_start", "end": "_end"}).sort("_start")
        joined = work.sort("_ts").join_asof(
            sess, left_on="_ts", right_on="_start", by=["_root", "_expiring"], strategy="backward",
            allow_exact_matches=kind is Kind.EVENT, check_sortedness=False)  # sorted just above
        inside = pl.col("_ts") < pl.col("_end") if kind is Kind.EVENT else pl.col("_ts") <= pl.col("_end")
        joined = joined.with_columns(session=pl.when(inside).then(pl.col("_name")).otherwise(pl.lit("closed")))
        labels = joined.sort("_i").select("trade_date", "session")
        return df.with_columns(labels)


def from_rows(rows: Sequence[Day], years: Sequence[int]) -> Calendar:
    """A calendar over the given years, which must be consecutive."""
    ys = sorted(set(years))
    if not ys:
        raise CalendarError("no calendar years")
    if ys != list(range(ys[0], ys[-1] + 1)):
        missing = sorted(set(range(ys[0], ys[-1] + 1)) - set(ys))
        raise CalendarError(f"calendar years aren't consecutive (missing {missing})")
    return Calendar({r.date: r for r in rows}, ys[0], ys[-1])
