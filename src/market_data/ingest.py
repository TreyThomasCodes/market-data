"""Conversions from ThetaDataDx 0.9.0 frames (`to_polars()` of its typed lists) to the store's columns.

Done once, before storing (README: Fetching):
- `date` (integer YYYYMMDD) + `ms_of_day` -> `ts`, time-zone aware. ms_of_day is ET wall-clock time.
- integer expirations -> DATE; rights -> `C`/`P`; the unit's root is added (ThetaData's rows don't carry it).
- quote ticks get `seq`: their order within a contract as returned (ThetaData has no sequence number for quotes).
- trade bars: `bar_start` = the row's time (ThetaData labels OHLC by bar start), `bar_end` = start + interval; bars
  with no trades are dropped (ThetaData fills them with 0.00 prices).

Assumptions from ThetaDataDx's type stubs, to confirm on the first production fetch (README: Phase 3): column names
as in the stubs; strikes in dollars; `right` as C/P or call/put; ms_of_day is wall-clock time (so the repeated hour
when clocks go back is ambiguous; that raises until it's probed). Anything else unexpected raises IngestError
instead of storing a guess.
"""
from __future__ import annotations

import polars as pl

from .calendar import ET_NAME
from .tables import TABLES
from .units import Unit


class IngestError(Exception):
    pass


QUOTE_COLS = ["bid_size", "bid_exchange", "bid", "bid_condition", "ask_size", "ask_exchange", "ask", "ask_condition"]
TRADE_COLS = ["sequence", "condition", "size", "exchange", "price", "condition_flags", "price_flags", "volume_type",
              "records_back"]
GREEKS_COLS = ["bid", "ask", "delta", "theta", "vega", "rho", "epsilon", "implied_volatility", "iv_error",
               "implied_vol_source", "underlying_price"]
OHLC_COLS = ["open", "high", "low", "close", "volume", "count", "vwap"]
RIGHTS = {"C": "C", "CALL": "C", "P": "P", "PUT": "P"}


def _need(df: pl.DataFrame, cols: list[str], what: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise IngestError(f"{what}: ThetaData frame lacks {missing} (has {df.columns})")


def _as_date(col: str) -> pl.Expr:
    """An integer YYYYMMDD (or an already-typed date) column as DATE."""
    c = pl.col(col)
    return (pl.when(c.is_null()).then(None)
            .otherwise(c.cast(pl.Int64).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=True)))


def _date_col(df: pl.DataFrame, col: str) -> pl.Expr:
    return pl.col(col) if df.schema[col] == pl.Date else _as_date(col)


def _ts(df: pl.DataFrame, date_col: str, ms_col: str) -> pl.Expr:
    """ET wall-clock date + ms of day -> a UTC timestamp."""
    local = _date_col(df, date_col).cast(pl.Datetime("us")) + pl.duration(milliseconds=pl.col(ms_col).cast(pl.Int64))
    return local.dt.replace_time_zone(ET_NAME, ambiguous="raise", non_existent="raise").dt.convert_time_zone("UTC")


def _contract(df: pl.DataFrame, unit: Unit) -> list[pl.Expr]:
    _need(df, ["expiration", "strike", "right"], unit.kind)
    rights = df["right"].cast(pl.String).str.to_uppercase()
    bad = rights.filter(~rights.is_in(list(RIGHTS)) | rights.is_null()).unique().to_list()
    if bad:
        raise IngestError(f"{unit.key}: unknown right value(s) {bad[:5]}")
    return [pl.lit(unit.root).alias("root"), _date_col(df, "expiration").alias("expiration"),
            pl.col("strike").cast(pl.Float64).alias("strike"),
            pl.col("right").cast(pl.String).str.to_uppercase().replace_strict(RIGHTS).alias("right")]


def _check_strikes(df: pl.DataFrame, unit: Unit) -> None:
    """Strikes in dollars, not ThetaData's old 1/1000ths: a whole-chain answer for an index can't have every strike
    above 100,000. Cheap guard for the stubs' `float` strikes."""
    if df.height and df["strike"].min() > 100_000:
        raise IngestError(f"{unit.key}: strikes look like 1/1000ths of a dollar (min {df['strike'].min()})")


def _finish(df: pl.DataFrame, unit: Unit) -> pl.DataFrame:
    t = TABLES[unit.kind]
    want = [c for c in t.names if c not in t.computed]
    _check_strikes(df, unit)
    return df.select(want)


def convert(unit: Unit, raw: pl.DataFrame | None) -> pl.DataFrame:
    """The unit's ThetaData answer (None: NotFound, an empty answer) as the frame `write_unit` takes."""
    t = TABLES[unit.kind]
    want = [c for c in t.names if c not in t.computed]
    if raw is None or raw.height == 0:
        return pl.DataFrame(schema={c: _EMPTY_TYPES[ty.split()[0]] for c, ty in t.columns if c in want})
    k = unit.kind
    if k in ("option_quote", "option_quote_tick"):
        _need(raw, ["date", "ms_of_day", *QUOTE_COLS], k)
        df = raw.with_columns(*_contract(raw, unit), _ts(raw, "date", "ms_of_day").alias("ts"))
        if k == "option_quote":
            df = df.with_columns(pl.lit(unit.interval_ms, pl.Int32).alias("interval_ms"))
        else:
            df = df.with_columns(pl.int_range(pl.len(), dtype=pl.Int32).over("expiration", "strike", "right")
                                 .alias("seq"))
        return _finish(df, unit)
    if k == "option_trade_quote":
        _need(raw, ["date", "ms_of_day", "quote_ms_of_day", *TRADE_COLS, *QUOTE_COLS], k)
        df = raw.with_columns(*_contract(raw, unit), _ts(raw, "date", "ms_of_day").alias("ts"),
                              _ts(raw, "date", "quote_ms_of_day").alias("quote_ts"))
        return _finish(df, unit)
    if k == "option_greeks":
        _need(raw, ["date", "ms_of_day", "underlying_date", "underlying_ms_of_day", "lambda_", *GREEKS_COLS], k)
        df = raw.with_columns(*_contract(raw, unit), _ts(raw, "date", "ms_of_day").alias("ts"),
                              _ts(raw, "underlying_date", "underlying_ms_of_day").alias("underlying_ts"),
                              pl.col("lambda_").alias("lambda"),
                              pl.lit(unit.interval_ms, pl.Int32).alias("interval_ms"))
        return _finish(df, unit)
    if k == "option_ohlc":
        _need(raw, ["date", "ms_of_day", *OHLC_COLS], k)
        df = raw.filter(pl.col("count") > 0)  # no trades: ThetaData's 0.00 filler bar
        df = df.with_columns(*_contract(df, unit), _ts(df, "date", "ms_of_day").alias("bar_start"),
                             pl.lit(unit.interval_ms, pl.Int32).alias("interval_ms"))
        df = df.with_columns((pl.col("bar_start") + pl.duration(milliseconds=unit.interval_ms)).alias("bar_end"))
        return _finish(df, unit)
    if k == "option_open_interest":
        _need(raw, ["date", "ms_of_day", "open_interest"], k)
        df = raw.with_columns(*_contract(raw, unit), _ts(raw, "date", "ms_of_day").alias("ts"))
        return _finish(df, unit)
    if k == "option_contracts":
        _need(raw, ["symbol", "expiration", "strike", "right"], k)
        other = raw.filter(pl.col("symbol") != unit.root)["symbol"].unique().to_list()
        if other:
            raise IngestError(f"{unit.key}: rows for other roots {other[:5]}")
        df = raw.with_columns(*_contract(raw, unit), pl.lit(unit.date, pl.Date).alias("date"),
                              pl.lit(unit.list_type).alias("list_type"))
        return _finish(df, unit)
    raise IngestError(f"no conversion for {k}")


_EMPTY_TYPES = {"TIMESTAMPTZ": pl.Datetime("us", "UTC"), "DATE": pl.Date, "VARCHAR": pl.String, "DOUBLE": pl.Float64,
                "INTEGER": pl.Int32, "SMALLINT": pl.Int16, "BIGINT": pl.Int64}
