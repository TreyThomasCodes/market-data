# market-data

Trey's market-data service. It fetches data from every outside source once, keeps it in one store, and serves it to every client project through one HTTP API.

**Status: Phase 1 (calendar) done 2026-11-22; Phase 2 (store) next.** Trey settled the main choices on 2026-11-21 and 2026-11-22 (below and in Open questions, where each is marked). The order of work is in [Build plan](#build-plan).

## Why

- **Nothing is fetched twice.** The research repo's caches are keyed by the exact query, so the same SPXW day fetched as 09:30–12:00, 12:00–16:00 and 16:01–16:05 is three files, and a fourth script asking for 09:30–16:00 fetches it all again. The service fetches fixed units and cuts every answer from them.
- **One place keeps to ThetaData's 4-request limit.** Each client process holds itself to 4, so two scripts running at once can send 8.
- **One answer to the questions every script re-solves.** Several research scripts each rebuild SPX from option `underlying_price`, find the settlement print (16:05, 13:05 on half days), load a chain at a time, and list trading days.
- **More sources than ThetaData.** Alpaca's free SIP bars, Alphanume's datasets, earnings and economic-release dates now, and later NT8 exports, Databento and breadth data.
- **Fast questions across all of it.** "Every 0DTE chain at one minute of the day since 2022" is one typed call.

What it doesn't change: the first fetch is still limited by each source's speed. The main backtest universe takes roughly 90 hours to fill once (measured on a sample 2026-11-22, see Time semantics).

## Decisions (Trey, 2026-11-21)

1. **Store:** DuckLake, with its catalog in a Postgres instance of its own on the main box, not a database on the existing Timescale instance. Measured against TimescaleDB, see [Why DuckLake](#why-ducklake).
2. **Repo:** this one, separate from the research repo, because other clients use it too.
3. **Host:** straight on Trey's main box for now (16 cores, 125 GB RAM, local NVMe), where the research backtests already run. Queries run inside the service, so it needs real cores.
4. **Live data** (IBKR quotes for live trading) comes later. Results keep the same shapes, so adding live data doesn't change callers.
5. **First sources:** ThetaData (intraday included), Alpaca, Alphanume, the trading calendar, and event dates (earnings, economic releases). NT8 and Databento after.

## Shape

```
clients (research scripts via md.py, other clients)
   │  HTTP only: typed calls, jobs, status.  Results as Arrow (zstd when large)
   ▼
market-data service  (Python, systemd, on the main box)
   │  answers from the store; fetches what's missing first
   │  one queue per source (ThetaData: at most 4 in flight)
   ▼                                  ▼
DuckLake                              ThetaData · Alpaca · Alphanume · event sources
  data:    Parquet on local NVMe
  catalog: Postgres on the same box
```

**The API is the only door.** No client reads the store directly. Two reasons decide it:
- A direct reader skips fetching. A missing day comes back as no rows, not an error, which is a silent gap in a backtest.
- Direct readers are tied to the table layout and to the DuckLake and DuckDB versions. Behind the API, only the service changes when those do.

The service is the only writer. The one exception is reading a Parquet file by hand on this box to check something: allowed, never in a backtest, and never a write.

## What it stores

| Kind | Source | Fetch unit | Notes |
|---|---|---|---|
| Option quotes, intraday | ThetaData `option/history/quote` | (root, expiration, trade date, interval), whole chain, whole session | The 1-minute whole chain is the common case: a SPXW 0DTE day is about 190k rows, 1.4 MB gzipped, about 3 s to fetch. |
| Option trades with quotes | ThetaData `option/history/trade_quote` | (root, expiration, trade date) | Ticks. |
| Option Greeks, intraday | ThetaData `option/history/greeks/first_order` | (root, expiration, trade date, interval) for a chain; (contract, trade date, interval) for one contract | One contract at 1 s is how SPX is rebuilt (below). |
| Option daily | ThetaData first-order Greeks at each product's session close, 15-minute `option/history/ohlc`, `open_interest` | (root, expiration, trade date) | No EOD endpoints (see Time semantics). Trade bars drop empty bars. |
| Contract and expiration lists | ThetaData `option/list/*` | (root or all, trade date) | Small. Lists for past dates are kept; lists for today refetch. |
| Stock bars | Alpaca SIP, minute and daily | (symbol, interval, month) | 2016-01-04 on, 04:00–20:00. Raw prices stored; split and dividend adjustment applied when read. Zero-volume bars Alpaca fills in after a move to OTC are dropped. |
| Index levels | Built from option `underlying_price` | (root, trade date, interval) | Index endpoints return 403 on Trey's tier. SPX at 1 s comes from one near-the-money SPXW contract's Greeks. A `source` column says which contract it came from. |
| Alphanume datasets | Alphanume API | (dataset, query) | Stored as returned. Recent rows keep changing (outcomes, lifecycle columns), so rows less than a set age refetch. |
| Events | To be chosen (see [Events](#events)) | (kind, date range) | Earnings dates with time of day, economic releases, FOMC. |
| Trading calendar | Built in | — | Trading days and early closes (ThetaData's `calendar_year`); sessions per product with dated rule changes; holiday GTH hours; trade-date assignment; settlement time per root. Used by the service and served to clients. |

**Fetch units, not queries.** A request is answered from the store when every unit it touches is covered. Otherwise the missing units are fetched and stored, then the answer is read from the store, so a client sees the same data whether or not a fetch happened. A request for one minute of a chain fetches the whole regular session at that interval, because the next script will want another minute. Other sessions are fetched only when a request asks for them.

**Coverage.** A table in the catalog database records each unit fetched, when, and its row count. Empty answers count, so weekends, holidays and delisted names are never fetched twice. To refetch something a source has corrected, delete its coverage row. There's no version history.

**Freshness.**
- **Today isn't stored.** Requests for a date whose data isn't complete yet pass through uncached. When a date counts as complete is open (see Open questions).
- **Recent Alphanume rows** refetch once older than a set age.
- **Kept forever:** past dates of everything else.

**Store layout.** One DuckLake table per kind above, partitioned by year and month of the trade date, sorted inside each file by root, expiration, strike, right and time. New units land as small files. A merge rewrites each touched month into one file, and files are deleted only after a grace period longer than the longest query, so a running query never loses one.

## Time semantics

**Every time column says exactly what it means (Trey, 2026-11-22).** Sources disagree on what a row "at 10:00" means. NinjaTrader and TradingView, for example, label a bar by different ends. Joining a bar labelled by its start to a snapshot at the same label silently uses a minute of the future. So the store never keeps a time that needs a convention to read:

- **Events** (tick quotes, trades): `ts`, when it happened.
- **Snapshots** (interval quotes, interval Greeks, index levels rebuilt from them): `ts`, the state as of `ts`, including anything at exactly `ts`. That's ThetaData's rule as measured: a 1-second row equals the last tick at or before that second.
- **Bars** (OHLC from any source, daily bars, bars the service builds): `bar_start` and `bar_end`, covering [start, end). Never a single label.
- **End-of-day rows**: the trade date, plus the instant the values stand for (e.g. ThetaData's report time) as its own column.
- All of these are time-zone aware (above), plus `trade_date` and `session`.

Each source's fetch layer converts its own convention once, at ingest, and only after a probe has confirmed what that convention is. The API joins bars to snapshots on `bar_end` = `ts`, so the 10:00 quote meets the bar that finished at 10:00; `md.py` gets a helper for it. A tick at exactly a bar boundary is in the snapshot at that time and in the bar that starts there; accepted as the standard bar convention.

**ThetaData's conventions, probed 2026-11-22** (`spikes/time_semantics.py`; SPY 2024-03-15 0DTE 510 C, SPXW 2024-03-14 5145 C expiring that day and the next):
- **Option OHLC** bars are labelled by their start and cover [t, t+1m), as documented. Bars rebuilt from `trade_quote` that way match on open, close, volume and count for every bar, and on high and low for all but 1–4 bars per day. The documented "SIP rules" probably leave some trades out of high and low. Labelling by the end matches almost nothing. A pull over 00:00–23:59 returns all 1,440 minutes, empty ones included.
- **Interval Greeks** carry the same bid and ask as the interval quote at the same `ts` (391/391 rows, all three cases). `underlying_ms_of_day` always equals `ts`, so it can't show how old the underlying price is.
- **The end-of-day row is a calendar-day report, not a trade date or a session.** It's generated at about 17:19–17:21 ET (`created`), not exactly 17:15. Its OHLCV counts trades from **00:00 on the calendar date up to the report**. For the non-expiring SPXW contract, that is exactly the trades 00:00–17:15: the overnight GTH half is in, and the evening 20:15+ GTH half of the same date, which belongs to the next trade date, is out. So it holds neither trade date D (which also includes the evening before) nor the regular session. Its close is the last trade before the report, 16:15:38 for SPY and 16:14:05 for SPXW, so trades in the 16:00–16:15 session count. Its bid and ask are the last quote before the report, which for non-expiring SPXW is the **last curb quote** (16:59:59.903, 3 × 15.10 / 3 × 24.60) and not the 16:00 or 16:15 quote. Greeks EOD uses the same quote, with `ms_of_day` = last trade time. The expiring SPXW row's open (32.89) and count (14,852 against 14,850 trades) match no trade window; unexplained.
- **Replacing EOD** (`spikes/daily_snapshot.py`; SPY 2024-04-19 monthly, every strike, 20 trade dates in March 2024, one call each). Greeks EOD over the month took 15.8 s on one run and 27.7 s on another. First-order Greeks at 16:00 only (one row per contract-day, bid and ask included) took 35–37 s. Option OHLC over 00:00–23:59 took 55 s at 15 minutes (439k rows, empty bars included) and 50 s at 1 minute (6.6M rows). OHLC volume summed over 00:00–17:15 equals EOD volume for all 5,998 contract-days. 15-minute bars have an edge at every session boundary (07:30, 09:15, 09:30, 16:00, 16:15, 17:00, 20:15; nothing trades 09:25–09:30).
- Trey, 2026-11-22: volume must not be decoupled from price. EOD volume covers a different window from any price snapshot, so EOD is not kept for volume either.
- **So the service doesn't use ThetaData's EOD endpoints (Trey, 2026-11-22).** Daily figures come from a quote/Greeks snapshot at each product's session close from the calendar, plus OHLC bars summed per trade date and session, so price and volume cover windows that say what they are. Cost: roughly 4 times EOD's fetch time on the chain measured. Trade bars are stored at 15 minutes, and bars with no trades are dropped at ingest (Trey, 2026-11-22). ThetaData fills an empty bar with zeros for open, high, low, close and VWAP, so a kept one would read as a real 0.00 price. A missing bar inside a covered unit means no trades in that interval. Sums treat it as zero, and a client that needs every slot gets the grid from the calendar. Main backtest universe fill, measured 2026-11-22 on two random samples, production, 4 in flight: EOD about 57 h; the replacement about 91 h, 1.6 times EOD. Calls vary widely, so these are single-run figures. The README's earlier figure of 25 hours didn't hold.

Related times kept apart: when an event happened against when it was known (earnings at 16:30 ET belong to the next session for trading; true dates for now, see Events), the settlement instant per product (AM-settled SPX on the opening print, PM-settled SPXW at 16:00 or 13:00 on half days), and when the service fetched a unit (coverage).

## Events

Clients need dates for earnings, economic releases and FOMC meetings.

| Event | Source | Probed 2026-11-21 |
|---|---|---|
| Earnings dates and time of day | SEC EDGAR: 8-K filings with Item 2.02, from `data.sec.gov/submissions/CIK##########.json` (`items`, `acceptanceDateTime`) | Free, no key. Times match the releases: Apple's at 16:30 ET (after the close) back to 2015; Delta's 39 and JPMorgan's at about 06:30 ET (before the open). The recent list holds about 1,000 filings, so heavy filers need the older batch files listed with it. `www.sec.gov`, which serves the ticker-to-CIK map and bulk files, returned 403 without a contact in the User-Agent. **No user details go to SEC for now (Trey, 2026-11-21):** use `data.sec.gov` only, with a generic User-Agent, and keep the ticker-to-CIK map for the names we need by hand or from another source. Item 2.02 also covers some non-earnings results (preliminary numbers, restatements). Alphanume `earnings-move-history` stays as a cross-check for the names it covers. |
| Economic releases (CPI, payrolls, PCE, GDP, …) | ALFRED/FRED release dates (`fred/release/dates`) | `FRED_API_KEY` in `.env`, checked 2026-11-21: CPI has 951 release dates back to 1949, payrolls 865, GDP 858, PPI 602, personal income/PCE 741. With `include_release_dates_with_no_data=true` the list also gives scheduled dates ahead (next CPI 2026-12-10). Dates only, no time of day: the time comes from a fixed table per release (CPI, payrolls, PPI at 08:30 ET, and so on). |
| FOMC meetings | The Fed's calendar pages | Free. The current page lists 2021–2027 (57 meetings parsed; `*` marks a projections meeting). Earlier years are one page each (`fomchistorical<year>.htm`) in a looser format that also lists unscheduled meetings and calls. |
| Dividends, splits, lock-ups, spin-offs | Alphanume (`dividend-capture`, `reverse-splits`, `lockup-expirations`, `spinoffs`); Alpaca corporate actions | In hand; Alpaca's not probed. |

**True dates for now (Trey, 2026-11-21).** Earnings dates are the dates that turned out true, not the dates as known beforehand. A client that needs dates as they were known beforehand would need point-in-time dates; source them when one does.

## Fetching

- **ThetaData** (checked 2026-10-21 and 2026-11-21; `spikes/compare_library.py`, `spikes/slot_limit.py`, `spikes/intraday_library.py`, `spikes/intraday_thetadatadx.py`):
  - **Client: [ThetaDataDx](https://github.com/userFRM/ThetaDataDx), pinned to 0.9.0 (Trey, 2026-11-21).**
    - It's an unofficial Rust client with Python bindings. Its authors are active on ThetaData's Discord and known to the owners, and it talks to the same servers as the official `thetadata` library.
    - It's pinned because ThetaData doesn't make breaking changes on its side (it versions an endpoint or gates a change behind a query parameter), so the only thing that can break is ThetaDataDx's own Python interface between releases (13 since July 2026).
    - It has a request pool sized to the account's 4-request limit, retries on `RESOURCE_EXHAUSTED`, and chunked streaming of large responses.
    - Its real-time feed (one session per account, needs Standard) could serve live data later.
    - Its flat files need Professional and cover only the last 7 days, so they're no use here.
    - It returns times as `ms_of_day`, dates as integers (`20240314`) and right as `C`/`P`. The fetch layer converts them once, before storing.
  - **Real data comes from production, not staging, on principle (Trey, 2026-11-21).**
  - **Checks:**
    - The official library matched the terminal on stock EOD, a Greeks EOD chain slice, expirations and a day's traded contracts (2026-10-21).
    - On 2026-11-21, SPXW 0DTE on 2024-03-14 at 1-minute intervals (135,286 rows each for quotes and first-order Greeks):
      - ThetaDataDx matched the official library row for row and value for value, apart from rounding. The only difference: the empty 09:30 underlying price is 0 in ThetaDataDx and NaN in the library.
      - Production and staging differed only at 09:30:00, where staging gives zeros for all 346 contracts and production real quotes, and in one row at 09:48 (5075 call: ask 90.40 × 100 on staging against 90.50 × 17). The 09:30 row looks like a ThetaData quirk (client scripts start at 09:31 around it). It needs running out before any rule is set on it (Trey, 2026-11-22).
    - Speed, median of 3 pulls each:

      | Pull | Terminal (staging) | ThetaDataDx, staging | ThetaDataDx, production | Official library, production |
      |---|---|---|---|---|
      | Quotes | 1.0 s | 0.8 s | 1.8 s | 3.5 s |
      | First-order Greeks | 2.3 s | | 4.6 s | 5.0 s |

      Most of the extra time on production comes before the first byte arrives. Both hostnames resolve to the same address, 45 ms away. So the environment sets most of the speed, and the client makes a smaller difference.
    - **Resolutions, 2026-11-22** (`spikes/resolution_ladder.py`, `spikes/resolution_open.py`; SPXW 0DTE on 2024-03-14, production, one request at a time). Findings only, nothing decided yet:

      | Pull | Fetch | Rows | Parquet |
      |---|---|---|---|
      | Whole chain, 5 min | 2.2 s | 27,334 | 0.1 MB |
      | Whole chain, 1 min | 2.2 s | 135,286 | 0.5 MB |
      | Whole chain, 1 s | 107 s | 8,096,746 | 38.5 MB |
      | One contract (5145 C), 1 s | 1.1 s | 23,401 | 0.2 MB |
      | One contract, tick quotes (09:30–16:00) | 2.8 s | 1,379,066 | 6.0 MB |
      | One contract, `trade_quote` | 0.4 s | 14,846 | 0.3 MB |

      - 5-minute rows equal the 1-minute rows at the same timestamps, on every column, for all 27,334 rows.
      - 1-minute rows equal the 1-second rows at the same timestamps everywhere except 09:30:00, where all 346 contracts differ (below).
      - A 1-second row equals the last tick at or before that second (`<=`, taking the last of ticks sharing a millisecond in the order returned) for every second of the contract's day. With `<` instead, 1,010 seconds differ.
      - The 1-second contract pull equals that contract's rows in the 1-second chain pull.
      - Ticks share milliseconds: 238,899 milliseconds hold more than one tick (up to 16), and quote ticks carry no sequence number, so the order returned is the only order.
      - Quote pulls default to 09:30–16:00. Ticks for this contract run from 04:00:00 to 09:15:00.386, where the last real quote (27 × 31.80 / 9 × 32.80) is followed in the same millisecond by an all-zero quote. Nothing comes until 09:30:03.890.
    - **Session windows, 2026-11-22** (`spikes/session_windows.py`; one near-the-money call per case, tick quotes and `trade_quote` over 00:00–23:59:59, production). Findings only:
      - **SPXW GTH** quotes and trades come back under the calendar date they happen on, not the trade date Cboe assigns them to. Trade date 2024-03-14 holds 00:00–09:15:00.764 and also 20:15:00.152–23:59:58.716 (the next session). ThetaData's docs say GTH runs 20:15 to 09:25 next day, and the end moved from 09:15 to 09:25 "effective trade date Monday, August 26, 2024 (starting hours of operation for Sunday night session, August 25, 2024)". The data agrees: last GTH tick 09:15:00.764 on 2024-03-14, and 09:25:00.2xx on 2026-11-19/20. The docs also say "After January 2022, Theta Data has full GTH / ETH coverage".
      - **SPXW after 16:00:** a non-expiring contract has quotes and trades 16:00–16:15 and quotes (2026: also trades) 16:15–17:00, the curb. An expiring contract stops at 16:00 on both dates checked.
      - **SPY options** run to 16:15, including on expiration day (2024-03-15: 839 trades 16:00–16:15). In 2024 the only pre-09:30 quotes are 0/0 placeholders, 08:00–09:25. By 2026-11-19 there are real quotes and trades from 07:30. Cboe's notices for the C1 pre-market session for equity options gave July 13, 2026, then a "TBD" August 2026 date; the go-live date isn't confirmed.
      - Zero quotes (0/0) appear at session edges: SPXW at the 09:15 GTH end and the curb, SPY pre-market. ThetaData's docs: "OPRA sends quotes with zero bid and or asks during premarket."
      - ThetaDataDx's history calls default to 09:30–16:00, so a default pull leaves out 16:00–16:15 for SPXW and SPY as well as every extended session.
    - **Session questions, 2026-11-22** (`spikes/session_questions.py`; near-the-money calls, tick quotes and `trade_quote` in narrow windows, production). Findings only:
      - **ThetaData's calendar** (`calendar_year`) lists every full close and 13:00 early close 2021–2026, including 2025-01-09.
      - **Sunday GTH** is filed under the Sunday's own date, from 20:15 (2024-03-10, 2026-11-15). Friday dates have nothing after 17:00. **Good Friday 2024-03-29** has no data; the Thursday before has no evening session, and GTH restarts Sunday 2024-03-31 at 20:15. On 2025-07-03, a half day before the July 4 holiday, there is an evening session from 20:15.
      - **GTH data starts in 2022:** none on the 2021 dates checked (SPX and SPXW, up to 2021-12-15); present from 2022-01-18. Curb data: none on 2022-01-18 or 2022-03-15, present on 2022-06-15.
      - **16:00–16:15**, non-expiring near-the-money call. 2024-03-14: quotes for SPX, SPXW, XSP, NDX, NDXP, RUTW, VIX, SPY, QQQ, IWM, DIA, GLD, XLF and EEM; none for TLT, SMH, AAPL, TSLA or NVDA. 2026-11-19: all of them, including TLT, SMH, AAPL, TSLA and NVDA.
      - **The curb (16:15–17:00)** has real activity only for SPX, SPXW, XSP and VIX, plus RUTW by 2026. Everything else has at most a few ticks at 16:15:00.
      - **Expiring contracts:** SPXW, XSP, NDXP, RUTW and AAPL stop at 16:00. Expiring SPY, QQQ and IWM keep quoting to 16:15; trades there vary (above).
      - **Pre-market equity options (07:30–09:25):** none in 2024. The first date with real SPY quotes is **2026-10-19** (Monday; found by bisection, with 10-15 and 10-16 empty and 10-19 to 10-21 present). By 2026-11-19 QQQ, IWM, GLD, XLF, AAPL, TSLA and NVDA have it too; DIA, TLT, EEM and SMH don't. RUTW has GTH quotes by 2026 (none in 2024).
      - **Early-close days** (2024-11-29, 2024-12-24, 2025-07-03, 2025-11-28, 2025-12-24): everything moves 3 hours earlier. Non-expiring SPXW and SPY trade 13:00–13:15, expiring SPXW stops at 13:00, expiring SPY runs to 13:15, and nothing trades after 13:15 (no curb).
    - **Holidays, 2026-11-22** (`spikes/holiday_evenings.py`; SPXW near-the-money call, ticks and trades over the whole calendar day). Findings only:
      - **Christmas and New Year** (2024-12-25, 2025-01-01): no evening session the night before (2024-12-24, 2024-12-31 have nothing after 20:00), nothing on the holiday until the evening session opens at 20:15.
      - **Thanksgiving, MLK Day, July 4** (2024-11-28, 2025-01-20, 2025-07-04, 2025-11-27): the evening session before the holiday runs on **until about 11:30 ET on the holiday** (ticks 00:00–11:29, trades up to 11:28), not 09:25. An evening session opens at 20:15 on the holiday itself, except July 4 2025, which was a Friday. A Sunday evening session runs before MLK Monday (2025-01-19).
      - **The 2025-01-09 closure** (national day of mourning): the overnight session ends at 09:25 as normal; evening session from 20:15.
      - Half days: no evening session after Friday 2024-11-29 (a Friday); an evening session after Thursday 2025-07-03 (before the July 4 holiday).
      - So the calendar needs holiday GTH hours per holiday. ThetaData's `calendar_year` doesn't carry them. By Cboe's rule (above), all GTH between two regular sessions takes the next business day's trade date. For Thanksgiving 2024 that puts Wednesday evening through Thursday 11:30 and Thursday evening into trade date Friday 2024-11-29. That assignment is Cboe's rule, not visible in ThetaData's data.
      - **Every holiday 2022–2026, 2026-11-22** (`spikes/holiday_evenings.py` with each holiday's evening before and the holiday itself). The rule holds without exception from 2022-05 on: no evening session before Christmas, New Year's Day or Good Friday (including Christmas and New Year observed on a Monday: nothing on Sunday 2022-12-25, 2023-01-01, 2023-12-31); before every other holiday (MLK, Presidents, Memorial, Juneteenth, July 4, Labor Day, Thanksgiving, also when observed on a Friday, 2026-06-19 and 2026-07-03) the evening session runs on to about 11:30 on the holiday; and an evening session opens at 20:15 on any holiday that isn't a Friday. Cboe's hours page (cboe.com/about/hours/us-options, read 2026-11-22) lists the same hours for 2026 and 2027. Two gaps in ThetaData, not in the rule: nothing at all on MLK and Presidents Day 2022 (Sunday evening and the holiday), though the morning after has GTH from 00:00; and no 20:15–24:00 ticks on New Year's Day 2026, though Cboe lists that session and 2026-01-02 has ticks from 00:00.
      - **When GTH and the curb start per root, 2026-11-22** (`spikes/calendar_probe.py`: bisection over trading days 2022-01-18 to 2026-11-19, near-the-money non-expiring call, more than 10 real quotes counts). First trade date with GTH quotes: SPX and SPXW already on 2022-01-18, VIX 2022-03-15, XSP 2022-12-12, RUTW 2026-02-11 (no RUTW chain at 12:00 on 02-09 and 02-10). Curb: SPX, SPXW and VIX 2022-04-25, XSP 2023-01-11, RUTW 2026-02-11. NDXP has neither. `calendar_year` answers for 2012–2027 (NotFound before 2012 and for 2028); early closes are all 09:30–13:00.
      - ThetaDataDx logs a warning above about 100 MB of response (the 1-second chain and the contract's ticks) and suggests `.stream(handler)`.
  - A new login invalidates the previous one on the same account and environment. So the service makes one client and shares it across its threads, and nothing else logs in to production while it runs. Trey's terminal is on the staging environment and isn't disturbed. A ThetaDataDx login to staging on 2026-11-21 didn't disturb the terminal either.
  - Over the limit fails at once with `RESOURCE_EXHAUSTED`; nothing queues on ThetaData's side. The service holds itself to 4 and treats `RESOURCE_EXHAUSTED` as "retry shortly", not as an outage.
  - Date ranges are split into ThetaData's 365-day maximum. `symbol=*` isn't allowed, and `expiration=*` goes one day per call.
- **Two priorities.** Interactive requests (a client waiting) and bulk jobs. Bulk uses at most 3 of the 4 ThetaData slots, so a 25-hour job never locks out a quick lookup.
- **Outages pause the queue.** When requests start failing together (timeouts, 5xx, connection errors), the queue stops, checks with one cheap request every minute or two, and resumes. Failed units go back on the queue, not into a failure list. On 2026-10-21 a client backfill without this logged 445 failures in one second while the terminal was down.
- **Alpaca and Alphanume** each get their own queue under their own rate limits. Keys come from the service's environment file, never from requests.
  - **Alpaca free tier: 200 API calls a minute** (Trey, 2026-11-22). The service paces itself under it, with interactive requests first, as with ThetaData. Every page of a paginated response counts as a call. What Alpaca returns over the limit isn't known until an Alpaca spike (see [Open questions](#open-questions)).
- **Billed sources** (Databento, when added) answer cost quotes freely. A billed pull refuses to run unless the request carries a dollar cap Trey approved.

## Interface

1. **Typed calls** for what the scripts do most. They take date ranges, time-of-day and session filters (RTH unless asked), and expiration and strike filters, and they stream results a day at a time, so a long range never has to fit in memory. Clients aggregate in Polars.
   - `GET /chain`: root, date, time or time range, optional expirations and strikes
   - `GET /quotes`: one contract, a range
   - `GET /bars`: symbol, interval, range, adjustment. Picks the source by rule (Alpaca for stocks, the option-derived series for indexes) and returns a `source` column. Any call can force a source.
   - `GET /events`: kind, date range, symbols
   - `GET /calendar`: sessions in a range
2. **No SQL endpoint (Trey, 2026-11-22).** SQL against the tables would tie clients to the table layout, which is exactly what the API exists to prevent, and it would need its own scope and lockdown machinery. If a measured workload is too slow through typed calls, it gets its own typed endpoint.
3. **No ThetaData-compatible paths (Trey, 2026-11-22): a clean break.** Existing clients and old scripts move to the typed calls and `md.py`; nothing mimics the terminal's URLs or CSV columns.
4. **Jobs:** `POST /jobs` with a list of requests to prefetch, `GET /jobs/<id>` for progress. A job runs on the server, so it survives a client disconnecting. The main backtest universe fetch is one job.
5. **Status:** `GET /status` shows queue depth per source, ThetaData slots in use, store and catalog health, and last merge.

**Results** are Arrow streams, zstd-compressed above a size threshold. Polars and pandas load them without converting. Errors are JSON.

**`md.py`**, a thin Python client, is the only client library: `md.chain("SPXW", "2024-03-14", "12:00")`, `md.bars("AAPL", "1m", start, end)`, each returning a Polars frame. Scripts don't know what's behind the API, so the store can change without touching them.

## Why DuckLake

DuckLake keeps a table as ordinary Parquet files plus a small SQL database, the catalog, that records which files make up the table at each snapshot. A write adds a Parquet file and commits one catalog row; a merge swaps files in one transaction; old files are deleted after a grace period. Readers each work from one snapshot, so they never see half a write or a file twice. The data stays plain Parquet. Measured on 267M rows of SPXW one-minute chain quotes from the research repo's cache, on the main box with a warm cache (scripts in `spikes/storage/`):

| | DuckLake (Postgres catalog) | TimescaleDB (compressed) |
|---|---|---|
| Size | ~0.94 GB | 3.0 GB (44 GB uncompressed) |
| Load 267M rows | ~25 s | 14.5 min, plus 9 min to compress |
| One chain at one minute | 15–30 ms | 1–6 ms |
| 0DTE chain at one minute on every day | 0.5–0.7 s | 2.0–2.9 s |
| Reads while writing at full speed | correct; writer 404k rows/s | correct; writer 51k rows/s |

- Plain Parquet without a catalog gave errors and double-counted a day while files were being merged, so it isn't an option.
- DuckLake's catalog in SQLite stalled lookups to about 0.5 s during writes; in Postgres it didn't.
- The HTTP and Arrow API cost nothing measurable on lookups and sweeps, and with 8 clients was faster than 8 direct readers (387 against 264 lookups a second). Raw pulls of millions of rows cost more (6.7M rows: 124 ms direct, 392 ms through the API with zstd), so typed calls stream by day and clients process each piece as it arrives.
- **The trade-off:** single lookups are slower than TimescaleDB's, because each query first asks the catalog for its files. If that ever matters, the service can keep the live file list in memory and read the right file directly (about 3 ms in the test).
- **Two traps to avoid in the build:**
  - Send each HTTP response's headers and body in one write, or kept-alive connections wait about 40 ms on the delayed-ACK timer.
  - Pin the DuckLake and DuckDB versions. The test ran catalog format 1.1 on DuckDB 2.0.1; it's young, so upgrade deliberately.

## Moving existing data in

- **The research repo's ThetaData cache isn't imported (Trey, 2026-11-22).** It was fetched (6.8 GB, 77,889 files) through Trey's terminal, which runs on staging, and the service takes real data from production only (Trey, 2026-11-21). Staging and production differ in places (the 09:30 row on 2024-03-14, one row at 09:48). Everything is refetched from production as requested. That includes the stock EOD backfill (35,037 files) and the Greeks EOD files (3,007), which the service wouldn't use anyway (see Time semantics); the free stock tier refuses dates before 2023-06-01 ("requiring a VALUE subscription", checked 2026-11-22). Those files stay where they are, for finished work.
- **The research repo's Alphanume downloads:** imported as stored queries.
- **Research scripts keep their own caches until moved.** Finished backtests aren't rewritten. New work uses `md.py`. When the service is live, the research repo's ThetaData rule changes to "all data through `md.py`".
- **Other clients' stores:** left alone for now.

## Deployment

- **Service:** Python, under systemd as its own user on the main box. Listens on the LAN only.
- **Data:** Parquet under a data directory on local NVMe (git-ignored). Not on network storage, which slows every sweep.
- **Catalog:** its own Postgres instance on the same box (Trey, 2026-11-21), so each query's catalog round trip stays local.
- **Secrets:** `THETADATA_API_KEY`, the Alpaca and Alphanume keys, and event-source keys go in an environment file readable only by the service user. Never in the repo, never in logs.
- **Upkeep:** merges, snapshot expiry and file cleanup run on a schedule in the service.

## Build plan

Revised 2026-11-22 to match the decisions above. Each phase ends with a check that has to pass before the next one starts. The probes behind the findings become test cases.

**Phase 0: Skeleton**
- uv project with exact pins: DuckDB 2.0.1, DuckLake catalog format 1.1, ThetaDataDx (`thetadatadx-py`) 0.9.0, Polars, Starlette, uvicorn, psycopg.
- A catalog Postgres instance of its own (not the `md-bench-tsdb` container, which stays as it is). A data directory on local NVMe, outside git.
- An environment-file loader that never logs a secret. pytest.
- Done when: the service starts, attaches an empty DuckLake catalog on the new Postgres (tables come in Phase 2), and `GET /status` answers.
- **Done 2026-11-22.** Catalog: Postgres 17.12 in the `md-catalog` container (`deploy/catalog.compose.yml`, `127.0.0.1:55433`, database `md_catalog`, named volume `md-catalog-pg`). Data under `data/` in the repo (git-ignored) until Phase 4's systemd unit. Settings come from `.env`, overridden by non-empty process environment values. A relative `MD_DATA_DIR` is taken from the env file's directory. Values containing `$`, ` #` or a backslash are refused, because docker compose reads them differently. The password reaches DuckDB as a secret, so it isn't in the ATTACH string, and `/status` gives only an error's type, never its text. The service won't start if others have any access to `.env`. DuckDB runs in UTC: it's set when connecting, because a session `SET` doesn't carry over to cursors and the box's zone is America/Chicago.
- Run: `docker compose -f deploy/catalog.compose.yml --env-file .env up -d`, then `uv run market-data` (port 8780) and `uv run pytest`.

**Phase 1: The calendar.** It comes first because every label, unit and default depends on it.
- Trading days and early closes from ThetaData's `calendar_year`, stored.
- Session rules per product group, each with an effective date: GTH 20:15 to 09:15, then 09:25 from trade date 2024-08-26; regular hours ending at 16:15 or 16:00 per product and expiry; curb 16:15–17:00 for SPX, SPXW, XSP, VIX and RUTW; equity-option pre-market 07:30–09:25 from 2026-10-19; half days 13:00 or 13:15.
- A hand-kept table of holiday GTH hours: none before Christmas and New Year, to about 11:30 ET on Thanksgiving, MLK Day and July 4.
- Trade-date assignment as the exchange does it, including the holiday rule.
- The labelling functions: from (`ts`, product, expiration) to (`trade_date`, `session`). Snapshots use (start, end], and events and bars use [start, end).
- `GET /calendar`.
- Done when: every probed date gives the measured answer. That includes 2024-03-14 (GTH to 09:15), 2024-08-26 (09:25 from then), 2024-11-28 (Thanksgiving GTH to 11:30), 2025-01-19 (Sunday before MLK), 2025-07-03 (half day with an evening session), and the 09:30:00 row falling outside regular hours.
- **Done 2026-11-22.** `src/market_data/calendar.py` (rules and labels), `calendar_store.py` (stored years, refresh), `thetadata.py` (`calendar_year` only, until Phase 3's shared client), `GET /calendar`.
  - `calendar_year` for 2012 to next year is stored in DuckLake (`calendar_days`, `calendar_years`), each year in one transaction. Past years are kept; the current and next year are refetched once a day, and a change is logged. The service refreshes at start (off the event loop), then checks hourly, fetching only what's due; each fetch logs in to production and closes, and a next year ThetaData hasn't published is asked for at most once a day. A refresh stores all its years or none. Without a key, or while ThetaData is down, it serves what's stored; `/status` fails only when no calendar covers today.
  - Holidays are named by rule (`exchange_holidays`), since `calendar_year` gives no names; a test checks every full close 2012–2027 is a named holiday or one of the four special closures, and every named holiday is a full close. Holiday GTH hours are kept per holiday kind (above), not per date.
  - Trade date: rolls at 20:15 ET for every product; on a day that isn't a trading day a row takes the next trading day. Sessions: options `gth`, `pre`, `rth`, `curb`; stocks `pre` (04:00), `rth`, `post` (to 20:00); `closed` for any time in no session. The names are proposed, not settled (Open questions). Events and bars by `bar_start` use [start, end), snapshots (start, end], including at the 20:15 roll.
  - `label_frame` labels a frame by joins: 0.02 s for a one-minute SPXW chain day, 0.84 s for a one-second one (8.4M rows); tests check it equals the per-row `label` on a grid around every session boundary.
  - `GET /calendar?root=&start=&end=[&expiration=][&asset=stock]`: Arrow, trade_date, session, start, end (ET).

**Phase 2: The store.** No fetching yet.
- One table per kind:
  - `ts` (time-zone aware), `trade_date` and `session` stored on every row, computed from `ts` and the calendar.
  - `seq` on quote ticks.
  - `bar_start` and `bar_end` on bars.
  - Partitioned by trade-date month.
- Coverage committed in the same DuckLake transaction as the data.
- The read path: list a request's units and split them into covered and missing.
- The label check: recompute `trade_date` and `session` and compare.
- Done when: the reads-while-writing correctness test from `spikes/storage/rw.py` passes as a real test. Merge settings wait for the merge spike.

**Phase 3: Fetching from ThetaData**
- One shared production client with 4 slots, bulk work limited to 3, `RESOURCE_EXHAUSTED` retried, `NotFoundError` stored as an empty answer, a pause during outages, single fetch for duplicate requests, and streaming for responses over about 100 MB.
- Conversions when data comes in: `ms_of_day` and integer dates to `ts`, `C`/`P` rights, `seq` on quote ticks, empty OHLC bars dropped, labels added.
- Kinds in this order:
  1. 1-minute whole-chain quotes, regular hours. 5-, 15- and 30-minute answers are read from them.
  2. Contract and expiration lists.
  3. 1-minute Greeks.
  4. Daily data: the session-close Greeks snapshot, 15-minute OHLC and open interest.
  5. 1-second data and ticks per contract, and `trade_quote`.
  6. Index levels from Greeks.
- Done when: the SPXW 0DTE chain for 2024-03-14 at 1 minute is fetched once, answered from the store the second time, and matches the probe (135,286 rows).

**Phase 4: The API and `md.py`**
- `GET /chain`, `/quotes`, `/bars`, `/calendar` and `/status`, then `/jobs`.
- Arrow streamed a day at a time, each response's headers and body sent in one write, regular hours by default.
- `md.py`, including a helper that joins bars to snapshots on `bar_end` = `ts`.
- A systemd unit listening on the LAN only.
- Done when: one existing study ported to `md.py` matches its cached version. The expected differences are where the rules changed: no 09:30:00 row in regular hours, and production data instead of staging.

**Phase 5: Other sources.** Alpaca after its spike, then Alphanume, then events: EDGAR Item 2.02 filings, FRED release dates with a table of release times, and FOMC dates. `GET /events`.

**Phase 6: Operations**
- Backups to the TrueNAS `backups` share, once the share setup is ready.
- The merge schedule, after the merge spike.
- The freshness rule, after the date-completeness results.
- The main backtest universe's fill as a job (about 90 hours).
- The research repo's rule changes to "all data through `md.py`".

**Spikes and when they're needed**
- Merge sizes and grace period: before Phase 2's upkeep code.
- Date completeness: before Phase 6's freshness rule. It's running now.
- Alpaca: before Phase 5.
- Expiring ETF options after 16:00, and when each ETF joined the 16:15 group: before the Phase 1 rules for those products are final.

## Later

- Live IBKR quotes and bars for live trading, in the same result shapes.
- NT8 futures exports (manual, from Windows) and Databento CME bars behind the cost-cap gate.
- Breadth data, if a test needs it.

## Deliberately left out

- Evidence hashing, manifests, request fingerprints, provenance graphs and version history.
- User accounts and anything public.
- Writes from clients.
- A SQL endpoint (Trey, 2026-11-22).

## Open questions

- **Alpaca spike:** what it returns over the 200 calls/min limit, its page size for bars, whether a multi-symbol request saves calls, whether bars are labelled by start or end, whether daily bars include extended hours, and its corporate-actions endpoint. Not probed yet.
- **The 09:30 row: decided (Trey, 2026-11-22).** Rows are stored as returned, and a snapshot belongs to the session whose (start, end] contains its time. So the 09:30:00 row is pre-open state and falls outside RTH, the close snapshot falls inside RTH, and RTH answers at 1 minute start at 09:31. That matches the bar join (the first RTH bar ends at 09:31). There is no special case for 09:30. The evidence (`spikes/session_questions.py`, 2026-11-22, production unless stated): the interval row at 09:30:00 is never a regular-session quote. The first regular-session tick comes at 09:30:00.0xx for SPY, QQQ and AAPL options and at 09:30:02–04 for SPXW, so the 09:30:00 row holds whatever came before. That is either an all-zero quote or the last quote from before the open: SPXW's last GTH quote from 09:15 (2024-03-14, 2024-06-12) or 09:25 (2026-11-19/20), or the last pre-market quote from 09:25 for equity options in 2026. From 2024-08-29 to 2025-09-15, GTH ends on an all-zero quote, and every root checked shows 0/0 at 09:30:00. Before the equity pre-market began, SPY, QQQ and AAPL show only 0/0 placeholders before 09:30. Production and staging agree except on 2024-03-14, where production's 1-minute row skipped the closing 0/0 tick that staging returned. On 2024-08-29 production returned the 0/0 too, so the tie-break between ticks sharing a millisecond isn't consistent. This is likely why client scripts start at 09:31.
- **Resolutions in the store: decided (Trey, 2026-11-22).** 5-, 15- and 30-minute quote answers are read from stored 1-minute rows. 1-second and tick data are fetched per contract by default; a whole chain at 1 second only on explicit request. Ticks follow the RTH-only rule.
- **Session semantics** (Trey, 2026-11-22: the service has to deal with them): sessions per product with dated rule changes, which trade date evening GTH belongs to, what a fetch unit's time window is, and what clients get by default. Trade date, from the exchanges (checked 2026-11-22): it rolls when the evening session opens, not at the regular close. Cboe: GTH trades from 20:15 to midnight take the next business day's trade date, trades after midnight take that day's, and curb trades (16:15–17:00) take the same trade date as the regular session before them. Before a Friday holiday, both Thursday evening and Sunday evening GTH take Monday's trade date. CME futures likewise roll the trade date at the evening open after the daily break. **Decided: fetch and return regular trading hours only, unless a request explicitly asks for another session (Trey, 2026-11-22).** RTH follows the exchange (Trey, 2026-11-22): it ends when the product's regular session ends, which is 16:15 for products that trade then, 16:00 for expiring PM-settled index options and single stocks, and 13:00 or 13:15 on half days, all from the calendar. So there is no separate `late` session. **Decided: trade dates follow the exchange (Trey, 2026-11-22).** A trade date starts when the evening session opens. Curb trades keep the regular session's trade date. Rows keep their real time: `ts` is ThetaData's date plus `ms_of_day` as one time-zone-aware timestamp (UTC inside, shown in ET), never bare clock time (Trey, 2026-11-22). `trade_date` and `session` are added as labels from the calendar, so a quote at 21:00 ET on 2024-03-14 keeps that `ts` and gets `trade_date` 2024-03-15, `session` `gth`. Fetch units and coverage stay on ThetaData's own date, since that's what its API takes. Still open: whether expiring ETF options trade after 16:00 (SPY 2024-03-15: 839 trades 16:00–16:15; SPY, QQQ and IWM 2026-11-20: quotes but at most 2 trades); when each ETF joined the 16:15 group.
- **`trade_date` and `session` are stored on every row (Trey, 2026-11-22).** They're computed from `ts` and the calendar when written, never copied from a source. A check recomputes them and compares, and a calendar correction rewrites the affected months. Benchmark (`spikes/storage/session_labels.py`, 267M rows, DuckDB 2.0.1, warm, median of 5), stored against joined from a sessions table at read time: one chain at one minute 11.4 against 11.5 ms; the 0DTE chain at one minute every day 2.1 against 29.2 s; one month of RTH rows 107 against 292 ms; a group-by over all data 0.52 against 9.0 s. Storage 1.12 GB both ways. Rewriting one month took 0.3 s. The benchmark data holds one corrupt row (trade date 3460-12-06, timestamp 2025-06-24 11:19); labels computed from `ts` would have caught it.
- **Build proposals (Trey, 2026-11-22):**
  - **Accepted:** coverage committed in the same DuckLake transaction as the data, so the two can't disagree after a crash.
  - **Accepted:** single fetch. Two requests that need the same missing unit share one fetch.
  - **Accepted:** a `seq` column on quote ticks, numbering them per contract and day in the order ThetaData returns them. ThetaData support (Discord, 2026-08-17): "the quotes are listed in the order they are received. No sequence number is available for the quote." Up to 16 share a millisecond, and ThetaData's own snapshots follow that order, so it has to survive sorting and merges. Trades carry OPRA's `sequence` already.
  - **Accepted:** ThetaDataDx's streaming interface above its ~100 MB warning.
  - **Accepted:** Starlette on uvicorn, with each response's headers and body sent in one write.
- **When a date is complete:** with no EOD report to wait for, the service needs its own rule. Being measured: `spikes/date_completeness.py` watches trade date 2026-11-23 at 18 checks from 10:30 ET that day to 09:00 ET on 2026-11-25 (row counts, last timestamps, checksums, errors). ThetaData files the evening GTH session under the calendar date until 23:59, and its docs say "data from the previous day isn't available from 00:00 ET to 01:45 ET".
- **Backups: decided (Trey, 2026-11-22).** A nightly `pg_dump` of the catalog plus a weekly copy of the Parquet directory to the `backups` share on Trey's TrueNAS server (Trey, 2026-11-22; not mounted on this box yet). How the service authenticates waits until TrueNAS's share setup changes, which Trey plans.
- **Calendar, left open by Phase 1 (2026-11-22):**
  - Session names (`gth`, `pre`, `rth`, `curb`, `post`, `closed`) are a proposal.
  - A tick a few milliseconds after a session's end (the closing quotes at 09:15:00.386, 16:15:00.0xx) is labelled `closed`. How many rows that is gets counted in Phase 3.
  - GTH and curb start dates are the first dates ThetaData has them, not exchange announcements. Equity-option pre-market is set from 2026-10-19 for SPY, QQQ, IWM, GLD, XLF, AAPL, TSLA and NVDA; only SPY's start was found by bisection.
  - TLT and SMH are treated as 16:00 products, though they quoted 16:00–16:15 on 2026-11-19 (the ETF spike below). RUT (AM) isn't listed; it falls back to 09:30–16:00.
  - Stock post-market on half days is set to end at 20:00, not probed (Alpaca spike).
  - Settlement instants per root aren't in the calendar yet.
- **Merge schedule and grace period**, and why merged months came out about 1.6 times bigger than bulk-loaded ones in the benchmark (56 MB against 33–37 MB).
