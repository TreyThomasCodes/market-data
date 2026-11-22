# market-data

Trey's market-data service. It fetches data from every outside source once, keeps it in one store, and serves it to every client project through one HTTP API.

**Status: design.** Nothing is built yet. Trey settled the main choices on 2026-11-21 (below); the open questions are at the end.

## Why

- **Nothing is fetched twice.** The research repo's caches are keyed by the exact query, so the same SPXW day fetched as 09:30–12:00, 12:00–16:00 and 16:01–16:05 is three files, and a fourth script asking for 09:30–16:00 fetches it all again. The service fetches fixed units and cuts every answer from them.
- **One place keeps to ThetaData's 4-request limit.** Each client process holds itself to 4, so two scripts running at once can send 8.
- **One answer to the questions every script re-solves.** Several research scripts each rebuild SPX from option `underlying_price`, find the settlement print (16:05, 13:05 on half days), load a chain at a time, and list trading days.
- **More sources than ThetaData.** Alpaca's free SIP bars, Alphanume's datasets, earnings and economic-release dates now, and later NT8 exports, Databento and breadth data.
- **Fast questions across all of it.** "Every 0DTE chain at one minute of the day since 2022" is one query.

What it doesn't change: the first fetch is still limited by each source's speed. The main backtest universe still takes about 25 hours to fill once.

## Decisions (Trey, 2026-11-21)

1. **Store:** DuckLake, with its catalog in a Postgres instance of its own on the main box, not a database on the existing Timescale instance. Measured against TimescaleDB, see [Why DuckLake](#why-ducklake).
2. **Repo:** this one, separate from the research repo, because other clients use it too.
3. **Host:** straight on Trey's main box for now (16 cores, 125 GB RAM, local NVMe), where the research repo's backtests already run. Queries run inside the service, so it needs real cores.
4. **Live data** (IBKR quotes for live trading) comes later. Results keep the same shapes, so adding live data doesn't change callers.
5. **First sources:** ThetaData (intraday included), Alpaca, Alphanume, the trading calendar, and event dates (earnings, economic releases). NT8 and Databento after.

## Shape

```
clients (research scripts via md.py, other clients)
   │  HTTP only: typed calls, read-only SQL, jobs, status.  Results as Arrow (zstd when large)
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
| Option end of day | ThetaData `option/history/greeks/eod`, `open_interest` | (root, expiration, trade date) | |
| Contract and expiration lists | ThetaData `option/list/*` | (root or all, trade date) | Small. Lists for past dates are kept; lists for today refetch. |
| Stock bars | Alpaca SIP, minute and daily | (symbol, interval, month) | 2016-01-04 on, 04:00–20:00. Raw prices stored; split and dividend adjustment applied when read. Zero-volume bars Alpaca fills in after a move to OTC are dropped. |
| Stock end of day | ThetaData `stock/history/eod` (Free) | (symbol, year) | Free only within a rolling window (about 2023-06 on). The research repo's saved backfill can't be fetched again: it's imported first and marked never-delete. |
| Index levels | Built from option `underlying_price` | (root, trade date, interval) | Index endpoints return 403 on Trey's tier. SPX at 1 s comes from one near-the-money SPXW contract's Greeks. A `source` column says which contract it came from. |
| Alphanume datasets | Alphanume API | (dataset, query) | Stored as returned. Recent rows keep changing (outcomes, lifecycle columns), so rows less than a set age refetch. |
| Events | To be chosen (see [Events](#events)) | (kind, date range) | Earnings dates with time of day, economic releases, FOMC. |
| Trading calendar | Built in | — | Sessions, half days, holidays, settlement time per root, ThetaData's 17:15 ET end-of-day cutoff. Used by the service and served to clients. |

**Fetch units, not queries.** A request is answered from the store when every unit it touches is covered. Otherwise the missing units are fetched and stored, then the answer is read from the store, so a client sees the same data whether or not a fetch happened. A request for one minute of a chain fetches the whole session at that interval, because the next script will want another minute.

**Coverage.** A table in the catalog database records each unit fetched, when, and its row count. Empty answers count, so weekends, holidays and delisted names are never fetched twice. To refetch something a source has corrected, delete its coverage row. There's no version history.

**Freshness.**
- **Today isn't stored.** Requests for today, or for any date without a finished end-of-day report, pass through uncached.
- **Recent Alphanume rows** refetch once older than a set age.
- **Kept forever:** past dates of everything else.
- **Never deleted:** the ThetaData stock EOD that has aged out of the free window.

**Store layout.** One DuckLake table per kind above, partitioned by year and month of the trade date, sorted inside each file by root, expiration, strike, right and time. New units land as small files. A merge rewrites each touched month into one file, and files are deleted only after a grace period longer than the longest query, so a running query never loses one.

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
  - A new login invalidates the previous one on the same account and environment. So the service makes one client and shares it across its threads, and nothing else logs in to production while it runs. Trey's terminal is on the staging environment and isn't disturbed. A ThetaDataDx login to staging on 2026-11-21 didn't disturb the terminal either.
  - Over the limit fails at once with `RESOURCE_EXHAUSTED`; nothing queues on ThetaData's side. The service holds itself to 4 and treats `RESOURCE_EXHAUSTED` as "retry shortly", not as an outage.
  - Date ranges are split into ThetaData's 365-day maximum. `symbol=*` isn't allowed, and `expiration=*` goes one day per call.
- **Two priorities.** Interactive requests (a client waiting) and bulk jobs. Bulk uses at most 3 of the 4 ThetaData slots, so a 25-hour job never locks out a quick lookup.
- **Outages pause the queue.** When requests start failing together (timeouts, 5xx, connection errors), the queue stops, checks with one cheap request every minute or two, and resumes. Failed units go back on the queue, not into a failure list. On 2026-10-21 a client backfill without this logged 445 failures in one second while the terminal was down.
- **Alpaca and Alphanume** each get their own queue under their own rate limits. Keys come from the service's environment file, never from requests.
  - **Alpaca free tier: 200 API calls a minute** (Trey, 2026-11-22). The service paces itself under it, with interactive requests first, as with ThetaData. Every page of a paginated response counts as a call. What Alpaca returns over the limit isn't known until an Alpaca spike (see [Open questions](#open-questions)).
- **Billed sources** (Databento, when added) answer cost quotes freely. A billed pull refuses to run unless the request carries a dollar cap Trey approved.

## Interface

1. **Typed calls** for what the scripts do most:
   - `GET /chain`: root, date, time or time range, optional expirations and strikes
   - `GET /quotes`: one contract, a range
   - `GET /bars`: symbol, interval, range, adjustment. Picks the source by rule (Alpaca for stocks, the option-derived series for indexes) and returns a `source` column. Any call can force a source.
   - `GET /events`: kind, date range, symbols
   - `GET /calendar`: sessions in a range
2. **Read-only SQL:** `POST /sql` runs DuckDB SQL against the store and returns only the result. A backtest sends its query, not a request for raw rows: "average 0DTE spread by minute over 1,100 days" comes back as 265 rows. The service first fills any units the query's date range needs, then runs it.
3. **Theta-compatible paths (not settled, see [Open questions](#open-questions)):** `GET /v3/<endpoint>?<same params>` returning the terminal's CSV columns, so existing clients and old scripts would only change their base URL.
4. **Jobs:** `POST /jobs` with a list of requests to prefetch, `GET /jobs/<id>` for progress. A job runs on the server, so it survives a client disconnecting. The main backtest universe fetch is one job.
5. **Status:** `GET /status` shows queue depth per source, ThetaData slots in use, store and catalog health, and last merge.

**Results** are Arrow streams, zstd-compressed above a size threshold. Polars and pandas load them without converting. Errors are JSON.

**`md.py`**, a thin Python client, is the only client library: `md.chain("SPXW", "2024-03-14", "12:00")`, `md.sql("...")`, `md.bars("AAPL", "1m", start, end)`, each returning a Polars frame. Scripts don't know what's behind the API, so the store can change without touching them.

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
- The HTTP and Arrow API cost nothing measurable on lookups and sweeps, and with 8 clients was faster than 8 direct readers (387 against 264 lookups a second). Raw pulls of millions of rows cost more (6.7M rows: 124 ms direct, 392 ms through the API with zstd), which is why backtests should send their SQL.
- **The trade-off:** single lookups are slower than TimescaleDB's, because each query first asks the catalog for its files. If that ever matters, the service can keep the live file list in memory and read the right file directly (about 3 ms in the test).
- **Two traps to avoid in the build:**
  - Send each HTTP response's headers and body in one write, or kept-alive connections wait about 40 ms on the delayed-ACK timer.
  - Pin the DuckLake and DuckDB versions. The test ran catalog format 1.1 on DuckDB 2.0.1; it's young, so upgrade deliberately.

## Moving existing data in

- **The research repo's ThetaData cache** (6.8 GB, 77,889 files):
  - Import whole-session files as units.
  - Partial windows (single-minute snapshots, half sessions) don't make a full unit and aren't imported; they refetch on demand.
  - **The stock EOD backfill is imported first, counted against the files, and marked never-delete** before anything in the research repo's cache is touched. Those files stay until Trey says otherwise.
- **The research repo's Alphanume downloads:** imported as stored queries.
- **Research scripts keep their own caches until moved.** Finished backtests aren't rewritten. New work uses `md.py`. When the service is live, the research repo's ThetaData rule changes to "all data through `md.py`".
- **Other clients' stores:** left alone for now.

## Deployment

- **Service:** Python, under systemd as its own user on the main box. Listens on the LAN only.
- **Data:** Parquet under a data directory on local NVMe (git-ignored). Not on network storage, which slows every sweep.
- **Catalog:** its own Postgres instance on the same box (Trey, 2026-11-21), so each query's catalog round trip stays local.
- **Secrets:** `THETADATA_API_KEY`, the Alpaca and Alphanume keys, and event-source keys go in an environment file readable only by the service user. Never in the repo, never in logs.
- **Upkeep:** merges, snapshot expiry and file cleanup run on a schedule in the service.

## Later

- Live IBKR quotes and bars for live trading, in the same result shapes.
- NT8 futures exports (manual, from Windows) and Databento CME bars behind the cost-cap gate.
- Breadth data, if a test needs it.

## Deliberately left out

- Evidence hashing, manifests, request fingerprints, provenance graphs and version history.
- User accounts and anything public.
- Writes from clients.

## Open questions

- **Theta-compatible `/v3` paths:** whether the service offers them at all (Trey, 2026-11-22: not settled).
- **Alpaca spike:** what it returns over the 200 calls/min limit, its page size for bars, and whether a multi-symbol request saves calls. Not probed yet.
- **The 09:30 row:** staging gives zero quotes at 09:30:00 and ThetaDataDx gives a 0 underlying price there. Work out what ThetaData actually returns at the open, across days and environments, before any first-print rule (Trey, 2026-11-22).
- **Backups:** of the catalog, and of the never-delete stock EOD at least. Everything else can be refetched.
- **Merge schedule and grace period**, and why merged months came out about 1.6 times bigger than bulk-loaded ones in the benchmark (56 MB against 33–37 MB).
