# Storage benchmarks (2026-11-21)

Scripts behind "Why DuckLake" in the top README. Data: 2,218 days of SPXW one-minute whole-chain quotes from an existing ThetaData cache (267M rows). The queries ask for 12:00; the timings in the top README were taken at another minute of the session, which doesn't change what a query reads. Run from a scratch folder holding the generated `p1/`, `p2/` and `lake/` data, with TimescaleDB in Docker on port 55432 (`timescale/timescaledb:latest-pg17`).

- `load.py`: the CSV files into Parquet (one file per unit, `p1/`; one per month, `p2/`) and TimescaleDB.
- `bench.py`: single-client query timings. `conc.py`: 1–32 concurrent readers.
- `rw.py`: reads while writing (plain Parquet, DuckLake with SQLite or Postgres catalog, TimescaleDB), checking every answer.
- `server.py`, `apibench.py`, `conc_api.py`: the HTTP and Arrow API against direct DuckDB.
- `session_labels.py`: `trade_date` and `session` stored on every row against joined from a sessions table at read time (2026-11-22).
