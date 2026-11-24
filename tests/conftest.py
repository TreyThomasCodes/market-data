import dataclasses
import json
import os
import uuid
from datetime import date, time
from pathlib import Path

import psycopg
import pytest

from market_data.calendar import Day, from_rows
from market_data.config import load_settings

FIXTURES = Path(__file__).parent / "fixtures"


def _time(v):
    return time.fromisoformat(v) if v else None


def calendar_days() -> dict[int, list[Day]]:
    """ThetaData's calendar_year answers for 2012-2027, as fetched 2026-11-22 (tests/fixtures)."""
    raw = json.loads((FIXTURES / "thetadata_calendar.json").read_text())
    return {int(y): [Day(date.fromisoformat(r["date"]), r["status"], _time(r["open"]), _time(r["close"]))
                     for r in rows] for y, rows in raw.items()}


@pytest.fixture(scope="session")
def cal():
    days = calendar_days()
    return from_rows([d for rows in days.values() for d in rows], list(days))


def fake_fetch(calls=None, missing=()):
    """A stand-in for ThetaData's calendar_year: the fixture's years, None for the rest and for `missing`."""
    days = calendar_days()

    def fetch(years):
        if calls is not None:
            calls.append(list(years))
        return {y: (None if y in missing else days.get(y)) for y in years}
    return fetch


# ---------------------------------------------------------------- the catalog instance

def _conninfo(s, dbname):
    return dict(host=s.catalog_host, port=s.catalog_port, user=s.catalog_user,
                password=s.catalog_password.reveal(), dbname=dbname, autocommit=True)


@pytest.fixture
def settings(tmp_path):
    if not (Path(".env").exists() or "MD_ENV_FILE" in os.environ or os.environ.get("MD_CATALOG_PASSWORD")):
        pytest.skip("no service settings (.env, MD_ENV_FILE or MD_CATALOG_PASSWORD)")
    base = load_settings()  # a ConfigError here fails the test
    db = f"md_test_{uuid.uuid4().hex[:12]}"
    try:
        with psycopg.connect(**_conninfo(base, base.catalog_db)) as c:
            c.execute(f"CREATE DATABASE {db}")
    except psycopg.OperationalError as e:
        pytest.skip(f"catalog not reachable: {e}")
    yield dataclasses.replace(base, catalog_db=db, data_dir=tmp_path)
    with psycopg.connect(**_conninfo(base, base.catalog_db)) as c:
        c.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")


def drop_catalog_db(settings):
    with psycopg.connect(**_conninfo(settings, "postgres")) as c:
        c.execute(f"DROP DATABASE {settings.catalog_db} WITH (FORCE)")
