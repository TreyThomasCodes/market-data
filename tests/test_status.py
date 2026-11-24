"""Against the real catalog instance, in a throwaway database.

Skipped only when no service settings exist at all; a broken .env fails the tests instead.
"""
import dataclasses
import logging
import threading
import time

import pytest
from starlette.testclient import TestClient

from market_data.app import create_app as _create_app
from market_data.config import Secret
from market_data.store import Store, StoreError

from conftest import drop_catalog_db as _drop, fake_fetch


def create_app(settings):
    return _create_app(settings, calendar_fetch=fake_fetch())


def test_status_answers(settings, caplog):
    with caplog.at_level(logging.DEBUG), TestClient(create_app(settings)) as client:
        r = client.get("/status")
    assert r.status_code == 200
    body = r.json()
    assert body["service"] == "market-data"
    store = body["store"]
    assert store["ok"] and store["catalog_format"] == "1.1"
    assert store["latest_snapshot"] == 18  # creation, 2 calendar tables, 16 calendar years (2012-2027)
    assert body["calendar"]["ok"] and body["calendar"]["first"] == "2012-01-01"
    assert store["data_path"] == str(settings.data_dir / "lake")
    pw = settings.catalog_password.reveal()
    assert pw not in r.text and pw not in caplog.text


def test_store_session_is_utc(settings):
    store = Store(settings)
    store.attach()
    try:
        with store.cursor() as cur:
            assert cur.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
    finally:
        store.close()


def test_reattach_keeps_catalog(settings):
    for _ in range(2):
        store = Store(settings)
        store.attach()
        assert store.health()["ok"]
        store.close()


def test_status_503_when_catalog_unhealthy(settings):
    with TestClient(create_app(settings)) as client:
        client.app.state.store.close()
        r = client.get("/status")
    assert r.status_code == 503 and r.json()["store"] == {"ok": False, "error": "StoreError"}


def test_lost_catalog_doesnt_leak_the_password(settings, caplog):
    """The catalog goes away under a running service: /status says only the error type."""
    pw = settings.catalog_password.reveal()
    with caplog.at_level(logging.DEBUG), TestClient(create_app(settings)) as client:
        assert client.get("/status").status_code == 200
        _drop(settings)
        r = client.get("/status")
    assert r.status_code == 503
    assert set(r.json()["store"]) == {"ok", "error"}
    assert pw not in r.text and pw not in caplog.text
    assert "store health check failed" in caplog.text


def test_error_text_with_the_password_is_scrubbed(settings, caplog, monkeypatch):
    """Whatever an error says, the password doesn't reach the response or the log."""
    pw = settings.catalog_password.reveal()
    with caplog.at_level(logging.DEBUG), TestClient(create_app(settings)) as client:
        def boom():
            raise RuntimeError(f"Unable to connect to Postgres at \"host=x password={pw}\"")
        monkeypatch.setattr(client.app.state.store, "cursor", boom)
        r = client.get("/status")
    assert r.status_code == 503 and r.json()["store"]["error"] == "RuntimeError"
    assert pw not in r.text and pw not in caplog.text and "password=<redacted>" in caplog.text


def test_wrong_password_fails_without_echoing_it(settings):
    bad = dataclasses.replace(settings, catalog_password=Secret("not-the-password-XYZ"))
    with pytest.raises(StoreError) as e:
        Store(bad).attach()
    assert "not-the-password-XYZ" not in str(e.value)
    assert e.value.__cause__ is None and e.value.__suppress_context__


def test_cursor_before_attach(settings):
    with pytest.raises(StoreError):
        with Store(settings).cursor():
            pass


def test_close_waits_for_cursors_in_use(settings):
    store = Store(settings)
    store.attach()
    held, closed = threading.Event(), threading.Event()

    def reader():
        with store.cursor() as cur:
            held.set()
            time.sleep(0.3)
            assert cur.execute("SELECT 1").fetchone() == (1,)  # connection still open

    t = threading.Thread(target=reader)
    t.start()
    held.wait()
    threading.Thread(target=lambda: (store.close(), closed.set())).start()
    assert not closed.wait(0.1)  # close() is waiting for the reader
    t.join()
    assert closed.wait(2)
    with pytest.raises(StoreError):
        with store.cursor():
            pass
