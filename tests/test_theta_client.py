"""The shared ThetaData client: slots, error handling, outage pause. No login: a fake ThetaDataDx client stands in,
raising ThetaDataDx's real exception classes."""
import threading
import time as clock

import pytest
import thetadatadx as tdx

from market_data import theta_client as tc
from market_data.theta_client import Priority, Slots, ThetaClient, ThetaUnavailable


class FakeMD:
    def __init__(self):
        self.script = []          # exceptions (or values) to return in order, then "ok"
        self.calls = []
        self.lock = threading.Lock()

    def __getattr__(self, name):
        def call(*args, **kw):
            with self.lock:
                self.calls.append(name)
                step = self.script.pop(0) if self.script else "ok"
            if isinstance(step, BaseException):
                raise step
            return step
        return call


class FakeClient:
    def __init__(self, md):
        self.market_data = md
        self.closed = False

    def close(self):
        self.closed = True


def client(md, logins=None, **kw):
    def connect():
        if logins is not None:
            logins.append(1)
        return FakeClient(md)
    return ThetaClient(connect, errors=tdx, sleep=lambda s: None, **kw)


def test_one_login_shared_by_every_call():
    md, logins = FakeMD(), []
    c = client(md, logins)
    for _ in range(5):
        assert c.call("calendar_year", "2026") == "ok"
    assert logins == [1]


def test_not_found_is_an_empty_answer():
    md = FakeMD()
    md.script = [tdx.NotFoundError("no data")]
    assert client(md).call("option_history_quote", "SPXW", "20240314") is None


def test_rate_limit_retries_without_counting_as_outage():
    md = FakeMD()
    md.script = [tdx.RateLimitError("busy")] * 5
    c = client(md)
    assert c.call("x") == "ok"
    assert len(md.calls) == 6 and c.paused_since is None


@pytest.mark.parametrize("err", [tdx.SubscriptionError, tdx.InvalidParameterError, tdx.SchemaMismatchError,
                                 tdx.AuthenticationError])
def test_other_errors_are_raised(err):
    md = FakeMD()
    md.script = [err("nope")]
    with pytest.raises(err):
        client(md).call("x")
    assert md.calls == ["x"]


def test_an_odd_failure_is_retried_without_pausing():
    md = FakeMD()
    md.script = [tdx.UnavailableError("blip")]
    c = client(md)
    assert c.call("x") == "ok"
    assert c.paused_since is None and md.calls == ["x", "x"]


def test_failures_together_pause_everything_until_a_probe_answers(monkeypatch):
    monkeypatch.setattr(tc, "PROBE_EVERY", 0)
    md = FakeMD()
    gate = threading.Event()
    probes = []

    def slow_sleep(s):
        if s == 0:  # the probe loop's wait: hold it until the test has seen the pause
            gate.wait(5)
            probes.append(1)

    md.script = [tdx.NetworkError("down"), tdx.UnavailableError("down"), tdx.DeadlineExceededError("down"),
                 tdx.UnavailableError("probe fails")]
    c = ThetaClient(lambda: FakeClient(md), errors=tdx, sleep=slow_sleep)
    out = []
    t = threading.Thread(target=lambda: out.append(c.call("x")))
    t.start()
    deadline = clock.time() + 5
    while c.paused_since is None and clock.time() < deadline:
        clock.sleep(0.01)
    assert c.paused_since is not None, "three failures in a row should pause"
    gate.set()   # first probe fails ("probe fails"), the second answers, then the request runs
    t.join(5)
    assert out == ["ok"] and c.paused_since is None
    assert md.calls == ["x", "x", "x", "calendar_year", "calendar_year", "x"]
    assert len(probes) == 2


def test_closed_client_refuses():
    c = client(FakeMD())
    c.call("x")
    c.close()
    with pytest.raises(ThetaUnavailable):
        c.call("x")
    with pytest.raises(ThetaUnavailable):
        ThetaClient(None, errors=tdx).call("x")


def test_slots_cap_and_bulk_share():
    s = Slots(total=4, bulk=3)
    held, release = [], threading.Event()

    def hold(p):
        with s.take(p):
            held.append(p)
            release.wait(5)

    bulk = [threading.Thread(target=hold, args=(Priority.BULK,)) for _ in range(4)]
    for t in bulk:
        t.start()
    clock.sleep(0.2)
    assert s.in_use == 3 and s.bulk_in_use == 3      # the fourth bulk waits: one slot stays free
    inter = threading.Thread(target=hold, args=(Priority.INTERACTIVE,))
    inter.start()
    clock.sleep(0.2)
    assert s.in_use == 4 and held.count(Priority.INTERACTIVE) == 1
    release.set()
    for t in bulk + [inter]:
        t.join(5)
    assert s.in_use == 0 and held.count(Priority.BULK) == 4


def test_waiting_interactive_goes_before_bulk():
    s = Slots(total=1, bulk=1)
    order, first_in = [], threading.Event()
    go = threading.Event()

    def take(p, tag):
        with s.take(p):
            order.append(tag)
            if tag == "first":
                first_in.set()
                go.wait(5)

    t0 = threading.Thread(target=take, args=(Priority.INTERACTIVE, "first"))
    t0.start()
    first_in.wait(5)
    tb = threading.Thread(target=take, args=(Priority.BULK, "bulk"))
    tb.start()
    clock.sleep(0.1)
    ti = threading.Thread(target=take, args=(Priority.INTERACTIVE, "interactive"))
    ti.start()
    clock.sleep(0.1)
    go.set()
    for t in (t0, tb, ti):
        t.join(5)
    assert order == ["first", "interactive", "bulk"]


def test_calendar_refresh_uses_the_shared_client():
    from types import SimpleNamespace
    from market_data.thetadata import fetch_calendar_years

    class CalMD(FakeMD):
        def calendar_year(self, year, timeout_ms=None):
            self.calls.append(("calendar_year", year))
            if year == "2028":
                raise tdx.NotFoundError("not published")
            return [SimpleNamespace(date=int(f"{year}0703"), status="early_close", open_time=34_200_000,
                                    close_time=46_800_000)]

    md, logins = CalMD(), []
    c = client(md, logins)
    c.call("option_history_quote")                  # the service's session, already logged in
    got = fetch_calendar_years(c, [2027, 2028])
    assert logins == [1]                            # no second login that would replace it
    assert got[2028] is None and got[2027][0].status == "early_close"
