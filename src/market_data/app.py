"""The HTTP API: `GET /status`, `GET /calendar`."""
from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone
from importlib.metadata import version

import pyarrow as pa
import pyarrow.ipc as ipc
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .calendar import ET_NAME, Asset, CalendarError
from .calendar_store import CalendarService, Fetch
from .config import Settings
from .fetcher import Fetcher
from .store import Store
from .tables import ensure_tables
from .writer import relabel_pending
from .theta_client import ThetaClient, connect_production
from .thetadata import fetch_calendar_years

ARROW = "application/vnd.apache.arrow.stream"


def error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status)


def arrow_response(table: pa.Table) -> Response:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return Response(sink.getvalue().to_pybytes(), media_type=ARROW)


def status(request: Request) -> JSONResponse:
    state = request.app.state
    store = state.store.health()
    cal = state.calendar.health()
    body = {
        "service": "market-data",
        "version": version("market-data"),
        "started": state.started.isoformat(),
        "store": store,
        "calendar": cal,
        "thetadata": state.theta.health(),
    }
    return JSONResponse(body, status_code=200 if store["ok"] and cal["ok"] else 503)


def _date(q, name: str, default: date | None = None) -> date:
    v = q.get(name)
    if v is None:
        if default is None:
            raise ValueError(f"{name} is required (YYYY-MM-DD)")
        return default
    try:
        return date.fromisoformat(v)
    except ValueError:
        raise ValueError(f"{name} must be a date (YYYY-MM-DD)") from None


def calendar(request: Request) -> Response:
    """Sessions of one product over trade dates start..end (end defaults to start), as Arrow: trade_date, session,
    start, end (time-zone aware, ET). For an option, `expiration` makes its expiry date's sessions those of an
    expiring contract; without it the contract is taken as not expiring in the range."""
    q = request.query_params
    try:
        root = q.get("root")
        if not root:
            raise ValueError("root is required")
        try:
            asset = Asset(q.get("asset", "option"))
        except ValueError:
            raise ValueError("asset must be option or stock") from None
        start = _date(q, "start")
        end = _date(q, "end", start)
        exp = _date(q, "expiration") if "expiration" in q else None
        if end < start:
            raise ValueError("end is before start")
    except ValueError as e:
        return error(400, str(e))
    cal = request.app.state.calendar.calendar
    if cal is None:
        return error(503, "the calendar isn't loaded yet")
    rows = {"trade_date": [], "session": [], "start": [], "end": []}
    try:
        for td in cal.trading_days(start, end):
            for s in cal.sessions(root.upper(), td, exp, asset):
                rows["trade_date"].append(td)
                rows["session"].append(s.name)
                rows["start"].append(s.start)
                rows["end"].append(s.end)
    except CalendarError as e:
        return error(400, str(e))
    ts = pa.timestamp("us", tz=ET_NAME)
    table = pa.table(rows, schema=pa.schema([("trade_date", pa.date32()), ("session", pa.string()),
                                             ("start", ts), ("end", ts)]))
    return arrow_response(table)


_THETADATA = object()


def create_app(settings: Settings, calendar_fetch: Fetch | None | object = _THETADATA,
               theta: ThetaClient | None = None) -> Starlette:
    """One ThetaData client for everything (README: Fetching): `theta` replaces it in tests. `calendar_fetch`
    replaces ThetaData as the calendar's source (tests); by default the calendar comes through the shared client,
    and not at all without a key."""
    if theta is None:
        key = settings.thetadata_api_key
        theta = ThetaClient((lambda: connect_production(key)) if key else None)
    if calendar_fetch is _THETADATA:
        calendar_fetch = (lambda years: fetch_calendar_years(theta, years)) if settings.thetadata_api_key else None

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette):
        store = Store(settings)
        store.attach()
        cal = CalendarService(store, calendar_fetch, on_change=lambda c: relabel_pending(store, c))
        fetcher = Fetcher(store, lambda: cal.calendar, theta)
        try:
            await run_in_threadpool(ensure_tables, store)
            await run_in_threadpool(cal.start)  # may fetch from ThetaData: off the event loop
            app.state.store = store
            app.state.calendar = cal
            app.state.theta = theta
            app.state.fetcher = fetcher
            app.state.started = datetime.now(timezone.utc)
            yield
        finally:
            await run_in_threadpool(cal.stop)
            fetcher.close()
            await run_in_threadpool(theta.close)
            store.close()

    return Starlette(routes=[Route("/status", status), Route("/calendar", calendar)], lifespan=lifespan)
