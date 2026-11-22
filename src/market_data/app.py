"""The HTTP API. Phase 0: `GET /status` only."""
from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from importlib.metadata import version

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .config import Settings
from .store import Store


def status(request: Request) -> JSONResponse:
    state = request.app.state
    store = state.store.health()
    body = {
        "service": "market-data",
        "version": version("market-data"),
        "started": state.started.isoformat(),
        "store": store,
    }
    return JSONResponse(body, status_code=200 if store["ok"] else 503)


def create_app(settings: Settings) -> Starlette:
    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette):
        store = Store(settings)
        store.attach()
        app.state.store = store
        app.state.started = datetime.now(timezone.utc)
        try:
            yield
        finally:
            store.close()

    return Starlette(routes=[Route("/status", status)], lifespan=lifespan)
