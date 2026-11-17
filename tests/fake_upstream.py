"""Controllable upstream used by the integration tests."""

from __future__ import annotations

import asyncio
import hashlib
import time

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

PATTERN = b"0123456789abcdef"
TILE = 4096


def _tile(seed: str, index: int) -> bytes:
    material = f"{seed}:{index}:".encode()
    return (material * (TILE // len(material) + 1))[:TILE]


def artifact_bytes(size: int, seed: str = "", chunk: int = 4096) -> bytes:
    """Deterministic artifact body shared by the upstream and the tests.

    Content depends only on ``(seed, byte offset)`` (``chunk`` is kept for
    call-site compatibility), so the same digest works regardless of how the
    upstream or caller segments the stream.
    """
    out = bytearray()
    tile = 0
    remaining = size
    while remaining > 0:
        take = min(TILE, remaining)
        out += _tile(seed, tile)[:take]
        remaining -= take
        tile += 1
    return bytes(out)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _artifact(request):
    params = request.query_params
    size = int(params.get("bytes", 1024))
    chunk = max(1, int(params.get("chunk", 64)))
    delay = float(params.get("delay", 0))
    header_delay = float(params.get("header_delay", 0))
    stats = request.app.state.stats
    stats["started"] += 1
    state = {"finished": False}

    if header_delay:
        await asyncio.sleep(header_delay)

    async def gen():
        try:
            sent = 0
            while sent < size:
                n = min(chunk, size - sent)
                block = (PATTERN * (n // len(PATTERN) + 1))[:n]
                yield block
                sent += n
                if delay:
                    await asyncio.sleep(delay)
            state["finished"] = True
            stats["completed"] += 1
        except asyncio.CancelledError:
            stats["cancelled"] += 1
            raise
        finally:
            if not state["finished"]:
                stats["aborted"] += 1

    return StreamingResponse(gen(), media_type="application/octet-stream")


async def _verified(request):
    """Deterministic artifact whose bytes do not depend on the upstream name."""
    params = request.query_params
    size = max(0, int(params.get("bytes", 1024)))
    chunk = max(1, int(params.get("chunk", 256)))
    seed = params.get("seed", "test")
    delay = float(params.get("delay", 0))
    header_delay = float(params.get("header_delay", 0))
    declared = params.get("declared")
    stats = request.app.state.stats
    stats["started"] += 1
    app_state = request.app.state
    app_state.last_verified_headers = dict(request.headers)
    app_state.verified_started = time.monotonic()
    app_state.verified_completed = None

    body = artifact_bytes(size, seed=seed, chunk=chunk)

    if header_delay:
        await asyncio.sleep(header_delay)

    async def gen():
        try:
            sent = 0
            while sent < len(body):
                yield body[sent:sent + chunk]
                sent += chunk
                app_state.verified_sent = sent
                if delay:
                    await asyncio.sleep(delay)
            app_state.verified_completed = time.monotonic()
            stats["completed"] += 1
        finally:
            if app_state.verified_completed is None:
                stats["aborted"] += 1

    headers = {"x-upstream-name": request.app.state.name}
    if declared is not None:
        headers["content-length"] = declared
    return StreamingResponse(gen(), media_type="application/octet-stream", headers=headers)


async def _truncated(request):
    """Stream fewer bytes than declared, then break the connection."""
    params = request.query_params
    size = int(params.get("bytes", 100000))
    cut = int(params.get("cut", 1000))
    chunk = max(1, int(params.get("chunk", 256)))
    request.app.state.verified_sent = 0

    async def gen():
        sent = 0
        while sent < min(cut, size):
            n = min(chunk, cut - sent)
            yield PATTERN * (n // len(PATTERN) + 1)
            sent += n
        raise ConnectionResetError("simulated truncation")

    headers = {"content-length": str(size)}
    return StreamingResponse(gen(), media_type="application/octet-stream", headers=headers)


async def _status(request):
    code = int(request.path_params.get("code", request.query_params.get("code", 500)))
    return Response(b"upstream-error-body", status_code=code, media_type="text/plain")


async def _fixed(request):
    """Fixed-length response: verifies content-length pass-through."""
    request.app.state.stats["started"] += 1
    body = b"fixed-length-body" * 128
    headers = {"content-length": str(len(body)), "x-upstream-fixed": "1"}
    request.app.state.stats["completed"] += 1
    return Response(body, media_type="application/octet-stream", headers=headers)


async def _healthz(request):
    app = request.app
    app.state.probes += 1
    started = time.monotonic()
    if app.state.probe_delay:
        await asyncio.sleep(app.state.probe_delay)
    status = app.state.probe_status
    return JSONResponse(
        {"status": "ok" if status == 200 else "not_ready"},
        status_code=status,
        headers={"x-probe-delay": f"{time.monotonic() - started:.3f}"},
    )


async def _control(request):
    app = request.app
    if "delay" in request.query_params:
        app.state.probe_delay = float(request.query_params["delay"])
    if "status" in request.query_params:
        app.state.probe_status = int(request.query_params["status"])
    return JSONResponse({"probe_delay": app.state.probe_delay, "probe_status": app.state.probe_status})


async def _state(request):
    app = request.app
    return JSONResponse(
        {
            "name": app.state.name,
            "probe_delay": app.state.probe_delay,
            "probe_status": app.state.probe_status,
            "stats": app.state.stats,
            "probes": app.state.probes,
        }
    )


def make_app(name: str) -> Starlette:
    app = Starlette(
        routes=[
            Route("/ready", _healthz),
            Route("/healthz", _healthz),
            Route("/artifact", _artifact),
            Route("/verified", _verified),
            Route("/truncated", _truncated),
            Route("/status/{code:int}", _status),
            Route("/fixed", _fixed),
            Route("/_control/probe", _control, methods=["POST", "GET"]),
            Route("/_state", _state),
        ]
    )
    app.state.name = name
    app.state.probe_delay = 0.0
    app.state.probe_status = 200
    app.state.stats = {"started": 0, "completed": 0, "cancelled": 0, "aborted": 0}
    app.state.probes = 0
    app.state.verified_sent = 0
    app.state.verified_completed = None
    app.state.last_verified_headers = {}
    return app
