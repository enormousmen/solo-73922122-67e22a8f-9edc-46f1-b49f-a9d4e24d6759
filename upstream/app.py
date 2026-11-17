"""Demo artifact upstream with controllable readiness latency and slow downloads.

Run two copies (blue/green).  Every response identifies itself via
``X-Upstream-Name`` so the relay demo makes routing visible.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
PROBE_DEFAULT_DELAY = float(os.environ.get("UPSTREAM_PROBE_DELAY", "0"))
PROBE_DEFAULT_STATUS = int(os.environ.get("UPSTREAM_PROBE_STATUS", "200"))
MAX_PROBE_DELAY = 120.0
VERIFIED_MAX_BYTES = 64 * 1024 * 1024

app = FastAPI(title=f"Artifact upstream {NAME}")
state = {"probe_delay": PROBE_DEFAULT_DELAY, "probe_status": PROBE_DEFAULT_STATUS}


@app.get("/ready")
async def ready():
    """Readiness endpoint (the relay's default probe path)."""
    return await _readiness()


@app.get("/healthz")
async def healthz():
    """Alias kept for probes/healthchecks that expect /healthz."""
    return await _readiness()


async def _readiness():
    """Readiness endpoint. A long delay mimics a slow control plane."""
    delay = state["probe_delay"]
    started = time.monotonic()
    if delay:
        await asyncio.sleep(delay)
    status = state["probe_status"]
    return JSONResponse(
        {"upstream": NAME, "status": "ok" if status == 200 else "not_ready"},
        status_code=status,
        headers={"x-upstream": NAME, "x-probe-delay": str(round(time.monotonic() - started, 3))},
    )


@app.post("/_control/probe")
async def control_probe(delay: float = 0.0, status: int = 200):
    """Set readiness latency/status, e.g. POST /_control/probe?delay=4&status=503."""
    if delay < 0 or delay > MAX_PROBE_DELAY:
        return JSONResponse({"error": "delay out of range"}, status_code=400)
    state["probe_delay"] = delay
    state["probe_status"] = status
    return {"upstream": NAME, "probe_delay": delay, "probe_status": status}


@app.get("/_control/probe")
async def control_probe_get():
    return {"upstream": NAME, **state}


@app.get("/artifact")
async def artifact(
    bytes: int = 1024 * 1024,
    chunk: int = 65536,
    delay: float = 0.0,
):
    """Stream ``bytes`` bytes with ``delay`` seconds between chunks."""
    bytes = max(0, min(bytes, 512 * 1024 * 1024))
    chunk = max(1, min(chunk, 1024 * 1024))
    delay = max(0.0, min(delay, 10.0))

    def generate() -> AsyncIterator[bytes]:
        async def gen() -> AsyncIterator[bytes]:
            remaining = bytes
            seq = 0
            while remaining > 0:
                size = min(chunk, remaining)
                seed = f"{NAME}:{seq}:".encode()
                block = (seed * (size // len(seed) + 1))[:size]
                yield block
                remaining -= size
                seq += 1
                if delay:
                    await asyncio.sleep(delay)

        return gen()

    headers = {
        "x-upstream": NAME,
        "x-artifact-bytes": str(bytes),
        "x-artifact-delay": str(delay),
        # No Content-Length: exercises chunk-by-chunk relaying.
    }
    return StreamingResponse(generate(), media_type="application/octet-stream", headers=headers)


VERIFIED_TILE = 4096


def _verified_tile(seed: str, tile: int) -> bytes:
    """One fixed tile of the deterministic artifact.

    Content depends only on ``(seed, tile index)``, never on how the caller
    chunks the stream, so the published digest is segmentation-independent.
    """
    material = f"verified:{seed}:{tile}:".encode()
    return (material * (VERIFIED_TILE // len(material) + 1))[:VERIFIED_TILE]


# Deterministic artifact content for the verified-download demo.  The bytes do
# NOT depend on UPSTREAM_NAME: blue and green serve byte-identical artifacts, so
# a digest published for one target verifies on the other after a switch.
def verified_artifact(seed: str, size: int, chunk: int) -> tuple[AsyncIterator[bytes], str]:
    """Return ``(generator, sha256_hex)`` for a deterministic artifact."""
    size = max(0, min(size, VERIFIED_MAX_BYTES))
    chunk = max(1, min(chunk, 1024 * 1024))
    digest = hashlib.sha256()
    full, tail = divmod(size, VERIFIED_TILE)
    for tile in range(full):
        digest.update(_verified_tile(seed, tile))
    if tail:
        digest.update(_verified_tile(seed, full)[:tail])

    async def gen() -> AsyncIterator[bytes]:
        remaining = size
        offset = 0
        while remaining > 0:
            n = min(chunk, remaining)
            start_tile, start_off = divmod(offset, VERIFIED_TILE)
            out = bytearray()
            tile = start_tile
            off = start_off
            while len(out) < n:
                take = min(VERIFIED_TILE - off, n - len(out))
                out += _verified_tile(seed, tile)[off:off + take]
                off = 0
                tile += 1
            yield bytes(out)
            remaining -= n
            offset += n

    return gen(), digest.hexdigest()


@app.get("/verified/manifest")
async def verified_manifest(
    seed: str = "default",
    bytes: int = 65536,
    chunk: int = 8192,
):
    """Publish the expected digest for a deterministic artifact (demo helper)."""
    _, digest = verified_artifact(seed, bytes, chunk)
    size = max(0, min(bytes, VERIFIED_MAX_BYTES))
    return {
        "upstream": NAME,
        "path": f"/verified/artifact?seed={seed}&bytes={size}",
        "sha256": digest,
        "bytes": size,
    }


@app.get("/verified/artifact")
async def verified_artifact_route(
    seed: str = "default",
    bytes: int = 65536,
    chunk: int = 8192,
    delay: float = 0.0,
):
    """Deterministic artifact for the digest-verified download API."""
    chunk = max(1, min(chunk, 1024 * 1024))
    delay = max(0.0, min(delay, 10.0))

    body, _ = verified_artifact(seed, bytes, chunk)
    if not delay:
        return StreamingResponse(body, media_type="application/octet-stream")

    async def slow() -> AsyncIterator[bytes]:
        async for block in body:
            yield block
            await asyncio.sleep(delay)

    return StreamingResponse(slow(), media_type="application/octet-stream")


@app.get("/verified/broken")
async def verified_broken(
    bytes: int = 65536,
    chunk: int = 8192,
    cut_after: int = 4096,
):
    """Announce the full length but cut the stream short (truncated transfer)."""
    size = max(1, min(bytes, VERIFIED_MAX_BYTES))
    chunk = max(1, min(chunk, 1024 * 1024))
    cut_after = max(0, min(cut_after, size))

    async def gen() -> AsyncIterator[bytes]:
        sent = 0
        while sent < cut_after:
            n = min(chunk, cut_after - sent)
            yield b"x" * n
            sent += n
        # Malformed final frame: uvicorn aborts the connection rather than
        # ending the chunked body cleanly, so the relay sees a truncated
        # transfer even though it promised full length.
        raise ConnectionResetError("simulated upstream cut")

    headers = {"content-length": str(size), "x-upstream-simulated": "truncated"}
    return StreamingResponse(gen(), media_type="application/octet-stream", headers=headers)


@app.get("/verified/digest")
async def verified_digest(
    seed: str = "default",
    bytes: int = 65536,
    chunk: int = 8192,
) -> PlainTextResponse:
    """Plain-text digest, convenient for shell pipelines."""
    _, digest = verified_artifact(seed, bytes, chunk)
    return PlainTextResponse(digest + "\n")


@app.get("/")
async def index():
    return {"upstream": NAME, "probe_delay": state["probe_delay"], "probe_status": state["probe_status"]}
