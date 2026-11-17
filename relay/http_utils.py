"""Small HTTP/ASGI helpers shared by the plain proxy and verified downloads."""

from __future__ import annotations

import asyncio
from typing import Iterable

from starlette.types import Receive

from .generations import HOP_BY_HOP

JSON_HEADERS = {"cache-control": "no-store", "x-content-type-options": "nosniff"}
# Response headers that describe the upstream hop, not the payload.  ``date``
# and ``server`` are dropped as well: uvicorn emits its own, and duplicates
# would make the relay's own identity ambiguous.
DROP_RESPONSE_HEADERS = HOP_BY_HOP | {
    "connection",
    "keep-alive",
    "content-length",
    "transfer-encoding",
    "trailer",
    "upgrade",
    "date",
    "server",
}
# Request headers that must not influence a verified download: ranges and
# conditionals could turn the upstream response into a partial/304, and an
# encoding negotiation could make the digest apply to compressed bytes.
VERIFIED_DROP_REQUEST_HEADERS = HOP_BY_HOP | {
    "host",
    "content-length",
    "accept-encoding",
    "range",
    "if-range",
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
    "expect",
    "trailer",
    "te",
    "upgrade",
}


def header_pairs(raw_headers: Iterable[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    return [(k.decode("latin-1"), v.decode("latin-1")) for k, v in raw_headers]


def connection_tokens(headers: list[tuple[str, str]]) -> set[str]:
    tokens: set[str] = set()
    for name, value in headers:
        if name.lower() == "connection":
            tokens.update(part.strip().lower() for part in value.split(",") if part.strip())
    return tokens


def strip_hop_by_hop(headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    drop = set(HOP_BY_HOP) | connection_tokens(headers)
    return [(k, v) for k, v in headers if k.lower() not in drop]


async def watch_disconnect(receive: Receive, event: asyncio.Event) -> None:
    """Set ``event`` when the client disconnects; runs for the response lifetime.

    Uvicorn delivers ``http.disconnect`` only through ``receive()``; there is no
    other signal for an aborted download, so this task is what frees the
    upstream connection (and staged file) when the client goes away.
    """
    try:
        while not event.is_set():
            message = await receive()
            if message["type"] == "http.disconnect":
                event.set()
                return
    except asyncio.CancelledError:
        raise
    except Exception:  # pragma: no cover - defensive
        event.set()
