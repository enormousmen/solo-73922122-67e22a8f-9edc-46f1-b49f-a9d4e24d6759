"""Digest-verified artifact downloads.

Unlike the plain proxy, this endpoint never streams unverified bytes to the
client.  It stages the upstream body in a temporary file while hashing it,
compares the SHA-256 against the expected digest, and only then emits response
headers and the file.  The whole operation (staging *and* delivery) is one
in-flight request on the generation that was current at accept time, so the
existing switch/drain rules govern it exactly like a plain download.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Any, BinaryIO, TYPE_CHECKING

import aiohttp
from fastapi import HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from starlette.types import Receive, Scope, Send

from .generations import Generation, InFlightRequest
from .http_utils import (
    DROP_RESPONSE_HEADERS,
    JSON_HEADERS,
    VERIFIED_DROP_REQUEST_HEADERS,
    header_pairs,
    strip_hop_by_hop,
    watch_disconnect,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .proxy import RelayApp

log = logging.getLogger("relay.verified")

HEX_DIGITS = frozenset("0123456789abcdef")


class VerifiedDownloadBody(BaseModel):
    path: str = Field(
        ...,
        min_length=1,
        description="Upstream-relative artifact path, optionally including a query string",
        examples=["/verified/hello.bin?bytes=65536"],
    )
    sha256: str = Field(
        ...,
        min_length=1,
        description="Expected SHA-256 of the complete artifact (64 hex chars; 'sha256:' accepted)",
        examples=["sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
    )


@dataclass
class _Staged:
    size: int
    digest: str
    headers: list[tuple[str, str]]


@dataclass
class _Failure:
    status: int
    error: str
    detail: str
    extra: dict[str, Any]


class VerifiedDownload:
    """Route handlers for ``GET|POST /api/verified-download``."""

    ROUTES = ("/api/verified-download", "/api/download")

    def __init__(self, relay: "RelayApp") -> None:
        self.relay = relay
        self.state = relay.state
        self.config = relay.config

    def register_routes(self, app) -> None:
        # Registered before the GET catch-all, so ordinary upstream paths are
        # unaffected.  Only the canonical path appears in /docs; the alias is
        # a convenience, not a second API.
        for index, route in enumerate(self.ROUTES):
            in_schema = index == 0
            app.get(route, tags=["verified download"], include_in_schema=in_schema)(self.api_get)
            app.post(route, tags=["verified download"], include_in_schema=in_schema)(self.api_post)
            # Shadow the catch-all for HEAD too: a HEAD must not silently
            # proxy the API path upstream.
            app.head(route, include_in_schema=False)(self.api_head)

    async def api_head(self) -> Response:
        return JSONResponse(
            {"error": "method_not_allowed", "detail": "use GET or POST"},
            status_code=405,
            headers={**JSON_HEADERS, "allow": "GET, POST"},
        )

    # -- endpoints ----------------------------------------------------------
    async def api_get(
        self,
        request: Request,
        path: str = Query(
            ...,
            description="Upstream-relative path, e.g. /artifact?bytes=1024",
        ),
        sha256: str = Query(
            ...,
            description="Expected SHA-256 (64 hex chars; 'sha256:' prefix accepted)",
        ),
    ) -> Response:
        return await self._handle(request, path, sha256)

    async def api_post(self, request: Request, body: VerifiedDownloadBody) -> Response:
        return await self._handle(request, body.path, body.sha256)

    # -- request validation -------------------------------------------------
    @staticmethod
    def _normalize_path(raw: str | None) -> str:
        if raw is None:
            raise HTTPException(status_code=400, detail="missing required 'path'")
        value = raw.strip()
        if not value:
            raise HTTPException(status_code=400, detail="'path' must not be empty")
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
            raise HTTPException(status_code=400, detail="'path' contains control characters")
        path_part = value.split("?", 1)[0]
        # Only an absolute URL as the *path* is rejected; a query string may
        # legitimately contain "://" (e.g. ?redirect=http://...).
        if path_part.startswith("//") or "://" in path_part or path_part.startswith("\\\\"):
            raise HTTPException(
                status_code=400,
                detail="'path' must be relative to the upstream target, not an absolute URL",
            )
        if not value.startswith("/"):
            value = "/" + value
        return value

    @staticmethod
    def _normalize_digest(raw: str | None) -> str:
        if raw is None:
            raise HTTPException(status_code=400, detail="missing required 'sha256'")
        value = raw.strip().lower()
        for prefix in ("sha256:", "sha256="):
            if value.startswith(prefix):
                value = value[len(prefix):].strip()
                break
        if len(value) != 64 or any(ch not in HEX_DIGITS for ch in value):
            raise HTTPException(
                status_code=400,
                detail="'sha256' must be 64 hexadecimal characters",
            )
        return value

    # -- main flow ----------------------------------------------------------
    async def _handle(
        self, request: Request, raw_path: str | None, raw_digest: str | None
    ) -> Response:
        upstream_path = self._normalize_path(raw_path)
        expected = self._normalize_digest(raw_digest)

        # Pin the generation with no await in between.  A concurrent commit
        # either already happened (we get the new target) or happens after
        # this tick (this request belongs to the old, draining generation).
        generation = self.state.current
        in_flight = generation.register(upstream_path, kind="verified-download")
        in_flight.detail.update(
            {
                "phase": "fetching",
                "target": generation.target_name,
                "expected_sha256": expected,
                "bytes_staged": 0,
            }
        )

        disconnect = asyncio.Event()
        disconnect_task = asyncio.create_task(
            watch_disconnect(request.receive, disconnect),
            name=f"relay-verified-disconnect-{in_flight.id}",
        )
        abort_wait = asyncio.ensure_future(in_flight.event.wait())
        disconnect_wait = asyncio.ensure_future(disconnect.wait())

        file_handle: BinaryIO | None = None
        temp_path: str | None = None
        upstream_response: aiohttp.ClientResponse | None = None
        handed_off = False
        try:
            try:
                temp_path, file_handle = await asyncio.to_thread(self._create_staging_file)
            except OSError as exc:
                return self._error(
                    500,
                    "staging_unavailable",
                    f"cannot create staging file: {type(exc).__name__}: {exc}",
                    generation,
                    in_flight,
                )

            headers = [
                (k, v)
                for k, v in strip_hop_by_hop(header_pairs(request.scope.get("headers", [])))
                if k.lower() not in VERIFIED_DROP_REQUEST_HEADERS
            ]
            # The digest covers the stored bytes, so never negotiate an
            # encoding (aiohttp decompression is disabled globally).
            headers.append(("accept-encoding", "identity"))
            url = generation.base_url + upstream_path

            get_task = asyncio.ensure_future(
                self.state.upstream_session.request(
                    "GET", url, headers=headers, allow_redirects=False
                )
            )
            done, _ = await asyncio.wait(
                {get_task, abort_wait, disconnect_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if get_task not in done:
                get_task.cancel()
                with contextlib.suppress(BaseException):
                    await get_task
                if disconnect.is_set():
                    return self._error(
                        499, "client_disconnected", "client went away", generation, in_flight
                    )
                return self._error(
                    503,
                    "generation_draining",
                    "drain window expired before the upstream responded",
                    generation,
                    in_flight,
                )

            try:
                upstream_response = get_task.result()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                status = 504 if isinstance(exc, asyncio.TimeoutError) else 502
                return self._error(
                    status,
                    "upstream_timeout" if status == 504 else "upstream_error",
                    f"{type(exc).__name__}: {exc}",
                    generation,
                    in_flight,
                )

            if upstream_response.status != 200:
                # Only a complete 200 is acceptable.  Never forward the error
                # body: the client asked for a verified artifact, not a relay
                # of an upstream error page.
                return self._error(
                    502,
                    "upstream_status",
                    f"upstream returned HTTP {upstream_response.status}; only 200 is accepted",
                    generation,
                    in_flight,
                    {"upstream_status": upstream_response.status},
                )

            # We asked for identity; hashing a compressed representation while
            # claiming the digest describes the artifact would be ambiguous.
            encoding = (upstream_response.headers.get("Content-Encoding") or "").strip().lower()
            if encoding not in ("", "identity"):
                return self._error(
                    502,
                    "upstream_encoding",
                    f"upstream applied Content-Encoding {encoding!r}; only identity is accepted",
                    generation,
                    in_flight,
                    {"content_encoding": encoding},
                )

            max_bytes = self.config.verified_max_bytes
            declared: int | None = None
            raw_length = upstream_response.headers.get("Content-Length")
            if raw_length is not None:
                try:
                    declared = int(raw_length)
                except ValueError:
                    declared = None
                if declared is not None and declared > max_bytes:
                    return self._error(
                        413,
                        "artifact_too_large",
                        f"artifact declares {declared} bytes; limit is {max_bytes}",
                        generation,
                        in_flight,
                        {"limit_bytes": max_bytes, "declared_bytes": declared},
                    )

            staged = await self._stage(
                upstream_response,
                file_handle,
                declared,
                max_bytes,
                expected,
                generation,
                in_flight,
                disconnect,
                abort_wait,
                disconnect_wait,
            )
            if isinstance(staged, _Failure):
                return self._error(
                    staged.status,
                    staged.error,
                    staged.detail,
                    generation,
                    in_flight,
                    staged.extra,
                )

            assert temp_path is not None
            in_flight.detail.update(
                {
                    "phase": "delivering",
                    "verified_sha256": staged.digest,
                    "bytes_staged": staged.size,
                }
            )
            response = VerifiedFileResponse(
                relay=self.relay,
                generation=generation,
                in_flight=in_flight,
                temp_path=temp_path,
                headers=staged.headers,
                abort_wait=abort_wait,
                disconnect=disconnect,
                disconnect_task=disconnect_task,
                disconnect_wait=disconnect_wait,
            )
            handed_off = True  # the response owns cleanup from here on
            return response
        finally:
            if not handed_off:
                if upstream_response is not None:
                    upstream_response.close()  # unread/aborted body must not be reused
                if file_handle is not None:
                    with contextlib.suppress(Exception):
                        file_handle.close()
                if temp_path is not None:
                    _unlink_quietly(temp_path)
                disconnect_task.cancel()
                disconnect_wait.cancel()
                abort_wait.cancel()
                with contextlib.suppress(BaseException):
                    await disconnect_task
                generation.unregister(in_flight)

    async def _stage(
        self,
        upstream: aiohttp.ClientResponse,
        file_handle: BinaryIO,
        declared: int | None,
        max_bytes: int,
        expected: str,
        generation: Generation,
        in_flight: InFlightRequest,
        disconnect: asyncio.Event,
        abort_wait: asyncio.Future,
        disconnect_wait: asyncio.Future,
    ) -> _Staged | _Failure:
        """Copy the upstream body to the staging file, hashing chunk by chunk."""
        hasher = hashlib.sha256()
        total = 0
        body = upstream.content.iter_chunked(self.config.chunk_size)
        try:
            while True:
                if in_flight.abort_reason is not None:
                    return _Failure(
                        503,
                        "generation_draining",
                        "drain window expired during verification",
                        {"bytes_staged": total},
                    )
                if disconnect.is_set():
                    return _Failure(
                        499,
                        "client_disconnected",
                        "client went away during verification",
                        {"bytes_staged": total},
                    )
                chunk_task = asyncio.ensure_future(body.__anext__())
                done, _ = await asyncio.wait(
                    {chunk_task, abort_wait, disconnect_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if chunk_task not in done:
                    chunk_task.cancel()
                    with contextlib.suppress(BaseException):
                        await chunk_task
                    if disconnect.is_set():
                        return _Failure(
                            499,
                            "client_disconnected",
                            "client went away during verification",
                            {"bytes_staged": total},
                        )
                    return _Failure(
                        503,
                        "generation_draining",
                        "drain window expired during verification",
                        {"bytes_staged": total},
                    )
                try:
                    chunk = chunk_task.result()
                except StopAsyncIteration:
                    break
                except asyncio.CancelledError:
                    raise
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    return _Failure(
                        502,
                        "upstream_truncated",
                        f"upstream stream failed during verification: {type(exc).__name__}: {exc}",
                        {"bytes_staged": total},
                    )
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    return _Failure(
                        413,
                        "artifact_too_large",
                        f"artifact exceeds the {max_bytes}-byte limit",
                        {"limit_bytes": max_bytes, "bytes_staged": total},
                    )
                hasher.update(chunk)
                try:
                    # Blocking file I/O runs off the event loop; the whole body
                    # is never held in memory.
                    await asyncio.to_thread(_write_and_flush, file_handle, chunk)
                except OSError as exc:
                    return _Failure(
                        500,
                        "staging_failed",
                        f"cannot write staging file: {type(exc).__name__}: {exc}",
                        {"bytes_staged": total},
                    )
                in_flight.detail["bytes_staged"] = total
        finally:
            try:
                file_handle.flush()
                file_handle.close()
            except OSError as exc:  # pragma: no cover - disk failure cleanup
                log.warning("closing staging file failed: %s", exc)

        # The upstream's own length is a second completeness check: a body
        # shorter than its declared length is a truncated transfer, not a
        # verified artifact.
        if declared is not None and total != declared:
            return _Failure(
                502,
                "upstream_truncated",
                f"upstream declared {declared} bytes but sent {total}",
                {"declared_bytes": declared, "received_bytes": total},
            )

        in_flight.detail["phase"] = "verifying"
        actual = hasher.hexdigest()
        if not hmac.compare_digest(actual, expected):
            return _Failure(
                422,
                "digest_mismatch",
                "staged artifact SHA-256 does not match the expected digest",
                {"expected_sha256": expected, "actual_sha256": actual},
            )

        response_headers = [
            (k, v)
            for k, v in strip_hop_by_hop(list(upstream.headers.items()))
            if k.lower() not in DROP_RESPONSE_HEADERS
            and not k.lower().startswith("x-relay-")
            and k.lower() != "content-length"
        ]
        if not any(k.lower() == "content-type" for k, _ in response_headers):
            response_headers.append(("content-type", "application/octet-stream"))
        response_headers.extend(
            [
                ("content-length", str(total)),
                ("x-relay-target", generation.target_name),
                ("x-relay-generation", str(generation.id)),
                ("x-relay-request-id", str(in_flight.id)),
                ("x-relay-verified", "sha256"),
                ("x-relay-sha256", actual),
            ]
        )
        return _Staged(digest=actual, size=total, headers=response_headers)

    # -- helpers ------------------------------------------------------------
    def _create_staging_file(self) -> tuple[str, BinaryIO]:
        directory = self.config.staging_dir
        if directory:
            os.makedirs(directory, exist_ok=True)
        # mkstemp gives 0600 and a unique name: concurrent downloads never
        # share or overwrite a staging file.
        fd, path = tempfile.mkstemp(prefix="relay-verified-", suffix=".part", dir=directory)
        return path, os.fdopen(fd, "wb")

    def _error(
        self,
        status: int,
        error: str,
        detail: str,
        generation: Generation,
        in_flight: InFlightRequest,
        extra: dict[str, Any] | None = None,
    ) -> JSONResponse:
        payload: dict[str, Any] = {
            "error": error,
            "detail": detail,
            "generation": generation.id,
            "target": generation.target_name,
            "request_id": in_flight.id,
        }
        if extra:
            payload.update(extra)
        headers = {
            **JSON_HEADERS,
            "x-relay-generation": str(generation.id),
            "x-relay-request-id": str(in_flight.id),
        }
        if status in (499, 503):
            headers["connection"] = "close"
        return JSONResponse(payload, status_code=status, headers=headers)


class VerifiedFileResponse(Response):
    """Deliver an already-verified staging file, chunk by chunk.

    The staging file is deleted in every exit path.  If the generation drains
    or the client disconnects mid-body, the response deliberately omits the
    final body frame: uvicorn closes the connection and the client sees a
    truncated transfer instead of a falsely complete artifact.
    """

    def __init__(
        self,
        *,
        relay: "RelayApp",
        generation: Generation,
        in_flight: InFlightRequest,
        temp_path: str,
        headers: list[tuple[str, str]],
        abort_wait: asyncio.Future,
        disconnect: asyncio.Event,
        disconnect_task: asyncio.Task,
        disconnect_wait: asyncio.Future,
    ) -> None:
        self.relay = relay
        self.generation = generation
        self.in_flight = in_flight
        self.temp_path = temp_path
        self.abort_wait = abort_wait
        self.disconnect = disconnect
        self.disconnect_task = disconnect_task
        self.disconnect_wait = disconnect_wait
        self.media_type = None
        self.status_code = 200
        self.background = None
        self.raw_headers = [
            (k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers
        ]

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        outcome = "completed"
        file_handle: BinaryIO | None = None
        try:
            if self.in_flight.abort_reason is not None:
                # Verified, but the drain window ended before headers went
                # out.  No artifact bytes have been sent, so a clean 503.
                await self._send_unavailable(send)
                outcome = f"generation_draining ({self.in_flight.abort_reason})"
                return
            if self.disconnect.is_set():
                outcome = "client_disconnected"
                return

            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
            file_handle = await asyncio.to_thread(open, self.temp_path, "rb")
            # Unlink the staging name as soon as the verified body is open.
            # The open descriptor keeps the data readable, but no staging file
            # remains on disk while the artifact is being delivered (and a
            # disconnect simply closes the descriptor).  On platforms where an
            # open file cannot be unlinked, cleanup happens in ``finally``.
            try:
                os.unlink(self.temp_path)
                self.temp_path = ""
            except OSError:
                pass
            chunk = await asyncio.to_thread(file_handle.read, self.relay.config.chunk_size)
            # A zero-length artifact completes immediately: there are no
            # artifact bytes to withhold, so a disconnect racing the empty body
            # frame must not be misreported as an interrupted download.
            while chunk:
                if self.in_flight.abort_reason is not None:
                    outcome = f"generation_draining ({self.in_flight.abort_reason})"
                    break
                if self.disconnect.is_set():
                    outcome = "client_disconnected"
                    break
                if not await self._send_body(send, chunk):
                    if self.in_flight.abort_reason is not None:
                        outcome = f"generation_draining ({self.in_flight.abort_reason})"
                    elif self.disconnect.is_set():
                        outcome = "client_disconnected"
                    else:
                        outcome = "send_failed"
                    break
                chunk = await asyncio.to_thread(file_handle.read, self.relay.config.chunk_size)

            if outcome == "completed":
                # Clean up before declaring completion, so a client that sees a
                # complete body can rely on the staging file already being gone.
                if file_handle is not None:
                    with contextlib.suppress(Exception):
                        file_handle.close()
                    file_handle = None
                _unlink_quietly(self.temp_path)
                await send({"type": "http.response.body", "body": b"", "more_body": False})
            else:
                # Deliberately no final body frame: uvicorn closes this one
                # connection and the client sees a truncated transfer.
                log.warning(
                    "interrupting verified download %s on generation %s (%s): %s",
                    self.in_flight.id,
                    self.generation.id,
                    self.generation.target_name,
                    outcome,
                )
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except (OSError, aiohttp.ClientError) as exc:
            outcome = f"client_disconnected: {type(exc).__name__}: {exc}"
            log.warning(
                "verified download %s on generation %s stopped while sending: %s",
                self.in_flight.id,
                self.generation.id,
                outcome,
            )
        finally:
            if file_handle is not None:
                with contextlib.suppress(Exception):
                    file_handle.close()
            _unlink_quietly(self.temp_path)
            self.disconnect_task.cancel()
            self.disconnect_wait.cancel()
            with contextlib.suppress(BaseException):
                await self.disconnect_task
            if not self.abort_wait.done():
                self.abort_wait.cancel()
            else:
                with contextlib.suppress(BaseException):
                    self.abort_wait.result()
            self.generation.unregister(self.in_flight)

    async def _send_body(self, send: Send, chunk: bytes) -> bool:
        """Send one chunk; False if drain/disconnect wins the race first."""
        send_task = asyncio.ensure_future(
            send({"type": "http.response.body", "body": chunk, "more_body": True})
        )
        done, _ = await asyncio.wait(
            {send_task, self.abort_wait, self.disconnect_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if send_task not in done:
            send_task.cancel()
            with contextlib.suppress(BaseException):
                await send_task
            return False
        try:
            send_task.result()  # surface send errors to __call__
        except OSError:
            # uvicorn reports a client that vanished mid-write as OSError /
            # ClientDisconnected (also an OSError).  Do not let it escape: the
            # connection is already gone, and the response must simply stop
            # (no final frame) while cleanup runs.
            return False
        return True

    async def _send_unavailable(self, send: Send) -> None:
        payload = json.dumps(
            {
                "error": "generation_draining",
                "detail": "drain window expired before the verified artifact was sent",
                "generation": self.generation.id,
                "target": self.generation.target_name,
                "request_id": self.in_flight.id,
            }
        ).encode()
        headers = [
            ("content-type", "application/json"),
            ("content-length", str(len(payload))),
            ("cache-control", "no-store"),
            ("x-relay-generation", str(self.generation.id)),
            ("connection", "close"),
        ]
        await send(
            {
                "type": "http.response.start",
                "status": 503,
                "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers],
            }
        )
        await send({"type": "http.response.body", "body": payload, "more_body": False})


def _write_and_flush(file_handle: BinaryIO, chunk: bytes) -> None:
    """Write one chunk and flush it (run in a worker thread by asyncio)."""
    file_handle.write(chunk)
    file_handle.flush()


def _unlink_quietly(path: str) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError:  # pragma: no cover - cleanup best effort
        log.warning("could not remove staging file %s", path, exc_info=True)
