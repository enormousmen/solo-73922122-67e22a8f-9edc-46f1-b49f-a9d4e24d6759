"""End-to-end tests for the digest-verified download API."""

from __future__ import annotations

import asyncio
import os
import time

import aiohttp
import pytest

from .fake_upstream import artifact_bytes, sha256_hex
from .test_proxy_integration import stack, status, switch

pytestmark = pytest.mark.asyncio

VERIFIED = "/api/verified-download"


def _artifact(size: int, seed: str = "test", chunk: int = 256) -> tuple[bytes, str]:
    body = artifact_bytes(size, seed=seed, chunk=chunk)
    return body, sha256_hex(body)


async def _get_verified(
    base: str,
    *,
    path: str,
    digest: str,
    params: dict | None = None,
) -> tuple[int, dict, bytes]:
    async with aiohttp.ClientSession() as session:
        query = {"path": path, "sha256": digest}
        if params:
            query.update(params)
        async with session.get(base + VERIFIED, params=query) as response:
            return (
                response.status,
                {k.lower(): v for k, v in response.headers.items()},
                await response.read(),
            )


async def _post_verified(base: str, *, path: str, digest: str) -> tuple[int, dict, bytes]:
    async with aiohttp.ClientSession() as session:
        async with session.post(base + VERIFIED, json={"path": path, "sha256": digest}) as response:
            return (
                response.status,
                {k.lower(): v for k, v in response.headers.items()},
                await response.read(),
            )


def _staging_files(directory) -> list[str]:
    return sorted(os.listdir(directory))


async def test_get_verified_download_returns_bytes_only_after_digest_matches(tmp_path):
    body, digest = _artifact(300_000, seed="alpha", chunk=256)
    async with stack(verified_max_bytes=1024 * 1024, staging_dir=str(tmp_path)) as s:
        path = "/verified?seed=alpha&bytes=300000&chunk=256"
        code, headers, payload = await _get_verified(s["base"], path=path, digest=digest)
        assert code == 200
        assert payload == body
        assert headers["x-relay-verified"] == "sha256"
        assert headers["x-relay-sha256"] == digest
        assert headers["x-relay-target"] == "blue"
        assert headers["x-relay-generation"] == "1"
        assert headers["content-length"] == str(len(body))
        assert headers["content-type"] == "application/octet-stream"
        assert _staging_files(tmp_path) == []
        snap = await status(s["base"])
        assert snap["generations"][0]["in_flight"] == 0
        # The upstream saw the query string exactly as supplied.
        sent_headers = s["upstreams"]["blue"].server.config.app.state.last_verified_headers
        assert sent_headers.get("accept-encoding") == "identity"


async def test_post_verified_download_accepts_prefixed_uppercase_digest(tmp_path):
    body, digest = _artifact(65_536, seed="post", chunk=1024)
    async with stack(staging_dir=str(tmp_path)) as s:
        code, headers, payload = await _post_verified(
            s["base"],
            path="/verified?seed=post&bytes=65536&chunk=1024",
            digest="sha256:" + digest.upper(),
        )
        assert code == 200
        assert payload == body
        assert headers["x-relay-sha256"] == digest
        assert _staging_files(tmp_path) == []


async def test_short_alias_download_endpoint_works(tmp_path):
    body, digest = _artifact(4096, seed="alias")
    async with stack(staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                s["base"] + "/api/download",
                params={"path": "/verified?seed=alias&bytes=4096", "sha256": digest},
            ) as response:
                assert response.status == 200
                assert await response.read() == body


async def test_digest_mismatch_returns_no_artifact_bytes_and_cleans_staging(tmp_path):
    body, digest = _artifact(100_000, seed="mismatch")
    wrong = "0" * 64
    async with stack(staging_dir=str(tmp_path)) as s:
        code, headers, payload = await _get_verified(
            s["base"], path="/verified?seed=mismatch&bytes=100000", digest=wrong
        )
        assert code == 422
        assert headers["content-type"].startswith("application/json")
        data = __import__("json").loads(payload)
        assert data["error"] == "digest_mismatch"
        assert data["expected_sha256"] == wrong
        assert data["actual_sha256"] == digest
        # Only the JSON envelope is returned: no artifact body bytes at all.
        assert len(payload) < 1000
        assert body[:64] not in payload
        assert _staging_files(tmp_path) == []
        snap = await status(s["base"])
        assert snap["generations"][0]["in_flight"] == 0


async def test_upstream_non_200_is_rejected_without_forwarding_its_body(tmp_path):
    async with stack(staging_dir=str(tmp_path)) as s:
        code, _, payload = await _get_verified(
            s["base"], path="/status/404", digest="0" * 64
        )
        assert code == 502
        data = __import__("json").loads(payload)
        assert data["error"] == "upstream_status"
        assert data["upstream_status"] == 404
        assert b"upstream-error-body" not in payload
        assert _staging_files(tmp_path) == []

        # A redirect is not a 200 either (redirects are never followed).
        async with aiohttp.ClientSession() as session:
            async with session.get(
                s["base"] + "/api/verified-download",
                params={
                    "path": "/status/302",
                    "sha256": "0" * 64,
                },
            ) as response:
                assert response.status == 502
                await response.read()


async def test_declared_length_over_limit_is_rejected_up_front(tmp_path):
    async with stack(verified_max_bytes=1000, staging_dir=str(tmp_path)) as s:
        # /fixed declares content-length 2176 > 1000.
        code, _, payload = await _get_verified(
            s["base"], path="/fixed", digest="0" * 64
        )
        assert code == 413
        data = __import__("json").loads(payload)
        assert data["error"] == "artifact_too_large"
        assert data["limit_bytes"] == 1000
        assert data["declared_bytes"] == 2176
        assert b"fixed-length-body" not in payload
        assert _staging_files(tmp_path) == []


async def test_chunked_body_over_limit_is_cut_off_and_staging_removed(tmp_path):
    async with stack(verified_max_bytes=2000, staging_dir=str(tmp_path)) as s:
        body, digest = _artifact(10_000, seed="big")
        code, _, payload = await _get_verified(
            s["base"], path="/verified?seed=big&bytes=10000&chunk=100", digest=digest
        )
        assert code == 413
        data = __import__("json").loads(payload)
        assert data["error"] == "artifact_too_large"
        assert len(payload) < 1000
        assert _staging_files(tmp_path) == []
        snap = await status(s["base"])
        assert snap["generations"][0]["in_flight"] == 0


async def test_truncated_upstream_stream_is_rejected(tmp_path):
    async with stack(staging_dir=str(tmp_path)) as s:
        code, _, payload = await _get_verified(
            s["base"], path="/truncated?bytes=100000&cut=1000", digest="0" * 64
        )
        assert code == 502
        data = __import__("json").loads(payload)
        assert data["error"] in ("upstream_truncated", "upstream_error")
        assert len(payload) < 1000
        assert _staging_files(tmp_path) == []


async def test_missing_or_malformed_parameters_are_rejected():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            # Required query parameters -> 422 from FastAPI, same as other APIs.
            async with session.get(s["base"] + VERIFIED) as r:
                assert r.status == 422
            async with session.get(
                s["base"] + VERIFIED, params={"path": "/verified"}
            ) as r:
                assert r.status == 422
            # Present but semantically invalid -> 400 with a clear message.
            async with session.get(
                s["base"] + VERIFIED,
                params={"path": "/verified", "sha256": "not-a-digest"},
            ) as r:
                assert r.status == 400
            async with session.get(
                s["base"] + VERIFIED,
                params={"path": "http://evil.example/x", "sha256": "0" * 64},
            ) as r:
                assert r.status == 400
            async with session.get(
                s["base"] + VERIFIED,
                params={"path": "/verified", "sha256": "z" * 64},
            ) as r:
                assert r.status == 400
            # POST body validation is a normal FastAPI 422.
            async with session.post(s["base"] + VERIFIED, json={"path": "/verified"}) as r:
                assert r.status == 422
            async with session.post(
                s["base"] + VERIFIED, json={"path": "/verified", "sha256": "bad"}
            ) as r:
                assert r.status == 400
        snap = await status(s["base"])
        assert snap["generations"][0]["in_flight"] == 0


async def test_verified_download_counts_as_in_flight_and_is_drained_with_staging(tmp_path):
    body, digest = _artifact(200_000, seed="slow", chunk=1000)
    async with stack(drain_seconds=0.8, staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            task = asyncio.create_task(
                session.get(
                    s["base"] + VERIFIED,
                    params={
                        "path": "/verified?seed=slow&bytes=200000&chunk=1000&delay=0.01",
                        "sha256": digest,
                    },
                )
            )
            # Wait until staging is visible in the status snapshot.
            for _ in range(100):
                snap = await status(s["base"])
                requests = snap["generations"][0]["in_flight_requests"]
                if requests:
                    break
                await asyncio.sleep(0.02)
            assert len(requests) == 1
            assert requests[0]["kind"] == "verified-download"
            assert requests[0]["detail"]["phase"] == "fetching"
            assert requests[0]["detail"]["expected_sha256"] == digest
            assert not task.done()  # no response headers before verification
            # The staged bytes are on disk (partial), not held in a response.
            staged = _staging_files(tmp_path)
            assert len(staged) == 1
            assert os.path.getsize(tmp_path / staged[0]) > 0

            code, _ = await switch(s["base"], "green")
            assert code == 200
            # A fresh verified download goes to the new generation immediately.
            fresh_body, fresh_digest = _artifact(4096, seed="slow")
            async with session.get(
                s["base"] + VERIFIED,
                params={"path": "/verified?seed=slow&bytes=4096", "sha256": fresh_digest},
            ) as fresh:
                assert fresh.headers["x-relay-target"] == "green"
                assert await fresh.read() == fresh_body

            response = await asyncio.wait_for(task, timeout=10)
            assert response.status == 503
            data = await response.json()
            assert data["error"] == "generation_draining"
            response.close()
        assert _staging_files(tmp_path) == []
        snap = await status(s["base"])
        old = [g for g in snap["generations"] if g["id"] == 1][0]
        assert old["state"] == "closed"
        assert old["in_flight"] == 0


async def test_drain_during_delivery_truncates_and_never_reports_complete(tmp_path):
    size = 12 * 1024 * 1024  # fits the default 16 MiB cap
    body, digest = _artifact(size, seed="deliver", chunk=65536)
    async with stack(drain_seconds=0.6, staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            response = await session.get(
                s["base"] + VERIFIED,
                params={
                    "path": f"/verified?seed=deliver&bytes={size}&chunk=65536",
                    "sha256": digest,
                },
            )
            assert response.status == 200
            assert response.headers["x-relay-sha256"] == digest
            assert response.headers["content-length"] == str(size)

            # Read one chunk, then switch.  Pausing the read for longer than
            # the drain window pins the interrupt: once we resume, delivery
            # must stop short rather than finish.
            first = await asyncio.wait_for(response.content.read(65536), timeout=5)
            assert first

            code, _ = await switch(s["base"], "green")
            assert code == 200
            await asyncio.sleep(1.2)

            received = len(first)
            with pytest.raises(aiohttp.ClientError):
                async for chunk in response.content.iter_any():
                    received += len(chunk)
            # Incomplete body, no final frame: never a 200-complete artifact.
            assert 0 < received < size
            response.close()
        assert _staging_files(tmp_path) == []
        snap = await status(s["base"])
        old = [g for g in snap["generations"] if g["id"] == 1][0]
        assert old["state"] == "closed" and old["in_flight"] == 0


async def test_client_disconnect_stops_staging_and_removes_temp_file(tmp_path):
    # A slow upstream keeps the request in the staging phase long enough for
    # the disconnect to be observed before verification finishes.
    body, digest = _artifact(300_000, seed="gone", chunk=1000)
    async with stack(drain_seconds=30.0, staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            response = await session.get(
                s["base"] + VERIFIED,
                params={
                    "path": "/verified?seed=gone&bytes=300000&chunk=1000&delay=0.01",
                    "sha256": digest,
                },
            )
            # Headers are not sent before verification; a slow upstream means
            # the client is still waiting for them here.  Abort the request by
            # closing the session/response connection.
            response.close()
        for _ in range(100):
            files = _staging_files(tmp_path)
            snap = await status(s["base"])
            if not files and snap["generations"][0]["in_flight"] == 0:
                break
            await asyncio.sleep(0.05)
        assert files == []
        assert snap["generations"][0]["in_flight"] == 0


async def test_client_disconnect_during_delivery_cleans_staging(tmp_path):
    size = 12 * 1024 * 1024
    body, digest = _artifact(size, seed="gone2", chunk=65536)
    async with stack(drain_seconds=30.0, staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            response = await session.get(
                s["base"] + VERIFIED,
                params={
                    "path": f"/verified?seed=gone2&bytes={size}&chunk=65536",
                    "sha256": digest,
                },
            )
            assert response.status == 200
            # Do not read the body: the relay is blocked on flow control and
            # still delivering.
            await asyncio.sleep(0.2)
            assert (await status(s["base"]))["generations"][0]["in_flight"] == 1
            response.close()
        for _ in range(100):
            snap = await status(s["base"])
            if not _staging_files(tmp_path) and snap["generations"][0]["in_flight"] == 0:
                break
            await asyncio.sleep(0.05)
        assert _staging_files(tmp_path) == []
        assert snap["generations"][0]["in_flight"] == 0


async def test_two_concurrent_verified_downloads_stage_separately(tmp_path):
    one_body, one_digest = _artifact(100_000, seed="one", chunk=1000)
    two_body, two_digest = _artifact(100_000, seed="two", chunk=1000)
    async with stack(staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            first = asyncio.create_task(
                session.get(
                    s["base"] + VERIFIED,
                    params={
                        "path": "/verified?seed=one&bytes=100000&chunk=1000&delay=0.01",
                        "sha256": one_digest,
                    },
                )
            )
            second = asyncio.create_task(
                session.get(
                    s["base"] + VERIFIED,
                    params={
                        "path": "/verified?seed=two&bytes=100000&chunk=1000&delay=0.01",
                        "sha256": two_digest,
                    },
                )
            )
            for _ in range(100):
                if len(_staging_files(tmp_path)) == 2:
                    break
                await asyncio.sleep(0.02)
            assert len(_staging_files(tmp_path)) == 2
            r1 = await first
            r2 = await second
            assert await r1.read() == one_body
            assert await r2.read() == two_body
            r1.close()
            r2.close()
        assert _staging_files(tmp_path) == []


async def test_slow_verified_download_does_not_block_switch_or_status(tmp_path):
    body, digest = _artifact(300_000, seed="slowapi", chunk=1000)
    async with stack(drain_seconds=30.0, staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            task = asyncio.create_task(
                session.get(
                    s["base"] + VERIFIED,
                    params={
                        "path": "/verified?seed=slowapi&bytes=300000&chunk=1000&delay=0.01",
                        "sha256": digest,
                    },
                )
            )
            await asyncio.sleep(0.2)
            started = time.monotonic()
            snap, result = await asyncio.gather(
                status(s["base"]), switch(s["base"], "green")
            )
            assert time.monotonic() - started < 1.5
            assert snap["generations"][0]["in_flight"] == 1
            assert result[0] == 200
            response = await task
            # The request was accepted before the switch and belongs to the
            # old generation; with a 30s drain window it can still complete.
            assert response.status == 200
            assert await response.read() == body
            response.close()


async def test_exact_limit_succeeds_and_one_byte_over_fails(tmp_path):
    async with stack(verified_max_bytes=1000, staging_dir=str(tmp_path)) as s:
        body, digest = _artifact(1000, seed="edge")
        code, headers, payload = await _get_verified(
            s["base"], path="/verified?seed=edge&bytes=1000", digest=digest
        )
        assert code == 200
        assert payload == body
        assert headers["content-length"] == "1000"

        over_body, over_digest = _artifact(1001, seed="edge")
        code, _, payload = await _get_verified(
            s["base"], path="/verified?seed=edge&bytes=1001", digest=over_digest
        )
        assert code == 413
        assert b"digest_mismatch" not in payload
        assert _staging_files(tmp_path) == []


async def test_empty_artifact_verifies(tmp_path):
    async with stack(staging_dir=str(tmp_path)) as s:
        digest = sha256_hex(b"")
        code, headers, payload = await _get_verified(
            s["base"], path="/verified?seed=empty&bytes=0", digest=digest
        )
        assert code == 200
        assert payload == b""
        assert headers["content-length"] == "0"
        assert headers["x-relay-sha256"] == digest
        # A zero-byte artifact still stages (and hashes) a file; cleanup can
        # trail the response by a tick, so allow a short grace period.
        for _ in range(40):
            if _staging_files(tmp_path) == []:
                break
            await asyncio.sleep(0.05)
        assert _staging_files(tmp_path) == []


async def test_query_string_with_absolute_url_value_is_forwarded(tmp_path):
    """Only the path part is validated; query values may contain '://'."""
    async with stack(staging_dir=str(tmp_path)) as s:
        body = artifact_bytes(64, seed="q")
        # The upstream test app ignores unknown query params; the point is
        # that the relay does not reject the request or rewrite the query.
        code, _, payload = await _get_verified(
            s["base"],
            path="/verified?seed=q&bytes=64&next=http://example/x",
            digest=sha256_hex(body),
        )
        assert code == 200
        assert payload == body


async def test_range_and_conditional_headers_are_not_forwarded(tmp_path):
    """The digest must cover the complete artifact, not a partial/304 view."""
    body = artifact_bytes(2048, seed="hdr")
    async with stack(staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                s["base"] + VERIFIED,
                params={"path": "/verified?seed=hdr&bytes=2048", "sha256": sha256_hex(body)},
                headers={
                    "Range": "bytes=0-10",
                    "Accept-Encoding": "gzip",
                    "If-None-Match": '"etag"',
                    "If-Modified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
                    "Authorization": "Bearer t",
                },
            ) as response:
                assert response.status == 200
                assert await response.read() == body
        seen = s["upstreams"]["blue"].server.config.app.state.last_verified_headers
        assert seen.get("accept-encoding") == "identity"
        for name in ("range", "if-none-match", "if-modified-since"):
            assert name not in seen
        # Ordinary end-to-end headers still pass through.
        assert seen.get("authorization") == "Bearer t"


async def test_head_on_verified_download_is_not_proxied_upstream(tmp_path):
    async with stack(staging_dir=str(tmp_path)) as s:
        async with aiohttp.ClientSession() as session:
            async with session.head(
                s["base"] + VERIFIED,
                params={"path": "/verified?bytes=10", "sha256": "0" * 64},
                allow_redirects=False,
            ) as response:
                assert response.status == 405
                assert response.headers["allow"] == "GET, POST"
        assert _staging_files(tmp_path) == []


async def test_openapi_and_docs_expose_verified_download_api():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + "/openapi.json") as r:
                spec = await r.json()
            async with session.get(s["base"] + "/docs") as r:
                assert r.status == 200
                assert "swagger" in (await r.text()).lower()
        paths = spec["paths"]
        assert "/api/verified-download" in paths
        assert {"get", "post"} <= set(paths["/api/verified-download"])
        # The alias is operational but not duplicated in the schema.
        assert "/api/download" not in paths
