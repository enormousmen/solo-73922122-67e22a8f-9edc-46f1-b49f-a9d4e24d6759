"""Unit tests for verified-download parameter handling (no network)."""

from fastapi import HTTPException
import pytest

from relay.verified import VerifiedDownload


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("/artifact", "/artifact"),
        ("artifact", "/artifact"),
        ("/verified?seed=x&bytes=1", "/verified?seed=x&bytes=1"),
        ("/verified?next=http://x/y", "/verified?next=http://x/y"),
        ("  /artifact?x=1  ", "/artifact?x=1"),
    ],
)
def test_path_normalization(raw, expected):
    assert VerifiedDownload._normalize_path(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "//evil/path", "http://evil/x", "https://evil/x", "\\\\evil", "/a\x00b"])
def test_path_rejection(raw):
    with pytest.raises(HTTPException) as info:
        VerifiedDownload._normalize_path(raw)
    assert info.value.status_code == 400


@pytest.mark.parametrize(
    "raw",
    [
        "a" * 64,
        "A" * 64,
        "sha256:" + "b" * 64,
        "sha256=" + "C" * 64,
        "  " + "d" * 64 + "  ",
    ],
)
def test_digest_normalization(raw):
    value = VerifiedDownload._normalize_digest(raw)
    assert value == value.lower()
    assert len(value) == 64


@pytest.mark.parametrize("raw", [None, "", "abc", "g" * 64, "a" * 63, "a" * 65, "sha256:"])
def test_digest_rejection(raw):
    with pytest.raises(HTTPException) as info:
        VerifiedDownload._normalize_digest(raw)
    assert info.value.status_code == 400
