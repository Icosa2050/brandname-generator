# ruff: noqa: E402
from __future__ import annotations

import io
import socket
import sys
import unittest
from pathlib import Path
from urllib import error
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.http_client import (
    USER_AGENT,
    HttpResponse,
    _normalize_headers,
    _request,
    _retry_after_seconds,
    fetch_json,
    fetch_response,
    fetch_status,
    fetch_text,
)
from brandpipe.models import ErrorKind


class _HeadersWithoutItems:
    def items(self):
        raise RuntimeError("bad headers")


class _FakeUrlopenResponse:
    def __init__(
        self,
        *,
        body: bytes,
        headers: object,
        status: int = 200,
        final_url: str = "https://example.test/final",
    ) -> None:
        self._body = body
        self.headers = headers
        self.status = status
        self._final_url = final_url

    def read(self) -> bytes:
        return self._body

    def geturl(self) -> str:
        return self._final_url

    def __enter__(self) -> "_FakeUrlopenResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class HttpClientTests(unittest.TestCase):
    def test_http_response_json_and_retryable_behavior(self) -> None:
        self.assertEqual(
            HttpResponse(
                ok=True,
                url="https://example.test",
                status_code=200,
                text='{"ok": true}',
                headers={},
                error_kind=ErrorKind.NONE,
                error_message="",
            ).json(),
            {"ok": True},
        )
        self.assertIsNone(
            HttpResponse(
                ok=True,
                url="https://example.test",
                status_code=200,
                text='["not", "a", "dict"]',
                headers={},
                error_kind=ErrorKind.NONE,
                error_message="",
            ).json()
        )
        self.assertIsNone(
            HttpResponse(
                ok=True,
                url="https://example.test",
                status_code=200,
                text="{broken-json",
                headers={},
                error_kind=ErrorKind.NONE,
                error_message="",
            ).json()
        )
        self.assertTrue(
            HttpResponse(
                ok=False,
                url="https://example.test",
                status_code=429,
                text="",
                headers={},
                error_kind=ErrorKind.RATE_LIMITED,
                error_message="limited",
            ).retryable
        )
        self.assertFalse(
            HttpResponse(
                ok=False,
                url="https://example.test",
                status_code=500,
                text="",
                headers={},
                error_kind=ErrorKind.HTTP,
                error_message="server",
            ).retryable
        )

    def test_header_helpers_normalize_and_parse_retry_after(self) -> None:
        normalized = _normalize_headers(
            {
                "Retry-After": "12",
                "X-Test": 9,
                None: "ignored",
                "Missing": None,
            }
        )

        self.assertEqual(normalized, {"Retry-After": "12", "X-Test": "9"})
        self.assertEqual(_normalize_headers(_HeadersWithoutItems()), {})
        self.assertEqual(_retry_after_seconds({"retry-after": "5.5"}), 5.5)
        self.assertEqual(_retry_after_seconds({"Retry-After": "-2"}), 0.0)
        self.assertIsNone(_retry_after_seconds({"Retry-After": "later"}))
        self.assertIsNone(_retry_after_seconds({"X-Test": "1"}))

    def test_request_builds_method_and_headers(self) -> None:
        req = _request(
            "https://example.test/search?q=brandpipe",
            method="POST",
            headers={"Accept": "application/json", "X-Test": "1"},
        )

        header_map = {key.lower(): value for key, value in req.header_items()}
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.full_url, "https://example.test/search?q=brandpipe")
        self.assertEqual(header_map["user-agent"], USER_AGENT)
        self.assertEqual(header_map["accept"], "application/json")
        self.assertEqual(header_map["x-test"], "1")

    def test_fetch_response_success_uses_response_metadata(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(req, timeout: float):
            captured["request"] = req
            captured["timeout"] = timeout
            return _FakeUrlopenResponse(
                body=b'{"status":"ok"}',
                headers={"Retry-After": "7", "X-Test": "1"},
                status=201,
                final_url="https://example.test/redirected",
            )

        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=fake_urlopen):
            response = fetch_response(
                "https://example.test/original",
                timeout=2.5,
                method="PATCH",
                headers={"Accept": "application/json"},
            )

        self.assertTrue(response.ok)
        self.assertEqual(response.url, "https://example.test/redirected")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.text, '{"status":"ok"}')
        self.assertEqual(response.headers, {"Retry-After": "7", "X-Test": "1"})
        self.assertEqual(response.error_kind, ErrorKind.NONE)
        self.assertEqual(response.retry_after_s, 7.0)
        self.assertEqual(captured["timeout"], 2.5)
        self.assertEqual(captured["request"].get_method(), "PATCH")

    def test_fetch_response_handles_rate_limited_http_error(self) -> None:
        http_error = error.HTTPError(
            url="https://example.test/api",
            code=429,
            msg="Too Many Requests",
            hdrs={"Retry-After": "12"},
            fp=io.BytesIO(b'{"error":"slow down"}'),
        )

        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=http_error):
            response = fetch_response("https://example.test/api")

        self.assertFalse(response.ok)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.error_kind, ErrorKind.RATE_LIMITED)
        self.assertEqual(response.text, '{"error":"slow down"}')
        self.assertEqual(response.retry_after_s, 12.0)
        self.assertTrue(response.retryable)

    def test_fetch_response_handles_non_rate_limited_http_error(self) -> None:
        http_error = error.HTTPError(
            url="https://example.test/api",
            code=503,
            msg="Service Unavailable",
            hdrs={"X-Test": "1"},
            fp=io.BytesIO(b"temporarily down"),
        )

        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=http_error):
            response = fetch_response("https://example.test/api")

        self.assertFalse(response.ok)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.error_kind, ErrorKind.HTTP)
        self.assertEqual(response.headers, {"X-Test": "1"})
        self.assertEqual(response.text, "temporarily down")
        self.assertIsNone(response.retry_after_s)
        self.assertFalse(response.retryable)

    def test_fetch_response_handles_socket_timeout(self) -> None:
        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=socket.timeout("timed out")):
            response = fetch_response("https://example.test/slow")

        self.assertFalse(response.ok)
        self.assertIsNone(response.status_code)
        self.assertEqual(response.error_kind, ErrorKind.TIMEOUT)
        self.assertEqual(response.error_message, "timed out")
        self.assertTrue(response.retryable)

    def test_fetch_response_handles_url_error_timeout_and_transport(self) -> None:
        timeout_error = error.URLError(TimeoutError("deadline"))
        transport_error = error.URLError(OSError("dns failed"))

        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=timeout_error):
            timeout_response = fetch_response("https://example.test/timeout")
        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=transport_error):
            transport_response = fetch_response("https://example.test/transport")

        self.assertEqual(timeout_response.error_kind, ErrorKind.TIMEOUT)
        self.assertEqual(timeout_response.error_message, "deadline")
        self.assertEqual(transport_response.error_kind, ErrorKind.TRANSPORT)
        self.assertEqual(transport_response.error_message, "dns failed")
        self.assertTrue(transport_response.retryable)

    def test_fetch_response_handles_unexpected_exception(self) -> None:
        with mock.patch("brandpipe.http_client.request.urlopen", side_effect=RuntimeError("boom")):
            response = fetch_response("https://example.test/crash")

        self.assertFalse(response.ok)
        self.assertEqual(response.error_kind, ErrorKind.UNEXPECTED)
        self.assertEqual(response.error_message, "RuntimeError: boom")
        self.assertFalse(response.retryable)

    def test_wrapper_helpers_delegate_to_fetch_response(self) -> None:
        sentinel = HttpResponse(
            ok=True,
            url="https://example.test",
            status_code=200,
            text="{}",
            headers={},
            error_kind=ErrorKind.NONE,
            error_message="",
        )

        with mock.patch("brandpipe.http_client.fetch_response", return_value=sentinel) as fetch_mock:
            self.assertIs(fetch_json("https://example.test/json", timeout=1.0), sentinel)
            self.assertIs(fetch_text("https://example.test/text", method="HEAD"), sentinel)
            self.assertIs(fetch_status("https://example.test/status", headers={"Accept": "*/*"}), sentinel)

        self.assertEqual(fetch_mock.call_count, 3)


if __name__ == "__main__":
    unittest.main()
