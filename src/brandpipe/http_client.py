from __future__ import annotations

from dataclasses import dataclass
import json
import socket
from typing import Any
from urllib import error, request

from .models import ErrorKind


USER_AGENT = "brandpipe/2.0"


@dataclass(frozen=True)
class HttpResponse:
    ok: bool
    url: str
    status_code: int | None
    text: str
    headers: dict[str, str]
    error_kind: ErrorKind
    error_message: str
    retry_after_s: float | None = None

    @property
    def retryable(self) -> bool:
        return self.error_kind in {
            ErrorKind.NONE,
            ErrorKind.RATE_LIMITED,
            ErrorKind.TIMEOUT,
            ErrorKind.TRANSPORT,
        }

    def json(self) -> dict[str, Any] | None:
        if not self.text:
            return None
        try:
            value = json.loads(self.text)
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None


def _normalize_headers(raw_headers) -> dict[str, str]:
    headers: dict[str, str] = {}
    try:
        items = raw_headers.items()
    except Exception:
        return headers
    for key, value in items:
        if key is None or value is None:
            continue
        headers[str(key)] = str(value)
    return headers


def _retry_after_seconds(headers: dict[str, str]) -> float | None:
    for key, value in headers.items():
        if str(key).lower() != "retry-after":
            continue
        try:
            return max(0.0, float(str(value).strip()))
        except ValueError:
            return None
    return None


def _request(url: str, *, method: str, headers: dict[str, str] | None) -> request.Request:
    req = request.Request(url, method=method)
    req.add_header("User-Agent", USER_AGENT)
    for key, value in (headers or {}).items():
        req.add_header(str(key), str(value))
    return req


def fetch_response(
    url: str,
    *,
    timeout: float = 8.0,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    req = _request(url, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="replace")
            response_headers = _normalize_headers(response.headers)
            return HttpResponse(
                ok=True,
                url=str(response.geturl() or url),
                status_code=int(getattr(response, "status", 200)),
                text=payload,
                headers=response_headers,
                error_kind=ErrorKind.NONE,
                error_message="",
                retry_after_s=_retry_after_seconds(response_headers),
            )
    except error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        response_headers = _normalize_headers(exc.headers)
        status_code = int(exc.code)
        error_kind = ErrorKind.RATE_LIMITED if status_code == 429 else ErrorKind.HTTP
        return HttpResponse(
            ok=False,
            url=str(exc.geturl() or url),
            status_code=status_code,
            text=payload,
            headers=response_headers,
            error_kind=error_kind,
            error_message=str(exc),
            retry_after_s=_retry_after_seconds(response_headers),
        )
    except socket.timeout as exc:
        return HttpResponse(
            ok=False,
            url=url,
            status_code=None,
            text="",
            headers={},
            error_kind=ErrorKind.TIMEOUT,
            error_message=str(exc),
        )
    except error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        error_kind = ErrorKind.TIMEOUT if isinstance(reason, TimeoutError) else ErrorKind.TRANSPORT
        return HttpResponse(
            ok=False,
            url=url,
            status_code=None,
            text="",
            headers={},
            error_kind=error_kind,
            error_message=str(reason),
        )
    except Exception as exc:
        return HttpResponse(
            ok=False,
            url=url,
            status_code=None,
            text="",
            headers={},
            error_kind=ErrorKind.UNEXPECTED,
            error_message=f"{exc.__class__.__name__}: {exc}",
        )


def fetch_json(
    url: str,
    *,
    timeout: float = 8.0,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    return fetch_response(url, timeout=timeout, method=method, headers=headers)


def fetch_text(
    url: str,
    *,
    timeout: float = 8.0,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    return fetch_response(url, timeout=timeout, method=method, headers=headers)


def fetch_status(
    url: str,
    *,
    timeout: float = 8.0,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    return fetch_response(url, timeout=timeout, method=method, headers=headers)
