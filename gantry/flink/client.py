# SPDX-License-Identifier: Apache-2.0
"""Small dependency-free clients for Flink SQL Gateway and JobManager REST."""

from __future__ import annotations

import asyncio
import base64
import json
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from gantry.flink.target import FlinkTarget


@dataclass(frozen=True, slots=True)
class HTTPResponse:
    status: int
    payload: object = None


class HTTPTransport(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> HTTPResponse: ...


class FlinkHTTPError(RuntimeError):
    def __init__(self, status: int | None, message: str, payload: object = None) -> None:
        self.status = status
        self.payload = payload
        super().__init__(message)


class UrllibTransport:
    def __init__(self, ssl_context: ssl.SSLContext | None = None) -> None:
        self._ssl_context = ssl_context

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> HTTPResponse:
        return await asyncio.to_thread(
            self._request_sync,
            method,
            url,
            headers,
            body,
            timeout_seconds,
        )

    def _request_sync(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: Mapping[str, object] | None,
        timeout: float,
    ) -> HTTPResponse:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(url, data=data, headers=dict(headers), method=method)
        try:
            with urlopen(request, timeout=timeout, context=self._ssl_context) as response:
                status = response.status
                raw = response.read()
        except HTTPError as error:
            raw = error.read()
            payload = _decode_payload(raw)
            raise FlinkHTTPError(
                error.code, _error_message(payload, error.reason), payload
            ) from error
        except (URLError, TimeoutError) as error:
            raise FlinkHTTPError(None, f"Flink request failed: {error}") from error
        return HTTPResponse(status=status, payload=_decode_payload(raw))


class FlinkRESTClient:
    """The two Flink REST surfaces required for durable SQL execution."""

    def __init__(self, target: FlinkTarget) -> None:
        self._gateway = target.gateway_endpoint
        self._jobmanager = target.jobmanager_endpoint
        version = target.config.get("api_version", "v2")
        assert isinstance(version, str)
        self._version = version if version.startswith("v") else f"v{version}"
        self._timeout = target.request_timeout
        supplied = target.config.get("transport")
        ssl_context = target.config.get("ssl_context")
        assert ssl_context is None or isinstance(ssl_context, ssl.SSLContext)
        self._transport = (
            cast(HTTPTransport, supplied) if supplied is not None else UrllibTransport(ssl_context)
        )
        self._headers = _authentication_headers(target.config)

    async def open_session(self, properties: Mapping[str, str]) -> str:
        payload = await self._gateway_request(
            "POST", "sessions", body={"sessionName": "gantry", "properties": dict(properties)}
        )
        return _required_string(payload, "sessionHandle", "open session")

    async def close_session(self, session: str) -> None:
        await self._gateway_request("DELETE", f"sessions/{_segment(session)}")

    async def heartbeat(self, session: str) -> None:
        await self._gateway_request("POST", f"sessions/{_segment(session)}/heartbeat", body={})

    async def execute_statement(
        self,
        session: str,
        statement: str,
        *,
        execution_timeout_ms: int,
        execution_config: Mapping[str, str],
    ) -> str:
        payload = await self._gateway_request(
            "POST",
            f"sessions/{_segment(session)}/statements",
            body={
                "statement": statement,
                "executionTimeout": execution_timeout_ms,
                "executionConfig": dict(execution_config),
            },
        )
        return _required_string(payload, "operationHandle", "execute statement")

    async def operation_status(self, session: str, operation: str) -> str:
        payload = await self._gateway_request(
            "GET",
            f"sessions/{_segment(session)}/operations/{_segment(operation)}/status",
        )
        return _required_string(payload, "status", "operation status").upper()

    async def fetch_result(
        self, session: str, operation: str, token: int = 0
    ) -> Mapping[str, object]:
        payload = await self._gateway_request(
            "GET",
            f"sessions/{_segment(session)}/operations/{_segment(operation)}/result/{token}",
            query={"rowFormat": "JSON"},
        )
        return _object(payload, "fetch result")

    async def cancel_operation(self, session: str, operation: str) -> None:
        await self._gateway_request(
            "POST",
            f"sessions/{_segment(session)}/operations/{_segment(operation)}/cancel",
            body={},
        )

    async def close_operation(self, session: str, operation: str) -> None:
        await self._gateway_request(
            "DELETE", f"sessions/{_segment(session)}/operations/{_segment(operation)}"
        )

    async def job_details(self, job_id: str) -> Mapping[str, object]:
        payload = await self._jobmanager_request("GET", f"jobs/{_segment(job_id)}")
        return _object(payload, "job details")

    async def job_exceptions(self, job_id: str) -> Mapping[str, object]:
        payload = await self._jobmanager_request("GET", f"jobs/{_segment(job_id)}/exceptions")
        return _object(payload, "job exceptions")

    async def job_metrics(
        self, job_id: str, names: tuple[str, ...]
    ) -> tuple[Mapping[str, object], ...]:
        payload = await self._jobmanager_request(
            "GET", f"jobs/{_segment(job_id)}/metrics", query={"get": ",".join(names)}
        )
        return _object_list(payload, "job metrics")

    async def vertex_metrics(
        self, job_id: str, vertex_id: str, names: tuple[str, ...]
    ) -> tuple[Mapping[str, object], ...]:
        payload = await self._jobmanager_request(
            "GET",
            f"jobs/{_segment(job_id)}/vertices/{_segment(vertex_id)}/subtasks/metrics",
            query={"get": ",".join(names), "agg": "min,max,sum,avg"},
        )
        return _object_list(payload, "vertex metrics")

    async def cancel_job(self, job_id: str) -> None:
        await self._jobmanager_request(
            "PATCH", f"jobs/{_segment(job_id)}", query={"mode": "cancel"}, body={}
        )

    async def _gateway_request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        return await self._request(
            method, f"{self._gateway}/{self._version}/{path}", query=query, body=body
        )

    async def _jobmanager_request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        return await self._request(method, f"{self._jobmanager}/{path}", query=query, body=body)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        query: Mapping[str, str] | None,
        body: Mapping[str, object] | None,
    ) -> object:
        if query:
            url = f"{url}?{urlencode(query)}"
        response = await self._transport.request(
            method,
            url,
            headers=self._headers,
            body=body,
            timeout_seconds=self._timeout,
        )
        if response.status < 200 or response.status >= 300:
            raise FlinkHTTPError(
                response.status,
                _error_message(response.payload, f"HTTP {response.status}"),
                response.payload,
            )
        return response.payload


def _authentication_headers(config: Mapping[str, object]) -> Mapping[str, str]:
    headers: dict[str, str] = {"Accept": "application/json", "Content-Type": "application/json"}
    supplied = config.get("headers")
    if supplied is not None:
        assert isinstance(supplied, Mapping)
        for name, value in supplied.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("Flink headers must map strings to strings")
            headers[name] = value
    token = config.get("token")
    basic = config.get("basic_auth")
    has_authorization = any(name.lower() == "authorization" for name in headers)
    if not has_authorization and isinstance(token, str):
        headers["Authorization"] = f"Bearer {token}"
    elif not has_authorization and isinstance(basic, tuple):
        username, password = basic
        encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"
    return headers


def _decode_payload(raw: bytes) -> object:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")


def _object(payload: object, operation: str) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise FlinkHTTPError(None, f"Flink {operation} returned an invalid response", payload)
    if not all(isinstance(key, str) for key in payload):
        raise FlinkHTTPError(None, f"Flink {operation} returned non-string keys", payload)
    return cast(Mapping[str, object], payload)


def _object_list(payload: object, operation: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise FlinkHTTPError(None, f"Flink {operation} returned an invalid response", payload)
    return tuple(cast(Mapping[str, object], item) for item in payload)


def _required_string(payload: object, field: str, operation: str) -> str:
    value = _object(payload, operation).get(field)
    if not isinstance(value, str) or not value:
        raise FlinkHTTPError(None, f"Flink {operation} response omitted {field}", payload)
    return value


def _error_message(payload: object, fallback: object) -> str:
    if isinstance(payload, Mapping):
        for key in ("errors", "message", "error"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
            if isinstance(value, list) and value:
                return "; ".join(str(item) for item in value)
    return str(fallback)


def _segment(value: str) -> str:
    return quote(value, safe="")
