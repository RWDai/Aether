from __future__ import annotations

from starlette.requests import Request

from src.utils.request_utils import (
    get_request_id,
    get_request_identity_metadata,
    get_request_metadata,
    update_request_state,
)


def _build_request(headers: dict[str, str] | None = None) -> Request:
    header_items = [
        (str(key).encode("latin-1"), str(value).encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/health",
        "raw_path": b"/health",
        "query_string": b"",
        "headers": header_items,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(scope, receive)


def test_get_request_id_prefers_request_state() -> None:
    request = _build_request(headers={"x-trace-id": "trace-header-123"})
    request.state.request_id = "req-state-123"

    assert get_request_id(request) == "req-state-123"


def test_get_request_id_falls_back_to_trace_header() -> None:
    request = _build_request(headers={"x-trace-id": "trace-header-123"})

    assert get_request_id(request) == "trace-header-123"


def test_get_request_id_returns_none_without_state_or_trace_header() -> None:
    request = _build_request()

    assert get_request_id(request) is None


def test_update_request_state_sets_selected_fields() -> None:
    request = _build_request()

    update_request_state(
        request,
        request_id="req-123",
        user_id="user-123",
        api_key_id="key-123",
        gateway_execution_path="executor_sync",
        rate_limit_scope="user",
    )

    assert request.state.request_id == "req-123"
    assert request.state.user_id == "user-123"
    assert request.state.api_key_id == "key-123"
    assert request.state.gateway_execution_path == "executor_sync"
    assert request.state.rate_limit_scope == "user"


def test_get_request_identity_metadata_reads_request_id_client_ip_and_user_agent() -> None:
    request = _build_request(
        headers={
            "x-trace-id": "trace-header-abc",
            "x-real-ip": "203.0.113.7",
            "user-agent": "pytest-agent",
        }
    )

    meta = get_request_identity_metadata(request)

    assert meta.request_id == "trace-header-abc"
    assert meta.client_ip == "203.0.113.7"
    assert meta.user_agent == "pytest-agent"


def test_get_request_metadata_reuses_identity_fields() -> None:
    request = _build_request(
        headers={
            "x-trace-id": "trace-xyz",
            "x-real-ip": "198.51.100.23",
            "user-agent": "pytest-meta-agent",
            "content-type": "application/json",
            "content-length": "42",
        }
    )

    metadata = get_request_metadata(request)

    assert metadata["request_id"] == "trace-xyz"
    assert metadata["client_ip"] == "198.51.100.23"
    assert metadata["user_agent"] == "pytest-meta-agent"
    assert metadata["method"] == "GET"
    assert metadata["path"] == "/health"
    assert metadata["content_type"] == "application/json"
    assert metadata["content_length"] == "42"
