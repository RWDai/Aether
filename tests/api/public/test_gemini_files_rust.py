from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import Response
from fastapi.responses import StreamingResponse

import src.api.public.gemini_files as gemini_files_mod
import src.services.proxy_node.resolver as resolver_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.api.public.gemini_files import UpstreamContext
from src.config.settings import config
from src.services.request.executor_plan import ExecutionProxySnapshot
from src.services.request.rust_executor_client import (
    RustExecutorStreamResult,
    RustExecutorSyncResult,
)


class _DummyStreamResponseCtx:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


class _FakeDBContext:
    def __init__(self, db: object) -> None:
        self._db = db

    def __enter__(self) -> object:
        return self._db

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


async def _iter_chunks(chunks: list[bytes]) -> AsyncGenerator[bytes]:
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_enrich_upstream_context_proxy_builds_tunnel_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = UpstreamContext(
        upstream_key="upstream-key",
        base_url="https://generativelanguage.googleapis.com",
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
        provider_proxy={"enabled": True, "node_id": "node-1"},
        key_proxy=None,
    )

    monkeypatch.setattr(
        resolver_mod,
        "resolve_effective_proxy",
        lambda provider_proxy, key_proxy=None: provider_proxy,
    )
    monkeypatch.setattr(
        resolver_mod,
        "get_system_proxy_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        resolver_mod,
        "resolve_delegate_config_async",
        AsyncMock(return_value={"tunnel": True, "node_id": "node-1"}),
    )
    monkeypatch.setattr(
        resolver_mod,
        "resolve_proxy_info_async",
        AsyncMock(return_value={"mode": "tunnel", "node_id": "node-1", "label": "Node 1"}),
    )

    async def _unexpected_build_proxy_url_async(proxy_config: object) -> str:
        raise AssertionError(f"proxy url should not be built for tunnel: {proxy_config!r}")

    monkeypatch.setattr(
        resolver_mod,
        "build_proxy_url_async",
        _unexpected_build_proxy_url_async,
    )

    enriched = await gemini_files_mod._enrich_upstream_context_proxy(ctx)

    assert enriched.proxy_config == {"enabled": True, "node_id": "node-1"}
    assert enriched.delegate_config == {"tunnel": True, "node_id": "node-1"}
    assert enriched.proxy_snapshot is not None
    assert enriched.proxy_snapshot.mode == "tunnel"
    assert enriched.proxy_snapshot.node_id == "node-1"
    assert enriched.proxy_snapshot.url is None


@pytest.mark.asyncio
async def test_proxy_request_passes_proxy_snapshot_to_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "executor_backend", "rust")
    proxy_snapshot = ExecutionProxySnapshot(
        enabled=True,
        mode="http",
        url="http://proxy.local:8080",
    )

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        assert getattr(plan, "method") == "GET"
        assert getattr(plan, "provider_id") == "prov-1"
        assert getattr(plan, "endpoint_id") == "ep-1"
        assert getattr(plan, "proxy").url == "http://proxy.local:8080"
        return RustExecutorSyncResult(
            status_code=200,
            headers={"content-type": "application/json", "x-rust-files": "true"},
            response_json={"files": [{"name": "files/abc"}]},
        )

    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    response = await gemini_files_mod._proxy_request(
        "GET",
        "https://generativelanguage.googleapis.com/v1beta/files",
        {"x-goog-api-key": "upstream-key"},
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
        proxy=proxy_snapshot,
    )

    assert response.status_code == 200
    assert response.headers["x-rust-files"] == "true"
    assert json.loads(response.body) == {"files": [{"name": "files/abc"}]}


@pytest.mark.asyncio
async def test_proxy_request_returns_503_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_try_rust_sync_proxy_request(*args: object, **kwargs: object) -> Response:
        del args, kwargs
        return gemini_files_mod._build_rust_unavailable_response()

    monkeypatch.setattr(
        gemini_files_mod,
        "_try_rust_sync_proxy_request",
        _fake_try_rust_sync_proxy_request,
    )
    response = await gemini_files_mod._proxy_request(
        "GET",
        "https://generativelanguage.googleapis.com/v1beta/files",
        {"x-goog-api-key": "upstream-key"},
        file_key_id="key-1",
        user_id="user-1",
    )

    body = json.loads(response.body)
    assert response.status_code == 503
    assert body["error"]["code"] == 503
    assert body["error"]["status"] == "UNAVAILABLE"


@pytest.mark.asyncio
async def test_download_file_uses_enriched_proxy_snapshot_for_regular_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "executor_backend", "rust")
    dummy_ctx = _DummyStreamResponseCtx()
    raw_ctx = UpstreamContext(
        upstream_key="upstream-key",
        base_url="https://generativelanguage.googleapis.com",
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
    )
    enriched_ctx = UpstreamContext(
        upstream_key="upstream-key",
        base_url="https://generativelanguage.googleapis.com",
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
        proxy_config={"enabled": True, "url": "http://proxy.local:8080"},
        delegate_config=None,
        proxy_snapshot=ExecutionProxySnapshot(
            enabled=True,
            mode="http",
            url="http://proxy.local:8080",
        ),
    )

    monkeypatch.setattr(gemini_files_mod, "_extract_gemini_api_key", lambda request: "client-key")
    monkeypatch.setattr(
        gemini_files_mod,
        "create_session",
        lambda: _FakeDBContext(SimpleNamespace()),
    )
    monkeypatch.setattr(
        gemini_files_mod.AuthService,
        "authenticate_api_key",
        lambda db, key: (SimpleNamespace(id="user-1"), SimpleNamespace(id="user-api-key")),
    )
    monkeypatch.setattr(gemini_files_mod, "_ensure_balance_access", lambda db, user, api_key: None)
    monkeypatch.setattr(
        gemini_files_mod,
        "_resolve_upstream_context",
        AsyncMock(return_value=raw_ctx),
    )
    monkeypatch.setattr(
        gemini_files_mod,
        "_enrich_upstream_context_proxy",
        AsyncMock(return_value=enriched_ctx),
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "method") == "GET"
        assert getattr(plan, "url") == (
            "https://generativelanguage.googleapis.com/v1beta/files/file-1:download?alt=media"
        )
        assert getattr(plan, "headers") == {"x-goog-api-key": "upstream-key"}
        assert getattr(plan, "proxy").url == "http://proxy.local:8080"
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "application/octet-stream", "x-rust-files": "true"},
            byte_iterator=_iter_chunks([b"file-", b"bytes"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_stream",
        _fake_execute_stream,
    )
    response = await gemini_files_mod._download_file_response(
        "file-1",
        SimpleNamespace(
            headers={},
            query_params={"alt": "media"},
        ),
    )

    assert isinstance(response, StreamingResponse)
    assert response.headers["x-rust-files"] == "true"
    body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"file-bytes"
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_download_file_returns_503_when_rust_stream_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "executor_backend", "rust")
    raw_ctx = UpstreamContext(
        upstream_key="upstream-key",
        base_url="https://generativelanguage.googleapis.com",
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
    )
    enriched_ctx = UpstreamContext(
        upstream_key="upstream-key",
        base_url="https://generativelanguage.googleapis.com",
        file_key_id="key-1",
        user_id="user-1",
        provider_id="prov-1",
        endpoint_id="ep-1",
    )

    monkeypatch.setattr(gemini_files_mod, "_extract_gemini_api_key", lambda request: "client-key")
    monkeypatch.setattr(
        gemini_files_mod,
        "create_session",
        lambda: _FakeDBContext(SimpleNamespace()),
    )
    monkeypatch.setattr(
        gemini_files_mod.AuthService,
        "authenticate_api_key",
        lambda db, key: (SimpleNamespace(id="user-1"), SimpleNamespace(id="user-api-key")),
    )
    monkeypatch.setattr(gemini_files_mod, "_ensure_balance_access", lambda db, user, api_key: None)
    monkeypatch.setattr(
        gemini_files_mod,
        "_resolve_upstream_context",
        AsyncMock(return_value=raw_ctx),
    )
    monkeypatch.setattr(
        gemini_files_mod,
        "_enrich_upstream_context_proxy",
        AsyncMock(return_value=enriched_ctx),
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        del self, plan
        raise rust_client_mod.RustExecutorClientError("executor unavailable")

    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_stream",
        _fake_execute_stream,
    )
    response = await gemini_files_mod._download_file_response(
        "file-1",
        SimpleNamespace(
            headers={},
            query_params={"alt": "media"},
        ),
    )

    body = json.loads(response.body)
    assert response.status_code == 503
    assert body["error"]["code"] == 503
    assert body["error"]["status"] == "UNAVAILABLE"
