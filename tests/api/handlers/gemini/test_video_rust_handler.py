from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.responses import JSONResponse, StreamingResponse

import src.api.handlers.gemini.video_handler as video_mod
import src.services.proxy_node.resolver as resolver_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.api.handlers.gemini.video_handler import GeminiVeoHandler
from src.core.api_format.conversion.internal_video import VideoStatus
from src.core.exceptions import ProviderNotAvailableException
from src.services.request.rust_executor_client import RustExecutorStreamResult


class _DummyStreamResponseCtx:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


async def _iter_chunks(chunks: list[bytes]) -> AsyncGenerator[bytes]:
    for chunk in chunks:
        yield chunk


def _make_handler() -> GeminiVeoHandler:
    return GeminiVeoHandler(
        db=SimpleNamespace(),
        user=SimpleNamespace(id="user-1"),
        api_key=SimpleNamespace(id="api-key-1"),
        request_id="req-gemini-video-test",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
    )


@pytest.mark.asyncio
async def test_handle_create_task_uses_rust_sync_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _make_handler()
    monkeypatch.setattr(video_mod.UsageService, "create_pending_usage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        handler._normalizer,
        "video_request_to_internal",
        lambda body: SimpleNamespace(model=str(body.get("model") or "veo-3")),
    )

    candidate = SimpleNamespace(provider=SimpleNamespace(name="provider-1", id="prov-1"))
    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="gemini",
        endpoint_kind="video",
        base_url="https://generativelanguage.googleapis.com",
        body_rules=None,
    )
    provider_key = SimpleNamespace(id="key-1")

    monkeypatch.setattr(
        handler,
        "_resolve_upstream_key",
        AsyncMock(return_value=("upstream-key", endpoint, provider_key, None)),
    )
    monkeypatch.setattr(
        handler,
        "_build_upstream_url",
        lambda base_url, model: f"https://generativelanguage.googleapis.com/v1beta/models/{model}:predictLongRunning",
    )
    monkeypatch.setattr(
        handler,
        "_build_upstream_headers",
        lambda original_headers, upstream_key, endpoint, auth_info, **kwargs: {
            "x-goog-api-key": upstream_key
        },
    )
    async def _fake_rust_sync(**kwargs: object) -> httpx.Response:
        assert kwargs["method"] == "POST"
        assert kwargs["provider_id"] == "prov-1"
        assert kwargs["endpoint_id"] == "ep-1"
        assert kwargs["key_id"] == "key-1"
        assert kwargs["body"] == {"model": "veo-3", "prompt": "hello"}
        return httpx.Response(
            200,
            request=httpx.Request("POST", str(kwargs["url"])),
            json={"name": "operations/ext-1"},
        )

    monkeypatch.setattr(handler, "_try_rust_sync_http_response", _fake_rust_sync)

    async def _fake_submit_with_failover(**kwargs: object) -> JSONResponse:
        response = await kwargs["submit_func"](candidate)
        assert response.status_code == 200
        assert response.json()["name"] == "operations/ext-1"
        return JSONResponse(status_code=400, content={"error": {"message": "stop"}})

    monkeypatch.setattr(handler, "_submit_with_failover", _fake_submit_with_failover)

    response = await handler.handle_create_task(
        http_request=SimpleNamespace(
            headers={},
            url=SimpleNamespace(scheme="https", netloc="example.com"),
        ),
        original_headers={},
        original_request_body={"model": "veo-3", "prompt": "hello"},
    )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_handle_download_content_uses_rust_executor_with_proxy_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(video_mod.config, "executor_backend", "rust")
    handler = _make_handler()
    dummy_ctx = _DummyStreamResponseCtx()

    monkeypatch.setattr(
        handler,
        "_get_task_by_external_id",
        lambda task_id: SimpleNamespace(
            id=task_id,
            status=VideoStatus.COMPLETED.value,
            video_url="https://storage.example.com/video.mp4",
            video_expires_at=datetime.now(timezone.utc).replace(year=2099),
            model="veo-3",
        ),
    )

    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="gemini",
        endpoint_kind="video",
        base_url="https://generativelanguage.googleapis.com",
        proxy={"enabled": True, "url": "http://proxy.local:8080"},
    )
    key = SimpleNamespace(id="key-1", api_key="encrypted", proxy=None)
    monkeypatch.setattr(handler, "_get_endpoint_and_key", lambda task: (endpoint, key))
    monkeypatch.setattr(video_mod.crypto_service, "decrypt", lambda _: "upstream-key")
    monkeypatch.setattr(
        video_mod,
        "get_provider_auth",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        video_mod,
        "resolve_provider_proxy",
        lambda endpoint, key: {"enabled": True, "url": "http://proxy.local:8080"},
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
        AsyncMock(return_value=None),
    )

    async def _fake_build_proxy_url_async(proxy_config: object) -> str:
        assert proxy_config == {"enabled": True, "url": "http://proxy.local:8080"}
        return "http://proxy.local:8080"

    async def _fake_resolve_proxy_info_async(proxy_config: object) -> dict[str, str]:
        assert proxy_config == {"enabled": True, "url": "http://proxy.local:8080"}
        return {"url": "http://proxy.local:8080"}

    monkeypatch.setattr(resolver_mod, "build_proxy_url_async", _fake_build_proxy_url_async)
    monkeypatch.setattr(
        resolver_mod,
        "resolve_proxy_info_async",
        _fake_resolve_proxy_info_async,
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "method") == "GET"
        assert getattr(plan, "url") == "https://storage.example.com/video.mp4"
        assert getattr(plan, "headers") == {"x-goog-api-key": "upstream-key"}
        assert getattr(plan, "proxy").url == "http://proxy.local:8080"
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "video/mp4", "x-rust-download": "true"},
            byte_iterator=_iter_chunks([b"gemini-", b"video"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(rust_client_mod.RustExecutorClient, "execute_stream", _fake_execute_stream)
    response = await handler.handle_download_content(
        task_id="operations/ext-1",
        http_request=SimpleNamespace(),
        original_headers={},
        query_params=None,
    )

    assert isinstance(response, StreamingResponse)
    body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"gemini-video"
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_handle_download_content_raises_when_rust_backend_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(video_mod.config, "executor_backend", "python")
    handler = _make_handler()

    monkeypatch.setattr(
        handler,
        "_get_task_by_external_id",
        lambda task_id: SimpleNamespace(
            id=task_id,
            status=VideoStatus.COMPLETED.value,
            video_url="https://storage.example.com/video.mp4",
            video_expires_at=datetime.now(timezone.utc).replace(year=2099),
            model="veo-3",
        ),
    )
    monkeypatch.setattr(
        handler,
        "_get_endpoint_and_key",
        lambda task: (
            SimpleNamespace(id="ep-1", provider_id="prov-1", proxy=None),
            SimpleNamespace(id="key-1", api_key=None, proxy=None),
        ),
    )

    with pytest.raises(ProviderNotAvailableException):
        await handler.handle_download_content(
            task_id="operations/ext-1",
            http_request=SimpleNamespace(),
            original_headers={},
            query_params=None,
        )
