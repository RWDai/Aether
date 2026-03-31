from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.api.handlers.base.endpoint_checker import EndpointCheckRequest, HttpRequestExecutor
from src.services.request.rust_executor_client import (
    RustExecutorStreamResult,
    RustExecutorSyncResult,
)


class _DummyStreamContext:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_endpoint_checker_sync_prefers_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as mod
    from src.services.request import rust_executor_client as rust_mod

    monkeypatch.setattr(mod.config, "executor_backend", "rust")

    executor = HttpRequestExecutor(timeout=15.0)
    proxy_snapshot = object()
    monkeypatch.setattr(
        executor,
        "_build_rust_proxy_snapshot",
        AsyncMock(return_value=proxy_snapshot),
    )

    captured: dict[str, Any] = {}

    async def _fake_execute_sync_json(
        self: object,
        plan: Any,
    ) -> RustExecutorSyncResult:
        captured["plan"] = plan
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "resp_1", "usage": {"prompt_tokens": 1, "completion_tokens": 2}},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(rust_mod.RustExecutorClient, "execute_sync_json", _fake_execute_sync_json)

    result = await executor.execute(
        EndpointCheckRequest(
            url="https://upstream.test/v1/chat/completions",
            headers={"authorization": "Bearer test"},
            json_body={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
            api_format="openai:chat",
            provider_name="openai",
            model_name="gpt-test",
            api_key_id="key_1",
            provider_id="provider_1",
        )
    )

    assert result.status_code == 200
    assert result.response_data == {
        "id": "resp_1",
        "usage": {"prompt_tokens": 1, "completion_tokens": 2},
    }
    assert captured["plan"].proxy is proxy_snapshot
    assert captured["plan"].method == "POST"
    assert captured["plan"].url == "https://upstream.test/v1/chat/completions"


@pytest.mark.asyncio
async def test_endpoint_checker_stream_prefers_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as mod
    from src.services.request import rust_executor_client as rust_mod

    monkeypatch.setattr(mod.config, "executor_backend", "rust")

    executor = HttpRequestExecutor(timeout=15.0)
    monkeypatch.setattr(
        executor,
        "_build_rust_proxy_snapshot",
        AsyncMock(return_value=None),
    )

    async def _byte_iter() -> Any:
        yield b'data: {"choices":[{"delta":{"content":"Hel'
        yield b'lo"}}]}\n\n'
        yield b'data: {"choices":[{"delta":{"content":" world"},"finish_reason":"stop"}]}\n\n'

    stream_ctx = _DummyStreamContext()

    async def _fake_execute_stream(self: object, plan: Any) -> RustExecutorStreamResult:
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "text/event-stream"},
            byte_iterator=_byte_iter(),
            response_ctx=stream_ctx,
        )

    monkeypatch.setattr(rust_mod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    result = await executor.execute(
        EndpointCheckRequest(
            url="https://upstream.test/v1/chat/completions",
            headers={"authorization": "Bearer test"},
            json_body={
                "model": "gpt-test",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
            api_format="openai:chat",
            provider_name="openai",
            model_name="gpt-test",
            is_stream=True,
        )
    )

    assert result.status_code == 200
    assert result.response_data == {
        "choices": [{"delta": {"content": " world"}, "finish_reason": "stop"}]
    }
    assert stream_ctx.closed is True


@pytest.mark.asyncio
async def test_endpoint_checker_proxy_snapshot_falls_back_to_system_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.services.proxy_node import resolver as resolver_mod

    executor = HttpRequestExecutor()

    monkeypatch.setattr(
        resolver_mod,
        "get_system_proxy_config_async",
        AsyncMock(return_value={"enabled": True, "url": "http://system-proxy.test:8080"}),
    )
    monkeypatch.setattr(
        resolver_mod,
        "resolve_delegate_config_async",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        resolver_mod,
        "build_proxy_url_async",
        AsyncMock(return_value="http://system-proxy.test:8080"),
    )
    monkeypatch.setattr(
        resolver_mod,
        "resolve_proxy_info_async",
        AsyncMock(return_value={"mode": "http", "label": "system-proxy"}),
    )

    snapshot = await executor._build_rust_proxy_snapshot(None)

    assert snapshot is not None
    assert snapshot.enabled is True
    assert snapshot.url == "http://system-proxy.test:8080"
    assert snapshot.mode == "http"


@pytest.mark.asyncio
async def test_endpoint_checker_returns_503_when_rust_executor_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as mod

    monkeypatch.setattr(mod.config, "executor_backend", "python")

    executor = HttpRequestExecutor(timeout=5.0)
    result = await executor.execute(
        EndpointCheckRequest(
            url="https://upstream.test/v1/chat/completions",
            headers={"authorization": "Bearer test"},
            json_body={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
            api_format="openai:chat",
            provider_name="openai",
            model_name="gpt-test",
        )
    )

    assert result.status_code == 503
    assert result.error_message == "端点检查仅支持 Rust executor"
