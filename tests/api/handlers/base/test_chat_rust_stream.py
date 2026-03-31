from __future__ import annotations

from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import src.api.handlers.base.chat_handler_base as chatmod
import src.services.proxy_node.resolver as proxymod
from src.api.handlers.base.chat_handler_base import ChatHandlerBase
from src.api.handlers.base.stream_context import StreamContext
from src.core.exceptions import ProviderNotAvailableException
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorStreamResult,
)


class _DummyAuthInfo:
    auth_header = "authorization"
    auth_value = "Bearer test"
    decrypted_auth_config = None

    def as_tuple(self) -> tuple[str, str]:
        return self.auth_header, self.auth_value


class _PassBuilder:
    def build(self, request_body: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
        return request_body, {"content-type": "application/json"}


class _DummyStreamResponseCtx:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


class _FakeStreamProcessor:
    def __init__(self) -> None:
        self.prefetched_chunks: list[bytes] | None = None
        self.response_ctx: _DummyStreamResponseCtx | None = None

    async def prefetch_and_check_error(
        self,
        byte_iterator: Any,
        provider: Any,
        endpoint: Any,
        ctx: Any,
        max_prefetch_lines: int = 5,
        max_prefetch_bytes: int = 65536,
    ) -> list[bytes]:
        del provider, endpoint, ctx, max_prefetch_lines, max_prefetch_bytes
        first = await anext(byte_iterator)
        self.prefetched_chunks = [first]
        return self.prefetched_chunks

    async def create_response_stream(
        self,
        ctx: Any,
        byte_iterator: Any,
        response_ctx: _DummyStreamResponseCtx,
        prefetched_chunks: list[bytes] | None = None,
        *,
        start_time: float | None = None,
    ) -> AsyncGenerator[bytes]:
        del ctx, start_time
        self.response_ctx = response_ctx
        try:
            for chunk in prefetched_chunks or []:
                yield chunk
            async for chunk in byte_iterator:
                yield chunk
        finally:
            await response_ctx.__aexit__(None, None, None)


class _FakeParser:
    def is_error_response(self, response_json: dict[str, Any]) -> bool:
        del response_json
        return False


class _FakeInternalUsage:
    input_tokens = 3
    output_tokens = 5
    cache_read_tokens = 1
    cache_write_tokens = 0


class _FakeInternalResponse:
    def __init__(self) -> None:
        self.id = "resp-sync"
        self.model = ""
        self.usage = _FakeInternalUsage()


class _FakeSourceNormalizer:
    def response_to_internal(self, response_json: dict[str, Any]) -> _FakeInternalResponse:
        assert response_json == {"id": "sync-1", "message": "hello"}
        return _FakeInternalResponse()


class _FakeTargetNormalizer:
    def stream_event_from_internal(
        self,
        event: dict[str, Any],
        state: Any,
    ) -> list[dict[str, Any]]:
        assert event == {"kind": "chunk"}
        assert getattr(state, "message_id", "") == "resp-sync"
        return [{"delta": "hello"}]


class _FakeRegistry:
    def get_normalizer(self, format_id: str) -> Any:
        if format_id == "provider:test":
            return _FakeSourceNormalizer()
        if format_id == "openai:chat":
            return _FakeTargetNormalizer()
        raise AssertionError(f"unexpected format: {format_id}")


class _DummyChatHandler(ChatHandlerBase):
    FORMAT_ID = "openai:chat"

    def __init__(self) -> None:
        self.request_id = "req-test"
        self.api_key = SimpleNamespace(id="user-key-1")
        self._request_builder = _PassBuilder()
        self.allowed_api_formats = ["openai:chat"]
        self.api_family = None
        self.endpoint_kind = None
        self.start_time = 0.0

    async def _convert_request(self, request: Any) -> Any:
        return request

    def _extract_usage(self, response: dict) -> dict[str, int]:
        return {}

    async def _get_mapped_model(
        self,
        source_model: str,
        provider_id: str,
        api_format: str | None = None,
    ) -> str | None:
        del source_model, provider_id, api_format
        return None

    def apply_mapped_model(self, request_body: dict[str, Any], mapped_model: str) -> dict[str, Any]:
        out = dict(request_body)
        out["model"] = mapped_model
        return out

    def prepare_provider_request_body(self, request_body: dict[str, Any]) -> dict[str, Any]:
        return dict(request_body)

    def finalize_provider_request(
        self,
        request_body: dict[str, Any],
        *,
        mapped_model: str | None,
        provider_api_format: str | None,
    ) -> dict[str, Any]:
        del mapped_model, provider_api_format
        return dict(request_body)

    def get_model_for_url(
        self,
        request_body: dict[str, Any],
        mapped_model: str | None,
    ) -> str | None:
        return mapped_model or str(request_body.get("model") or "")


def _patch_stream_setup(
    monkeypatch: pytest.MonkeyPatch,
    *,
    proxy_info: dict[str, Any] | None = None,
    delegate_config: dict[str, Any] | None = None,
) -> None:
    async def _fake_get_provider_auth(endpoint: Any, key: Any) -> _DummyAuthInfo:
        del endpoint, key
        return _DummyAuthInfo()

    async def _fake_resolve_proxy_info(proxy_config: Any) -> Any:
        del proxy_config
        return proxy_info

    async def _fake_resolve_delegate(proxy_config: Any) -> Any:
        del proxy_config
        return delegate_config

    async def _fake_get_system_proxy() -> None:
        return None

    monkeypatch.setattr(chatmod, "get_provider_auth", _fake_get_provider_auth)
    monkeypatch.setattr(
        chatmod,
        "get_provider_behavior",
        lambda **kwargs: SimpleNamespace(
            envelope=None,
            same_format_variant=None,
            cross_format_variant=None,
        ),
    )
    monkeypatch.setattr(chatmod, "build_provider_url", lambda *args, **kwargs: "https://upstream.test/v1/chat/completions")
    monkeypatch.setattr(chatmod, "get_upstream_stream_policy", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chatmod,
        "resolve_upstream_is_stream",
        lambda *, client_is_stream, policy: client_is_stream,
    )
    monkeypatch.setattr(chatmod, "enforce_stream_mode_for_upstream", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chatmod,
        "maybe_patch_request_with_prompt_cache_key",
        lambda request_body, **kwargs: request_body,
    )
    monkeypatch.setattr(proxymod, "resolve_effective_proxy", lambda provider_proxy, key_proxy=None: None)
    monkeypatch.setattr(proxymod, "resolve_proxy_info_async", _fake_resolve_proxy_info)
    monkeypatch.setattr(proxymod, "get_proxy_label", lambda proxy_info: "direct")
    monkeypatch.setattr(proxymod, "resolve_delegate_config_async", _fake_resolve_delegate)
    monkeypatch.setattr(proxymod, "get_system_proxy_config_async", _fake_get_system_proxy)
    monkeypatch.setattr(proxymod, "build_proxy_url_async", _fake_get_system_proxy)


async def _iter_chunks(chunks: list[bytes]) -> AsyncGenerator[bytes]:
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_execute_stream_request_uses_rust_executor_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    dummy_ctx = _DummyStreamResponseCtx()

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "stream") is True
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "text/event-stream", "x-upstream-test": "true"},
            byte_iterator=_iter_chunks(
                [
                    b"data: {\"id\":\"chunk-1\"}\n\n",
                    b"data: [DONE]\n\n",
                ]
            ),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    stream = await handler._execute_stream_request(
        ctx,
        stream_processor,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
        {},
        candidate=candidate,
    )

    received = [chunk async for chunk in stream]

    assert received == [
        b"data: {\"id\":\"chunk-1\"}\n\n",
        b"data: [DONE]\n\n",
    ]
    assert ctx.status_code == 200
    assert ctx.response_headers["x-upstream-test"] == "true"
    assert stream_processor.prefetched_chunks == [b"data: {\"id\":\"chunk-1\"}\n\n"]
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_execute_stream_request_uses_rust_sync_executor_for_non_stream_upstream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    stream_processor.on_streaming_start = None
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    async def _fake_prepare_provider_request(self: object, **kwargs: Any) -> object:
        del self, kwargs
        return chatmod.ProviderRequestResult(
            request_body={"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
            url_model="gpt-test",
            mapped_model=None,
            envelope=None,
            extra_headers={},
            upstream_is_stream=False,
            needs_conversion=False,
            provider_api_format="provider:test",
            client_api_format="openai:chat",
            auth_info=_DummyAuthInfo(),
            tls_profile=None,
        )

    async def _fake_execute_sync_json(self: object, plan: object) -> object:
        del self
        assert getattr(plan, "stream") is False
        return SimpleNamespace(
            status_code=200,
            response_json={"id": "sync-1", "message": "hello"},
            response_body_bytes=None,
            headers={"content-type": "application/json"},
        )

    async def _should_not_call_stream(self: object, plan: object) -> RustExecutorStreamResult:
        del self, plan
        raise AssertionError("stream executor should not be used")

    async def _should_not_get_http_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python upstream client should not be used")

    monkeypatch.setattr(
        _DummyChatHandler,
        "_prepare_provider_request",
        _fake_prepare_provider_request,
    )
    monkeypatch.setattr(chatmod, "get_format_converter_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(
        chatmod,
        "iter_internal_response_as_stream_events",
        lambda internal_resp: [{"kind": "chunk"}],
    )
    monkeypatch.setattr(chatmod, "get_parser_for_format", lambda _format: _FakeParser())
    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_sync_json", _fake_execute_sync_json)
    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _should_not_call_stream)
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _should_not_get_http_client,
    )

    stream = await handler._execute_stream_request(
        ctx,
        stream_processor,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
        {},
        candidate=candidate,
    )

    received = [chunk async for chunk in stream]

    assert received == [
        b'data: {"delta": "hello"}\n\n',
        b"data: [DONE]\n\n",
    ]
    assert ctx.status_code == 200
    assert ctx.input_tokens == 3
    assert ctx.output_tokens == 5
    assert ctx.cached_tokens == 1


@pytest.mark.asyncio
async def test_execute_stream_request_accepts_async_generator_stream_processor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(name="provider", id="provider-1", provider_type="", proxy=None)
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    dummy_ctx = _DummyStreamResponseCtx()

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "stream") is True
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "text/event-stream"},
            byte_iterator=_iter_chunks(
                [
                    b"data: {\"id\":\"chunk-1\"}\n\n",
                    b"data: [DONE]\n\n",
                ]
            ),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    stream = await handler._execute_stream_request(
        ctx,
        stream_processor,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
        {},
        candidate=candidate,
    )

    received = [chunk async for chunk in stream]

    assert received == [
        b"data: {\"id\":\"chunk-1\"}\n\n",
        b"data: [DONE]\n\n",
    ]
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_execute_stream_request_allows_tunnel_delegate_for_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(
        monkeypatch,
        proxy_info={"node_id": "node-1", "node_name": "relay-node", "mode": "tunnel"},
        delegate_config={"tunnel": True, "node_id": "node-1"},
    )
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy={"enabled": True, "node_id": "node-1"},
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    dummy_ctx = _DummyStreamResponseCtx()

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "proxy") is not None
        assert getattr(plan.proxy, "mode") == "tunnel"
        assert getattr(plan.proxy, "node_id") == "node-1"
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "text/event-stream"},
            byte_iterator=_iter_chunks([b"data: [DONE]\n\n"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    stream = await handler._execute_stream_request(
        ctx,
        stream_processor,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
        {},
        candidate=candidate,
    )

    received = [chunk async for chunk in stream]

    assert received == [b"data: [DONE]\n\n"]
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_execute_stream_request_allows_tls_profile_for_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(name="provider", id="provider-1", provider_type="", proxy=None)
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    dummy_ctx = _DummyStreamResponseCtx()

    async def _fake_prepare_provider_request(self: object, **kwargs: Any) -> object:
        del self, kwargs
        return chatmod.ProviderRequestResult(
            request_body={"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
            url_model="gpt-test",
            mapped_model=None,
            envelope=None,
            extra_headers={},
            upstream_is_stream=True,
            needs_conversion=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            auth_info=_DummyAuthInfo(),
            tls_profile="claude_code_nodejs",
        )

    monkeypatch.setattr(
        _DummyChatHandler,
        "_prepare_provider_request",
        _fake_prepare_provider_request,
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "tls_profile") == "claude_code_nodejs"
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "text/event-stream"},
            byte_iterator=_iter_chunks([b"data: [DONE]\n\n"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    stream = await handler._execute_stream_request(
        ctx,
        stream_processor,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
        {},
        candidate=candidate,
    )

    received = [chunk async for chunk in stream]

    assert received == [b"data: [DONE]\n\n"]
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_execute_stream_request_turns_rust_upstream_error_into_http_status_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    stream_processor = _FakeStreamProcessor()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(name="provider", id="provider-1", provider_type="", proxy=None)
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    dummy_ctx = _DummyStreamResponseCtx()

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "stream") is True
        return RustExecutorStreamResult(
            status_code=429,
            headers={"content-type": "application/json"},
            byte_iterator=_iter_chunks([b'{"error":{"message":"slow down"}}']),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await handler._execute_stream_request(
            ctx,
            stream_processor,
            provider,
            endpoint,
            key,
            {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
            {},
            candidate=candidate,
        )

    assert exc_info.value.response.status_code == 429
    assert "slow down" in exc_info.value.upstream_response  # type: ignore[attr-defined]
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_execute_stream_request_raises_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(name="provider", id="provider-1", provider_type="", proxy=None)
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        del plan
        raise RustExecutorClientError("executor down")

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _fake_execute_stream)
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await handler._execute_stream_request(
            ctx,
            object(),
            provider,
            endpoint,
            key,
            {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
            {},
            candidate=candidate,
        )

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == "executor down"


@pytest.mark.asyncio
async def test_execute_stream_request_raises_when_remote_contract_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_stream_setup(monkeypatch)
    monkeypatch.setattr(chatmod.config, "executor_backend", "rust")

    handler = _DummyChatHandler()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    provider = SimpleNamespace(name="provider", id="provider-1", provider_type="", proxy=None)
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:chat", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None)
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    async def _should_not_call_rust(self: object, plan: object) -> RustExecutorStreamResult:
        del self, plan
        raise AssertionError("rust executor should not be called")

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(chatmod, "is_remote_contract_eligible", lambda plan: False)
    monkeypatch.setattr(chatmod.RustExecutorClient, "execute_stream", _should_not_call_rust)
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await handler._execute_stream_request(
            ctx,
            object(),
            provider,
            endpoint,
            key,
            {"model": "gpt-test", "messages": [{"role": "user", "content": "hello"}]},
            {},
            candidate=candidate,
        )

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == (
        "execution contract is not eligible for rust executor"
    )
