from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest

import src.api.handlers.base.cli_stream_mixin as cli_stream_mod
import src.api.handlers.base.cli_sync_mixin as cli_sync_mod
import src.services.proxy_node.resolver as proxymod
import src.services.task as taskmod
from src.api.handlers.base.cli_stream_mixin import CliStreamMixin
from src.api.handlers.base.cli_sync_mixin import CliSyncMixin
from src.api.handlers.base.stream_context import StreamContext
from src.core.exceptions import ProviderNotAvailableException
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorStreamResult,
    RustExecutorSyncResult,
)


class _DummyParser:
    def extract_usage_from_response(self, response: dict[str, Any]) -> dict[str, int]:
        del response
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_creation_tokens": 0,
        }

    def extract_text_content(self, response: dict[str, Any]) -> str:
        return str(response.get("id") or "")


class _DummyTelemetry:
    async def record_success(self, **kwargs: Any) -> int:
        del kwargs
        return 0

    async def record_failure(self, **kwargs: Any) -> None:
        del kwargs


class _DummySyncHandler(CliSyncMixin):
    FORMAT_ID = "openai:cli"

    def __init__(self, *, upstream_is_stream: bool = False) -> None:
        self.db = None
        self.redis = None
        self.user = SimpleNamespace(id="user-1")
        self.api_key = SimpleNamespace(id="user-key-1")
        self.request_id = "req-cli-sync"
        self.client_ip = "127.0.0.1"
        self.user_agent = "pytest"
        self.start_time = 0.0
        self.allowed_api_formats = ["openai:cli"]
        self.primary_api_format = "openai:cli"
        self.api_family = None
        self.endpoint_kind = None
        self.telemetry = _DummyTelemetry()
        self.perf_metrics = None
        self._parser = _DummyParser()
        self._upstream_is_stream = upstream_is_stream

    @property
    def parser(self) -> _DummyParser:
        return self._parser

    def _create_pending_usage(self, **kwargs: object) -> bool:
        del kwargs
        return True

    def _build_request_metadata(self, http_request: Any | None = None) -> dict[str, Any]:
        del http_request
        return {}

    def _merge_scheduling_metadata(
        self,
        request_metadata: dict[str, Any] | None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        del kwargs
        return dict(request_metadata or {})

    def _resolve_capability_requirements(
        self,
        model_name: str,
        request_headers: dict[str, str] | None = None,
        request_body: dict[str, Any] | None = None,
    ) -> dict[str, bool]:
        del model_name, request_headers, request_body
        return {}

    async def _resolve_preferred_key_ids(
        self,
        model_name: str,
        request_body: dict[str, Any] | None = None,
    ) -> list[str] | None:
        del model_name, request_body
        return None

    def extract_model_from_request(
        self,
        request_body: dict[str, Any],
        path_params: dict[str, Any] | None = None,
    ) -> str:
        del path_params
        return str(request_body.get("model") or "unknown")

    async def _get_mapped_model(self, source_model: str, provider_id: str) -> str | None:
        del source_model, provider_id
        return None

    async def _build_upstream_request(self, **kwargs: Any) -> Any:
        payload = dict(kwargs["request_body"])
        return SimpleNamespace(
            payload=payload,
            headers={"content-type": "application/json"},
            url="https://upstream.test/v1/responses",
            url_model=str(payload.get("model") or ""),
            envelope=None,
            upstream_is_stream=self._upstream_is_stream,
            tls_profile=None,
            selected_base_url=None,
        )

    def _extract_response_metadata(self, response_json: dict[str, Any]) -> dict[str, Any]:
        return {"id": response_json.get("id")}


class _DummyStreamResponseCtx:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


class _DummyCliStreamHandler(CliStreamMixin):
    FORMAT_ID = "openai:cli"

    def __init__(self, *, upstream_is_stream: bool) -> None:
        self.request_id = "req-cli-stream"
        self.api_key = SimpleNamespace(id="user-key-1")
        self._upstream_is_stream = upstream_is_stream

    async def _get_mapped_model(self, source_model: str, provider_id: str) -> str | None:
        del source_model, provider_id
        return None

    async def _build_upstream_request(self, **kwargs: Any) -> Any:
        payload = dict(kwargs["request_body"])
        return SimpleNamespace(
            payload=payload,
            headers={"content-type": "application/json"},
            url="https://upstream.test/v1/responses",
            url_model=str(payload.get("model") or ""),
            envelope=None,
            upstream_is_stream=self._upstream_is_stream,
            tls_profile=None,
            selected_base_url=None,
        )

    def apply_mapped_model(self, request_body: dict[str, Any], mapped_model: str) -> dict[str, Any]:
        out = dict(request_body)
        out["model"] = mapped_model
        return out

    def _extract_response_metadata(self, response_json: dict[str, Any]) -> dict[str, Any]:
        return {"id": response_json.get("id")}

    def _record_converted_chunks(self, ctx: Any, converted_events: Any) -> None:
        del ctx, converted_events

    def _mark_first_output(self, ctx: Any, output_state: dict[str, Any]) -> None:
        del ctx
        output_state["first_yield"] = False

    async def _prefetch_and_check_embedded_error(
        self,
        byte_iterator: Any,
        provider: Any,
        endpoint: Any,
        ctx: Any,
    ) -> list[bytes]:
        del provider, endpoint, ctx
        first = await anext(byte_iterator)
        return [first]

    async def _create_response_stream_with_prefetch(
        self,
        ctx: Any,
        byte_iterator: Any,
        response_ctx: _DummyStreamResponseCtx,
        prefetched_chunks: list[bytes],
    ) -> AsyncGenerator[bytes]:
        del ctx

        async def _gen() -> AsyncGenerator[bytes]:
            try:
                for chunk in prefetched_chunks:
                    yield chunk
                async for chunk in byte_iterator:
                    yield chunk
            finally:
                await response_ctx.__aexit__(None, None, None)

        return _gen()


async def _iter_chunks(chunks: list[bytes]) -> AsyncGenerator[bytes]:
    for chunk in chunks:
        yield chunk


def _patch_proxy_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(proxymod, "resolve_effective_proxy", lambda provider_proxy, key_proxy=None: None)
    monkeypatch.setattr(proxymod, "get_proxy_label", lambda proxy_info: "direct")

    async def _fake_resolve_proxy_info(proxy_config: Any) -> Any:
        del proxy_config
        return None

    async def _fake_resolve_delegate(proxy_config: Any) -> Any:
        del proxy_config
        return None

    async def _fake_build_proxy_url(proxy_config: Any) -> Any:
        del proxy_config
        return None

    monkeypatch.setattr(proxymod, "resolve_proxy_info_async", _fake_resolve_proxy_info)
    monkeypatch.setattr(proxymod, "resolve_delegate_config_async", _fake_resolve_delegate)
    monkeypatch.setattr(proxymod, "build_proxy_url_async", _fake_build_proxy_url)


@pytest.mark.asyncio
async def test_cli_process_sync_uses_rust_executor_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummySyncHandler()
    monkeypatch.setattr(cli_sync_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

    class _FakeTaskService:
        def __init__(self, db: Any, redis: Any) -> None:
            del db, redis

        async def execute(self, **kwargs: Any) -> Any:
            candidate = SimpleNamespace(
                request_candidate_id="cand-1",
                mapping_matched_model=None,
                needs_conversion=False,
                output_limit=None,
            )
            provider = SimpleNamespace(
                name="provider",
                id="provider-1",
                provider_type="",
                proxy=None,
                request_timeout=None,
                stream_first_byte_timeout=None,
            )
            endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli")
            key = SimpleNamespace(id="key-1", api_key="sk-test", proxy=None)
            response = await kwargs["request_func"](provider, endpoint, key, candidate)
            return SimpleNamespace(
                response=response,
                provider_name="provider",
                request_candidate_id="cand-1",
                provider_id="provider-1",
                endpoint_id="endpoint-1",
                key_id="key-1",
                pool_summary=None,
            )

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        assert getattr(plan, "provider_api_format") == "openai:cli"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "resp-rust-cli"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(taskmod, "TaskService", _FakeTaskService)
    monkeypatch.setattr(
        cli_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await handler.process_sync(
        original_request_body={"model": "gpt-4.1", "input": "hello"},
        original_headers={},
    )

    assert response.status_code == 200
    assert json.loads(response.body) == {"id": "resp-rust-cli"}


@pytest.mark.asyncio
async def test_cli_process_sync_aggregates_upstream_stream_after_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummySyncHandler(upstream_is_stream=True)
    monkeypatch.setattr(cli_sync_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

    class _FakeTaskService:
        def __init__(self, db: Any, redis: Any) -> None:
            del db, redis

        async def execute(self, **kwargs: Any) -> Any:
            candidate = SimpleNamespace(
                request_candidate_id="cand-1",
                mapping_matched_model=None,
                needs_conversion=False,
                output_limit=None,
            )
            provider = SimpleNamespace(
                name="provider",
                id="provider-1",
                provider_type="",
                proxy=None,
                request_timeout=None,
                stream_first_byte_timeout=None,
            )
            endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli")
            key = SimpleNamespace(id="key-1", api_key="sk-test", proxy=None)
            response = await kwargs["request_func"](provider, endpoint, key, candidate)
            return SimpleNamespace(
                response=response,
                provider_name="provider",
                request_candidate_id="cand-1",
                provider_id="provider-1",
                endpoint_id="endpoint-1",
                key_id="key-1",
                pool_summary=None,
            )

    class _FakeNormalizer:
        def response_from_internal(self, response: Any, *, requested_model: str) -> dict[str, Any]:
            return {
                "aggregated": True,
                "requested_model": requested_model,
                "internal_id": response.id,
            }

    class _FakeRegistry:
        def get_normalizer(self, format_id: str) -> _FakeNormalizer:
            assert format_id == "openai:cli"
            return _FakeNormalizer()

    captured_chunks: list[bytes] = []

    async def _fake_aggregate(
        byte_iter: object,
        *,
        provider_api_format: str,
        provider_name: str,
        model: str,
        request_id: str,
        envelope: object = None,
        provider_parser: object = None,
    ) -> object:
        del envelope, provider_parser
        async for chunk in byte_iter:  # type: ignore[attr-defined]
            captured_chunks.append(chunk)
        assert provider_api_format == "openai:cli"
        assert provider_name == "provider"
        assert model == "gpt-4.1"
        assert request_id == "req-cli-sync"
        return SimpleNamespace(id="agg-cli-1")

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        assert getattr(plan, "provider_api_format") == "openai:cli"
        assert getattr(plan, "stream") is True
        return RustExecutorSyncResult(
            status_code=200,
            response_body_bytes=b"data: {\"id\":\"chunk-1\"}\n\ndata: [DONE]\n\n",
            headers={"content-type": "text/event-stream"},
        )

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(taskmod, "TaskService", _FakeTaskService)
    monkeypatch.setattr(
        cli_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(cli_sync_mod, "get_format_converter_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(
        cli_sync_mod,
        "aggregate_upstream_stream_to_internal_response",
        _fake_aggregate,
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    response = await handler.process_sync(
        original_request_body={"model": "gpt-4.1", "input": "hello"},
        original_headers={},
    )

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "aggregated": True,
        "requested_model": "gpt-4.1",
        "internal_id": "agg-cli-1",
    }
    assert captured_chunks == [b"data: {\"id\":\"chunk-1\"}\n\ndata: [DONE]\n\n"]


@pytest.mark.asyncio
async def test_cli_process_sync_raises_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummySyncHandler()
    monkeypatch.setattr(cli_sync_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

    class _FakeTaskService:
        def __init__(self, db: Any, redis: Any) -> None:
            del db, redis

        async def execute(self, **kwargs: Any) -> Any:
            candidate = SimpleNamespace(
                request_candidate_id="cand-1",
                mapping_matched_model=None,
                needs_conversion=False,
                output_limit=None,
            )
            provider = SimpleNamespace(
                name="provider",
                id="provider-1",
                provider_type="",
                proxy=None,
                request_timeout=None,
                stream_first_byte_timeout=None,
            )
            endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli")
            key = SimpleNamespace(id="key-1", api_key="sk-test", proxy=None)
            await kwargs["request_func"](provider, endpoint, key, candidate)
            raise AssertionError("task service should not reach Python local execution")

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        del self, plan
        raise RustExecutorClientError("executor down")

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(taskmod, "TaskService", _FakeTaskService)
    monkeypatch.setattr(
        cli_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await handler.process_sync(
            original_request_body={"model": "gpt-4.1", "input": "hello"},
            original_headers={},
        )

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == "executor down"


@pytest.mark.asyncio
async def test_cli_process_sync_raises_when_remote_contract_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummySyncHandler()
    monkeypatch.setattr(cli_sync_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(cli_sync_mod, "is_remote_contract_eligible", lambda plan: False)
    _patch_proxy_resolver(monkeypatch)

    class _FakeTaskService:
        def __init__(self, db: Any, redis: Any) -> None:
            del db, redis

        async def execute(self, **kwargs: Any) -> Any:
            candidate = SimpleNamespace(
                request_candidate_id="cand-1",
                mapping_matched_model=None,
                needs_conversion=False,
                output_limit=None,
            )
            provider = SimpleNamespace(
                name="provider",
                id="provider-1",
                provider_type="",
                proxy=None,
                request_timeout=None,
                stream_first_byte_timeout=None,
            )
            endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli")
            key = SimpleNamespace(id="key-1", api_key="sk-test", proxy=None)
            await kwargs["request_func"](provider, endpoint, key, candidate)
            raise AssertionError("task service should not complete after local upstream attempt")

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        raise AssertionError("rust executor should not be used when contract is ineligible")

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(taskmod, "TaskService", _FakeTaskService)
    monkeypatch.setattr(
        cli_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await handler.process_sync(
            original_request_body={"model": "gpt-4.1", "input": "hello"},
            original_headers={},
        )

    assert exc_info.value.message == "CLI 请求暂不支持当前 Rust executor 契约"
    assert exc_info.value.upstream_response == "remote_contract_ineligible"


@pytest.mark.asyncio
async def test_cli_execute_stream_request_uses_rust_sync_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummyCliStreamHandler(upstream_is_stream=False)
    ctx = StreamContext(model="gpt-test", api_format="openai:cli")
    ctx.client_api_format = "openai:cli"

    monkeypatch.setattr(cli_stream_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        assert getattr(plan, "stream") is False
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "sync-bridge-rust"},
            headers={"content-type": "application/json"},
        )

    async def _fake_streamify(**kwargs: Any) -> AsyncGenerator[bytes]:
        assert kwargs["response_json"] == {"id": "sync-bridge-rust"}
        yield b"data: cli-bridge\n\n"

    monkeypatch.setattr(
        cli_stream_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(handler, "_streamify_sync_response", _fake_streamify)

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
        stream_first_byte_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None, auth_type="", api_key="sk-test")
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    stream = await handler._execute_stream_request(
        ctx,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "input": "hello"},
        {},
        candidate=candidate,
    )
    if hasattr(stream, "__await__"):
        stream = await stream
    chunks = [chunk async for chunk in stream]

    assert chunks == [b"data: cli-bridge\n\n"]


@pytest.mark.asyncio
@pytest.mark.parametrize("upstream_is_stream", [False, True])
async def test_cli_execute_stream_request_raises_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    upstream_is_stream: bool,
) -> None:
    handler = _DummyCliStreamHandler(upstream_is_stream=upstream_is_stream)
    ctx = StreamContext(model="gpt-test", api_format="openai:cli")
    ctx.client_api_format = "openai:cli"

    monkeypatch.setattr(cli_stream_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
        stream_first_byte_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None, api_key="sk-test", auth_type="")
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    if upstream_is_stream:
        async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
            del self, plan
            raise RustExecutorClientError("executor down")

        monkeypatch.setattr(
            cli_stream_mod.RustExecutorClient,
            "execute_stream",
            _fake_execute_stream,
        )
    else:
        async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
            del self, plan
            raise RustExecutorClientError("executor down")

        monkeypatch.setattr(
            cli_stream_mod.RustExecutorClient,
            "execute_sync_json",
            _fake_execute_sync_json,
        )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        stream = await handler._execute_stream_request(
            ctx,
            provider,
            endpoint,
            key,
            {"model": "gpt-test", "input": "hello"},
            {},
            candidate=candidate,
        )
        if hasattr(stream, "__await__"):
            stream = await stream
        _ = [chunk async for chunk in stream]

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == "executor down"


@pytest.mark.asyncio
@pytest.mark.parametrize("upstream_is_stream", [False, True])
async def test_cli_execute_stream_request_raises_when_remote_contract_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
    upstream_is_stream: bool,
) -> None:
    handler = _DummyCliStreamHandler(upstream_is_stream=upstream_is_stream)
    ctx = StreamContext(model="gpt-test", api_format="openai:cli")
    ctx.client_api_format = "openai:cli"

    monkeypatch.setattr(cli_stream_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(cli_stream_mod, "is_remote_contract_eligible", lambda plan: False)
    _patch_proxy_resolver(monkeypatch)

    async def _fake_get_upstream_client(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("python fallback should not be used")

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        raise AssertionError("rust sync executor should not be used when contract is ineligible")

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        raise AssertionError("rust stream executor should not be used when contract is ineligible")

    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_upstream_client",
        _fake_get_upstream_client,
    )
    monkeypatch.setattr(
        cli_stream_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(
        cli_stream_mod.RustExecutorClient,
        "execute_stream",
        _fake_execute_stream,
    )

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
        stream_first_byte_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None, api_key="sk-test", auth_type="")
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        stream = await handler._execute_stream_request(
            ctx,
            provider,
            endpoint,
            key,
            {"model": "gpt-test", "input": "hello"},
            {},
            candidate=candidate,
        )
        if hasattr(stream, "__await__"):
            stream = await stream
        _ = [chunk async for chunk in stream]

    assert exc_info.value.message == "CLI 请求暂不支持当前 Rust executor 契约"
    assert exc_info.value.upstream_response == "remote_contract_ineligible"


@pytest.mark.asyncio
async def test_cli_execute_stream_request_uses_rust_native_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _DummyCliStreamHandler(upstream_is_stream=True)
    ctx = StreamContext(model="gpt-test", api_format="openai:cli")
    ctx.client_api_format = "openai:cli"

    monkeypatch.setattr(cli_stream_mod.config, "executor_backend", "rust")
    _patch_proxy_resolver(monkeypatch)

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
            response_ctx=_DummyStreamResponseCtx(),
        )

    monkeypatch.setattr(
        cli_stream_mod.RustExecutorClient,
        "execute_stream",
        _fake_execute_stream,
    )

    provider = SimpleNamespace(
        name="provider",
        id="provider-1",
        provider_type="",
        proxy=None,
        request_timeout=None,
        stream_first_byte_timeout=None,
    )
    endpoint = SimpleNamespace(id="endpoint-1", api_format="openai:cli", base_url="https://x")
    key = SimpleNamespace(id="key-1", proxy=None, api_key="sk-test")
    candidate = SimpleNamespace(
        request_candidate_id="cand-1",
        mapping_matched_model=None,
        needs_conversion=False,
        output_limit=None,
    )

    stream = await handler._execute_stream_request(
        ctx,
        provider,
        endpoint,
        key,
        {"model": "gpt-test", "input": "hello"},
        {},
        candidate=candidate,
    )
    if hasattr(stream, "__await__"):
        stream = await stream
    chunks = [chunk async for chunk in stream]

    assert chunks == [
        b"data: {\"id\":\"chunk-1\"}\n\n",
        b"data: [DONE]\n\n",
    ]
    assert ctx.status_code == 200
    assert ctx.response_headers["x-upstream-test"] == "true"
