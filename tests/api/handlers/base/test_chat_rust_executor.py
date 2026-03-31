from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

import src.api.handlers.base.chat_sync_executor as chat_sync_mod
from src.api.handlers.base.chat_sync_executor import ChatSyncExecutor
from src.core.exceptions import EmbeddedErrorException
from src.core.exceptions import ProviderNotAvailableException
from src.services.request.executor_plan import (
    ExecutionPlan,
    ExecutionPlanBody,
    PreparedExecutionPlan,
)
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorSyncResult,
)


class _FakeEnvelope:
    name = "fake-envelope"

    def __init__(self) -> None:
        self.status_codes: list[int] = []
        self.postprocessed_payloads: list[dict[str, object]] = []

    def on_http_status(self, *, base_url: str | None, status_code: int) -> None:
        self.status_codes.append(status_code)

    def on_connection_error(self, *, base_url: str | None, exc: Exception) -> None:
        raise AssertionError("connection error hook should not be used in this test")

    def unwrap_response(self, data: dict[str, object]) -> dict[str, object]:
        return dict(data["payload"])  # type: ignore[index]

    def postprocess_unwrapped_response(self, *, model: str, data: dict[str, object]) -> None:
        self.postprocessed_payloads.append(dict(data))


class _FakeNormalizer:
    def response_from_internal(
        self,
        internal_resp: object,
        *,
        requested_model: str,
    ) -> dict[str, object]:
        return {
            "aggregated": True,
            "requested_model": requested_model,
            "internal_id": getattr(internal_resp, "id", "missing"),
        }


def _make_prepared_plan() -> PreparedExecutionPlan:
    return PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-test",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=30.0,
    )


def _make_proxy_prepared_plan() -> PreparedExecutionPlan:
    prepared = _make_prepared_plan()
    prepared.contract.proxy = chat_sync_mod.ExecutionProxySnapshot(
        enabled=True,
        mode="http",
        label="proxy.internal",
        url="http://proxy.internal:8080",
    )
    prepared.proxy_config = {"url": "http://proxy.internal:8080"}
    return prepared


def _make_tunnel_prepared_plan() -> PreparedExecutionPlan:
    prepared = _make_prepared_plan()
    prepared.contract.proxy = chat_sync_mod.ExecutionProxySnapshot(
        enabled=True,
        mode="tunnel",
        node_id="node-1",
        label="relay-node",
    )
    prepared.delegate_config = {"tunnel": True, "node_id": "node-1"}
    prepared.proxy_config = {"node_id": "node-1"}
    return prepared


def _make_upstream_stream_prepared_plan() -> PreparedExecutionPlan:
    prepared = _make_prepared_plan()
    prepared.contract.stream = True
    prepared.upstream_is_stream = True
    return prepared


def _make_tls_prepared_plan() -> PreparedExecutionPlan:
    prepared = _make_prepared_plan()
    prepared.contract.tls_profile = "claude_code_nodejs"
    prepared.provider_type = "claude_code"
    return prepared


def _make_executor() -> ChatSyncExecutor:
    handler = SimpleNamespace(request_id="req-test")
    executor = ChatSyncExecutor(handler)
    executor._ctx.provider_api_format_for_error = "openai:chat"
    executor._ctx.client_api_format_for_error = "openai:chat"
    executor._ctx.needs_conversion_for_error = False
    return executor


@pytest.mark.asyncio
async def test_execute_sync_plan_uses_rust_executor_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.request_id == "req-test"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "chatcmpl-1"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {"id": "chatcmpl-1"}
    assert executor._ctx.status_code == 200
    assert executor._ctx.response_json == {"id": "chatcmpl-1"}


@pytest.mark.asyncio
async def test_execute_sync_plan_allows_supported_proxy_urls_for_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_proxy_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.proxy is not None
        assert plan.proxy.url == "http://proxy.internal:8080"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "chatcmpl-proxy"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {"id": "chatcmpl-proxy"}


@pytest.mark.asyncio
async def test_execute_sync_plan_allows_tunnel_delegate_for_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_tunnel_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.proxy is not None
        assert plan.proxy.mode == "tunnel"
        assert plan.proxy.node_id == "node-1"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "chatcmpl-tunnel"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {"id": "chatcmpl-tunnel"}


@pytest.mark.asyncio
async def test_execute_sync_plan_allows_tls_profile_for_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_tls_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.tls_profile == "claude_code_nodejs"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"id": "chatcmpl-tls"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="claude-3.7-sonnet",
    )

    assert response == {"id": "chatcmpl-tls"}


@pytest.mark.asyncio
async def test_execute_sync_plan_applies_envelope_postprocessing_after_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()
    prepared_plan.envelope = _FakeEnvelope()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"payload": {"id": "wrapped-1", "message": "ok"}},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {"id": "wrapped-1", "message": "ok"}
    assert prepared_plan.envelope.status_codes == [200]
    assert prepared_plan.envelope.postprocessed_payloads == [
        {"id": "wrapped-1", "message": "ok"}
    ]


@pytest.mark.asyncio
async def test_execute_sync_plan_applies_format_conversion_after_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()
    prepared_plan.needs_conversion = True
    prepared_plan.contract.provider_api_format = "gemini:chat"
    prepared_plan.contract.client_api_format = "openai:chat"
    executor._ctx.provider_api_format_for_error = "gemini:chat"
    executor._ctx.client_api_format_for_error = "openai:chat"
    executor._ctx.needs_conversion_for_error = True

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    class _FakeRegistry:
        def convert_response(
            self,
            response_json: dict[str, object],
            source_format: str,
            target_format: str,
            *,
            requested_model: str,
        ) -> dict[str, object]:
            assert source_format == "gemini:chat"
            assert target_format == "openai:chat"
            assert requested_model == "gpt-4.1"
            return {
                "converted": True,
                "source_id": response_json["provider_id"],
            }

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.provider_api_format == "gemini:chat"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"provider_id": "gemini-1"},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(chat_sync_mod, "get_format_converter_registry", lambda: _FakeRegistry())

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {"converted": True, "source_id": "gemini-1"}
    assert executor._ctx.provider_response_json == {"provider_id": "gemini-1"}


@pytest.mark.asyncio
async def test_execute_sync_plan_aggregates_upstream_stream_after_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_upstream_stream_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    class _FakeRegistry:
        def get_normalizer(self, format_id: str) -> _FakeNormalizer:
            assert format_id == "openai:chat"
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
        async for chunk in byte_iter:  # type: ignore[attr-defined]
            captured_chunks.append(chunk)
        assert provider_api_format == "openai:chat"
        assert provider_name == "provider"
        assert model == "gpt-4.1"
        assert request_id == "req-test"
        return SimpleNamespace(id="agg-1")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.stream is True
        return RustExecutorSyncResult(
            status_code=200,
            response_body_bytes=b"data: {\"id\":\"chunk-1\"}\n\ndata: [DONE]\n\n",
            headers={"content-type": "text/event-stream"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )
    monkeypatch.setattr(chat_sync_mod, "get_format_converter_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(
        "src.api.handlers.base.upstream_stream_bridge.aggregate_upstream_stream_to_internal_response",
        _fake_aggregate,
    )

    response = await executor._execute_sync_plan(
        prepared_plan=prepared_plan,
        provider=SimpleNamespace(name="provider"),
        model="gpt-4.1",
    )

    assert response == {
        "aggregated": True,
        "requested_model": "gpt-4.1",
        "internal_id": "agg-1",
    }
    assert captured_chunks == [b"data: {\"id\":\"chunk-1\"}\n\ndata: [DONE]\n\n"]
    assert executor._ctx.status_code == 200


@pytest.mark.asyncio
async def test_execute_sync_plan_turns_rust_http_error_into_httpx_status_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.url.endswith("/chat/completions")
        return RustExecutorSyncResult(
            status_code=429,
            response_json={"error": {"message": "slow down"}},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await executor._execute_sync_plan(
            prepared_plan=prepared_plan,
            provider=SimpleNamespace(name="provider"),
            model="gpt-4.1",
        )

    assert exc_info.value.response.status_code == 429
    assert '"message": "slow down"' in exc_info.value.upstream_response  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_execute_sync_plan_preserves_embedded_error_semantics_from_rust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.provider_api_format == "openai:chat"
        return RustExecutorSyncResult(
            status_code=200,
            response_json={
                "error": {
                    "message": "bad request",
                    "type": "invalid_request_error",
                    "code": 400,
                }
            },
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    with pytest.raises(EmbeddedErrorException) as exc_info:
        await executor._execute_sync_plan(
            prepared_plan=prepared_plan,
            provider=SimpleNamespace(name="provider"),
            model="gpt-4.1",
        )

    assert exc_info.value.error_message == "bad request"
    assert exc_info.value.error_code == 400


@pytest.mark.asyncio
async def test_execute_sync_plan_raises_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _fake_execute_sync_json(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        assert plan.request_id == "req-test"
        raise RustExecutorClientError("executor down")

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _fake_execute_sync_json,
    )

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await executor._execute_sync_plan(
            prepared_plan=prepared_plan,
            provider=SimpleNamespace(name="provider"),
            model="gpt-4.1",
        )

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == "executor down"


@pytest.mark.asyncio
async def test_execute_sync_plan_raises_when_remote_contract_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _make_executor()
    prepared_plan = _make_prepared_plan()
    prepared_plan.contract.proxy = chat_sync_mod.ExecutionProxySnapshot(
        enabled=True,
        mode="tunnel",
        label="relay-node",
    )

    monkeypatch.setattr(chat_sync_mod.config, "executor_backend", "rust")

    async def _should_not_call_rust(self: object, plan: ExecutionPlan) -> RustExecutorSyncResult:
        del self, plan
        raise AssertionError("rust executor should not be called")

    monkeypatch.setattr(
        chat_sync_mod.RustExecutorClient,
        "execute_sync_json",
        _should_not_call_rust,
    )

    assert prepared_plan.remote_eligible is False

    with pytest.raises(ProviderNotAvailableException) as exc_info:
        await executor._execute_sync_plan(
            prepared_plan=prepared_plan,
            provider=SimpleNamespace(name="provider"),
            model="gpt-4.1",
        )

    assert exc_info.value.message == "执行器暂时不可用，请稍后重试"
    assert exc_info.value.upstream_response == (
        "execution contract is not eligible for rust executor"
    )
