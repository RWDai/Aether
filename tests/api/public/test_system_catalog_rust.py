from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import HTTPException


def _build_provider_fixture() -> SimpleNamespace:
    endpoint = SimpleNamespace(
        id="endpoint_1",
        base_url="https://upstream.test",
        api_format="openai:chat",
        is_active=True,
    )
    key = SimpleNamespace(
        id="key_1",
        is_active=True,
        api_formats=None,
    )
    return SimpleNamespace(
        id="provider_1",
        name="openai",
        endpoints=[endpoint],
        api_keys=[key],
    )


@pytest.mark.asyncio
async def test_test_connection_prefers_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.public import system_catalog as mod

    provider = _build_provider_fixture()

    monkeypatch.setattr(mod, "_select_provider", lambda _db, _provider_name: provider)
    monkeypatch.setattr(mod, "build_test_request_body", lambda *_args, **_kwargs: {"model": "gpt-test"})
    monkeypatch.setattr(mod, "get_provider_auth", AsyncMock(return_value=None))

    class _DummyBuilder:
        def build(self, *_args: object, **_kwargs: object) -> tuple[dict[str, str], dict[str, str]]:
            return {"model": "gpt-test"}, {"authorization": "Bearer test"}

    monkeypatch.setattr(mod, "PassthroughRequestBuilder", lambda: _DummyBuilder())
    monkeypatch.setattr(
        mod,
        "build_provider_url",
        lambda *_args, **_kwargs: "https://upstream.test/v1/chat/completions",
    )

    proxy_snapshot = object()
    monkeypatch.setattr(
        mod,
        "_build_test_connection_transport_context",
        AsyncMock(return_value=({"enabled": True}, {"node_id": "node-1", "tunnel": True}, proxy_snapshot)),
    )

    rust_response = httpx.Response(
        200,
        request=httpx.Request("POST", "https://upstream.test/v1/chat/completions"),
        json={"id": "resp_rust"},
    )
    rust_call = AsyncMock(return_value=rust_response)
    monkeypatch.setattr(mod, "_try_rust_test_connection_response", rust_call)

    result = await mod._test_connection_response(
        request=SimpleNamespace(query_params={}),
        db=MagicMock(),
        provider=None,
        model="gpt-test",
        api_format=None,
    )

    assert result["status"] == "success"
    assert result["response_id"] == "resp_rust"
    rust_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_test_connection_returns_503_when_rust_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.public import system_catalog as mod

    provider = _build_provider_fixture()

    monkeypatch.setattr(mod, "_select_provider", lambda _db, _provider_name: provider)
    monkeypatch.setattr(mod, "build_test_request_body", lambda *_args, **_kwargs: {"model": "gpt-test"})
    monkeypatch.setattr(mod, "get_provider_auth", AsyncMock(return_value=None))

    class _DummyBuilder:
        def build(self, *_args: object, **_kwargs: object) -> tuple[dict[str, str], dict[str, str]]:
            return {"model": "gpt-test"}, {"authorization": "Bearer test"}

    monkeypatch.setattr(mod, "PassthroughRequestBuilder", lambda: _DummyBuilder())
    monkeypatch.setattr(
        mod,
        "build_provider_url",
        lambda *_args, **_kwargs: "https://upstream.test/v1/chat/completions",
    )

    monkeypatch.setattr(
        mod,
        "_build_test_connection_transport_context",
        AsyncMock(return_value=({"enabled": True}, {"node_id": "node-1", "tunnel": True}, None)),
    )
    monkeypatch.setattr(
        mod,
        "_try_rust_test_connection_response",
        AsyncMock(
            side_effect=HTTPException(
                status_code=503,
                detail="System catalog test-connection requires Rust executor",
            )
        ),
    )
    with pytest.raises(HTTPException) as exc_info:
        await mod._test_connection_response(
            request=SimpleNamespace(query_params={}),
            db=MagicMock(),
            provider=None,
            model="gpt-test",
            api_format=None,
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "System catalog test-connection requires Rust executor"
