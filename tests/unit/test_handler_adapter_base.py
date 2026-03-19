from types import SimpleNamespace
from typing import Any

import pytest

from src.api.handlers.claude.adapter import ClaudeChatAdapter
from src.api.handlers.gemini.adapter import GeminiChatAdapter
from src.core.provider_auth_types import ProviderAuthInfo


@pytest.mark.asyncio
async def test_check_endpoint_rejects_base_url_dict() -> None:
    with pytest.raises(TypeError, match="base_url must be a non-empty string"):
        await ClaudeChatAdapter.check_endpoint(
            client=None,  # type: ignore[arg-type]
            base_url={"base_url": "https://api.anthropic.com"},  # type: ignore[arg-type]
            api_key="test-key",
            request_data={
                "model": "claude-sonnet-4-5-20250929",
                "messages": [{"role": "user", "content": "hello"}],
                "max_tokens": 32,
                "stream": False,
            },
        )


def test_validate_test_base_url_trims_whitespace() -> None:
    assert ClaudeChatAdapter._validate_test_base_url("  https://api.anthropic.com/v1  ") == (
        "https://api.anthropic.com/v1"
    )


@pytest.mark.asyncio
async def test_claude_check_endpoint_passes_original_body_to_body_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.api.handlers.base import request_builder as request_builder_module

    captured: dict[str, Any] = {}

    def fake_apply_body_rules(
        body: dict[str, Any],
        body_rules: list[dict[str, Any]],
        original_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        captured["body_rules"] = body_rules
        captured["original_body"] = original_body
        return body

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["json_body"] = kwargs["json_body"]
        return {"status_code": 200, "json_body": kwargs["json_body"]}

    def fake_build_request_body(
        cls: type[ClaudeChatAdapter],
        request_data: dict[str, Any] | None = None,
        *,
        base_url: str | None = None,
        provider_type: str | None = None,
    ) -> dict[str, Any]:
        del request_data, base_url, provider_type
        return {
            "messages": [{"role": "user", "content": "hello"}],
            "system": "keep",
        }

    monkeypatch.setattr(request_builder_module, "apply_body_rules", fake_apply_body_rules)
    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(
        ClaudeChatAdapter, "build_request_body", classmethod(fake_build_request_body)
    )

    result = await ClaudeChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://api.anthropic.com/v1",
        api_key="test-key",
        request_data={"model": "claude-sonnet-4-5-20250929", "stream": False},
        body_rules=[{"action": "set", "path": "messages", "value": []}],
    )

    assert captured["original_body"] == captured["json_body"]
    assert result["status_code"] == 200
    assert result["json_body"] == captured["json_body"]


@pytest.mark.asyncio
async def test_gemini_check_endpoint_passes_original_body_to_body_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.api.handlers.base import request_builder as request_builder_module

    captured: dict[str, Any] = {}

    def fake_apply_body_rules(
        body: dict[str, Any],
        body_rules: list[dict[str, Any]],
        original_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        captured["body_rules"] = body_rules
        captured["original_body"] = original_body
        return body

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["json_body"] = kwargs["json_body"]
        return {"status_code": 200, "json_body": kwargs["json_body"]}

    def fake_build_request_body(
        cls: type[GeminiChatAdapter], request_data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        _ = cls, request_data
        return {
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "systemInstruction": {"parts": [{"text": "system"}]},
            "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}},
            "generationConfig": {"temperature": 0.1},
        }

    monkeypatch.setattr(request_builder_module, "apply_body_rules", fake_apply_body_rules)
    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(
        GeminiChatAdapter, "build_request_body", classmethod(fake_build_request_body)
    )

    result = await GeminiChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://generativelanguage.googleapis.com",
        api_key="test-key",
        request_data={"model": "gemini-2.5-pro", "stream": False},
        body_rules=[{"action": "drop", "path": "toolConfig"}],
    )

    assert captured["original_body"] == captured["json_body"]
    assert result["status_code"] == 200
    assert result["json_body"] == captured["json_body"]


@pytest.mark.asyncio
async def test_gemini_check_endpoint_uses_provider_transport_for_vertex_ai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.services.provider import auth as provider_auth_module
    from src.services.provider import transport as provider_transport_module

    captured: dict[str, Any] = {}

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["url"] = kwargs["url"]
        captured["headers"] = kwargs["headers"]
        captured["json_body"] = kwargs["json_body"]
        return {"status_code": 200}

    async def fake_get_provider_auth(endpoint: Any, key: Any) -> None:
        captured["provider_auth_endpoint"] = endpoint
        captured["provider_auth_key"] = key
        return None

    def fake_build_provider_url(
        endpoint: Any,
        *,
        query_params: dict[str, Any] | None = None,
        path_params: dict[str, Any] | None = None,
        is_stream: bool = False,
        key: Any = None,
        decrypted_auth_config: dict[str, Any] | None = None,
    ) -> str:
        captured["build_provider_url"] = {
            "endpoint": endpoint,
            "query_params": query_params,
            "path_params": path_params,
            "is_stream": is_stream,
            "key": key,
            "decrypted_auth_config": decrypted_auth_config,
        }
        return (
            "https://aiplatform.googleapis.com/v1/publishers/google/models/"
            "gemini-2.5-pro:streamGenerateContent?key=test-key"
        )

    def fake_build_request_body(
        cls: type[GeminiChatAdapter],
        request_data: dict[str, Any] | None = None,
        *,
        base_url: str | None = None,
        provider_type: str | None = None,
    ) -> dict[str, Any]:
        del cls, request_data, base_url, provider_type
        return {
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.1},
        }

    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(provider_auth_module, "get_provider_auth", fake_get_provider_auth)
    monkeypatch.setattr(provider_transport_module, "build_provider_url", fake_build_provider_url)
    monkeypatch.setattr(
        GeminiChatAdapter, "build_request_body", classmethod(fake_build_request_body)
    )

    endpoint = SimpleNamespace(id="ep-vertex-gemini")
    key = SimpleNamespace(id="key-vertex-gemini", auth_type="api_key")

    result = await GeminiChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://generativelanguage.googleapis.com",
        api_key="test-key",
        request_data={"model": "gemini-2.5-pro", "stream": True},
        provider_type="vertex_ai",
        provider_endpoint=endpoint,
        provider_api_key=key,
        model_name="gemini-2.5-pro",
    )

    assert result["status_code"] == 200
    assert (
        captured["url"] == "https://aiplatform.googleapis.com/v1/publishers/google/models/"
        "gemini-2.5-pro:streamGenerateContent?key=test-key"
    )
    assert captured["build_provider_url"]["path_params"] == {"model": "gemini-2.5-pro"}
    assert captured["build_provider_url"]["is_stream"] is True
    assert captured["build_provider_url"]["key"] is key
    assert captured["headers"] == {}


@pytest.mark.asyncio
async def test_gemini_check_endpoint_infers_vertex_ai_from_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.services.provider import auth as provider_auth_module
    from src.services.provider import transport as provider_transport_module

    captured: dict[str, Any] = {}

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["url"] = kwargs["url"]
        return {"status_code": 200}

    async def fake_get_provider_auth(endpoint: Any, key: Any) -> None:
        captured["provider_auth_endpoint"] = endpoint
        captured["provider_auth_key"] = key
        return None

    def fake_build_provider_url(
        endpoint: Any,
        *,
        query_params: dict[str, Any] | None = None,
        path_params: dict[str, Any] | None = None,
        is_stream: bool = False,
        key: Any = None,
        decrypted_auth_config: dict[str, Any] | None = None,
    ) -> str:
        captured["build_provider_url"] = {
            "endpoint": endpoint,
            "query_params": query_params,
            "path_params": path_params,
            "is_stream": is_stream,
            "key": key,
            "decrypted_auth_config": decrypted_auth_config,
        }
        return (
            "https://aiplatform.googleapis.com/v1/publishers/google/models/"
            "gemini-3.1-pro-preview:generateContent?key=test-key"
        )

    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(provider_auth_module, "get_provider_auth", fake_get_provider_auth)
    monkeypatch.setattr(provider_transport_module, "build_provider_url", fake_build_provider_url)

    endpoint = SimpleNamespace(id="ep-aiplatform", api_format="gemini:chat")
    key = SimpleNamespace(id="key-aiplatform", auth_type="api_key")

    result = await GeminiChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://aiplatform.googleapis.com",
        api_key="test-key",
        request_data={"model": "gemini-3.1-pro-preview", "stream": False},
        provider_endpoint=endpoint,
        provider_api_key=key,
        model_name="gemini-3.1-pro-preview",
    )

    assert result["status_code"] == 200
    assert (
        captured["url"] == "https://aiplatform.googleapis.com/v1/publishers/google/models/"
        "gemini-3.1-pro-preview:generateContent?key=test-key"
    )
    assert captured["build_provider_url"]["path_params"] == {"model": "gemini-3.1-pro-preview"}
    assert captured["build_provider_url"]["is_stream"] is False


@pytest.mark.asyncio
async def test_gemini_check_endpoint_uses_provider_transport_for_vertex_service_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.services.provider import auth as provider_auth_module
    from src.services.provider import transport as provider_transport_module

    captured: dict[str, Any] = {}
    auth_info = ProviderAuthInfo(
        auth_header="Authorization",
        auth_value="Bearer vertex-token",
        decrypted_auth_config={"project_id": "demo-project", "region": "global"},
    )

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["url"] = kwargs["url"]
        captured["headers"] = kwargs["headers"]
        return {"status_code": 200}

    async def fake_get_provider_auth(endpoint: Any, key: Any) -> ProviderAuthInfo:
        captured["provider_auth_endpoint"] = endpoint
        captured["provider_auth_key"] = key
        return auth_info

    def fake_build_provider_url(
        endpoint: Any,
        *,
        query_params: dict[str, Any] | None = None,
        path_params: dict[str, Any] | None = None,
        is_stream: bool = False,
        key: Any = None,
        decrypted_auth_config: dict[str, Any] | None = None,
    ) -> str:
        captured["build_provider_url"] = {
            "endpoint": endpoint,
            "query_params": query_params,
            "path_params": path_params,
            "is_stream": is_stream,
            "key": key,
            "decrypted_auth_config": decrypted_auth_config,
        }
        return (
            "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/"
            "publishers/google/models/gemini-3.1-pro-preview:generateContent"
        )

    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(provider_auth_module, "get_provider_auth", fake_get_provider_auth)
    monkeypatch.setattr(provider_transport_module, "build_provider_url", fake_build_provider_url)

    endpoint = SimpleNamespace(id="ep-vertex-gemini-sa", api_format="gemini:chat")
    key = SimpleNamespace(id="key-vertex-gemini-sa", auth_type="service_account")

    result = await GeminiChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://aiplatform.googleapis.com",
        api_key="ignored",
        request_data={"model": "gemini-3.1-pro-preview", "stream": False},
        provider_endpoint=endpoint,
        provider_api_key=key,
        model_name="gemini-3.1-pro-preview",
    )

    assert result["status_code"] == 200
    assert (
        captured["url"]
        == "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/"
        "publishers/google/models/gemini-3.1-pro-preview:generateContent"
    )
    assert captured["build_provider_url"]["path_params"] == {"model": "gemini-3.1-pro-preview"}
    assert captured["build_provider_url"]["is_stream"] is False
    assert (
        captured["build_provider_url"]["decrypted_auth_config"] == auth_info.decrypted_auth_config
    )
    assert captured["headers"]["Authorization"] == "Bearer vertex-token"


@pytest.mark.asyncio
async def test_claude_check_endpoint_uses_provider_transport_for_vertex_ai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.handlers.base import endpoint_checker as endpoint_checker_module
    from src.services.provider import auth as provider_auth_module
    from src.services.provider import transport as provider_transport_module

    captured: dict[str, Any] = {}
    auth_info = ProviderAuthInfo(
        auth_header="Authorization",
        auth_value="Bearer vertex-token",
        decrypted_auth_config={"project_id": "demo-project", "region": "global"},
    )

    async def fake_run_endpoint_check(**kwargs: Any) -> dict[str, Any]:
        captured["url"] = kwargs["url"]
        captured["headers"] = kwargs["headers"]
        captured["json_body"] = kwargs["json_body"]
        return {"status_code": 200}

    async def fake_get_provider_auth(endpoint: Any, key: Any) -> ProviderAuthInfo:
        captured["provider_auth_endpoint"] = endpoint
        captured["provider_auth_key"] = key
        return auth_info

    def fake_build_provider_url(
        endpoint: Any,
        *,
        query_params: dict[str, Any] | None = None,
        path_params: dict[str, Any] | None = None,
        is_stream: bool = False,
        key: Any = None,
        decrypted_auth_config: dict[str, Any] | None = None,
    ) -> str:
        captured["build_provider_url"] = {
            "endpoint": endpoint,
            "query_params": query_params,
            "path_params": path_params,
            "is_stream": is_stream,
            "key": key,
            "decrypted_auth_config": decrypted_auth_config,
        }
        return (
            "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/"
            "publishers/anthropic/models/claude-3-7-sonnet@20250219:rawPredict"
        )

    def fake_build_request_body(
        cls: type[ClaudeChatAdapter],
        request_data: dict[str, Any] | None = None,
        *,
        base_url: str | None = None,
        provider_type: str | None = None,
    ) -> dict[str, Any]:
        del cls, request_data, base_url, provider_type
        return {
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 32,
        }

    monkeypatch.setattr(endpoint_checker_module, "run_endpoint_check", fake_run_endpoint_check)
    monkeypatch.setattr(provider_auth_module, "get_provider_auth", fake_get_provider_auth)
    monkeypatch.setattr(provider_transport_module, "build_provider_url", fake_build_provider_url)
    monkeypatch.setattr(
        ClaudeChatAdapter, "build_request_body", classmethod(fake_build_request_body)
    )

    endpoint = SimpleNamespace(id="ep-vertex-claude")
    key = SimpleNamespace(id="key-vertex-claude", auth_type="service_account")

    result = await ClaudeChatAdapter.check_endpoint(
        client=None,  # type: ignore[arg-type]
        base_url="https://api.anthropic.com/v1",
        api_key="ignored",
        request_data={"model": "claude-3-7-sonnet@20250219", "stream": False},
        provider_type="vertex_ai",
        provider_endpoint=endpoint,
        provider_api_key=key,
        model_name="claude-3-7-sonnet@20250219",
    )

    assert result["status_code"] == 200
    assert (
        captured["url"]
        == "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/"
        "publishers/anthropic/models/claude-3-7-sonnet@20250219:rawPredict"
    )
    assert captured["build_provider_url"]["path_params"] == {"model": "claude-3-7-sonnet@20250219"}
    assert captured["build_provider_url"]["is_stream"] is False
    assert captured["build_provider_url"]["key"] is key
    assert (
        captured["build_provider_url"]["decrypted_auth_config"] == auth_info.decrypted_auth_config
    )
    assert captured["headers"]["Authorization"] == "Bearer vertex-token"
