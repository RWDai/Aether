from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from src.core.exceptions import InvalidRequestException
from src.services.provider.adapters.vertex_ai.transport import build_vertex_ai_url


def test_build_vertex_ai_url_uses_express_mode_for_gemini_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = SimpleNamespace(auth_type="api_key", api_key="enc-key")

    from src.core.crypto import crypto_service

    monkeypatch.setattr(crypto_service, "decrypt", lambda value: "test-key")

    url = build_vertex_ai_url(
        SimpleNamespace(),
        is_stream=True,
        effective_query_params={"foo": "bar"},
        path_params={"model": "gemini-2.5-pro"},
        key=key,
    )

    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "aiplatform.googleapis.com"
    assert parsed.path == "/v1/publishers/google/models/gemini-2.5-pro:streamGenerateContent"
    assert parse_qs(parsed.query) == {
        "foo": ["bar"],
        "key": ["test-key"],
        "alt": ["sse"],
    }


def test_build_vertex_ai_url_uses_standard_vertex_path_for_gemini_service_account() -> None:
    key = SimpleNamespace(auth_type="service_account")

    url = build_vertex_ai_url(
        SimpleNamespace(),
        is_stream=False,
        effective_query_params={"foo": "bar", "beta": "1"},
        path_params={"model": "gemini-3.1-pro-preview"},
        key=key,
        decrypted_auth_config={"project_id": "demo-project", "region": "global"},
    )

    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "aiplatform.googleapis.com"
    assert (
        parsed.path
        == "/v1/projects/demo-project/locations/global/publishers/google/models/gemini-3.1-pro-preview:generateContent"
    )
    assert parse_qs(parsed.query) == {"foo": ["bar"]}


def test_build_vertex_ai_url_rejects_claude_api_key() -> None:
    key = SimpleNamespace(auth_type="api_key")

    with pytest.raises(InvalidRequestException, match="Claude 模型"):
        build_vertex_ai_url(
            SimpleNamespace(),
            is_stream=False,
            effective_query_params={},
            path_params={"model": "claude-3-7-sonnet@20250219"},
            key=key,
        )


def test_build_vertex_ai_url_uses_standard_vertex_path_for_claude_service_account() -> None:
    key = SimpleNamespace(auth_type="service_account")

    url = build_vertex_ai_url(
        SimpleNamespace(),
        is_stream=False,
        effective_query_params={"foo": "bar", "beta": "1"},
        path_params={"model": "claude-3-7-sonnet@20250219"},
        key=key,
        decrypted_auth_config={"project_id": "demo-project", "region": "global"},
    )

    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "aiplatform.googleapis.com"
    assert (
        parsed.path
        == "/v1/projects/demo-project/locations/global/publishers/anthropic/models/claude-3-7-sonnet@20250219:rawPredict"
    )
    assert parse_qs(parsed.query) == {"foo": ["bar"]}
