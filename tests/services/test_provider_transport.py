from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from src.services.provider.transport import build_provider_url


@dataclass
class _DummyEndpoint:
    base_url: str
    api_format: str
    custom_path: str | None = None


def test_gemini_stream_adds_alt_sse_and_drops_key_query_param() -> None:
    endpoint = _DummyEndpoint(
        base_url="https://generativelanguage.googleapis.com",
        api_format="gemini:chat",
    )

    url = build_provider_url(
        endpoint,  # type: ignore[arg-type] - test stub
        query_params={"key": "SECRET"},
        path_params={"model": "gemini-1.5-pro"},
        is_stream=True,
    )

    assert url.startswith(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:streamGenerateContent"
    )
    assert "key=" not in url
    assert "alt=sse" in url


def test_gemini_stream_does_not_override_existing_alt() -> None:
    endpoint = _DummyEndpoint(
        base_url="https://generativelanguage.googleapis.com",
        api_format="gemini:chat",
    )

    url = build_provider_url(
        endpoint,  # type: ignore[arg-type] - test stub
        query_params={"alt": "json"},
        path_params={"model": "gemini-1.5-pro"},
        is_stream=True,
    )

    assert "alt=json" in url
    assert "alt=sse" not in url


def test_gemini_non_stream_does_not_add_alt() -> None:
    endpoint = _DummyEndpoint(
        base_url="https://generativelanguage.googleapis.com",
        api_format="gemini:chat",
    )

    url = build_provider_url(
        endpoint,  # type: ignore[arg-type] - test stub
        path_params={"model": "gemini-1.5-pro"},
        is_stream=False,
    )

    assert url.endswith("/v1beta/models/gemini-1.5-pro:generateContent")
    assert "alt=" not in url


def test_vertex_gemini_api_key_base_url_uses_vertex_transport_without_provider_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = _DummyEndpoint(
        base_url="https://aiplatform.googleapis.com",
        api_format="gemini:chat",
    )
    key = SimpleNamespace(auth_type="api_key", api_key="enc-key")

    from src.core.crypto import crypto_service

    monkeypatch.setattr(crypto_service, "decrypt", lambda value: "test-key")

    url = build_provider_url(
        endpoint,  # type: ignore[arg-type] - test stub
        path_params={"model": "gemini-3.1-pro-preview"},
        is_stream=False,
        key=key,  # type: ignore[arg-type] - test stub
    )

    assert (
        url == "https://aiplatform.googleapis.com/v1/publishers/google/models/"
        "gemini-3.1-pro-preview:generateContent?key=test-key"
    )


def test_vertex_gemini_service_account_base_url_uses_vertex_transport_without_provider_type() -> (
    None
):
    endpoint = _DummyEndpoint(
        base_url="https://aiplatform.googleapis.com",
        api_format="gemini:chat",
    )
    key = SimpleNamespace(auth_type="service_account", auth_config={"project_id": "demo-project"})

    url = build_provider_url(
        endpoint,  # type: ignore[arg-type] - test stub
        path_params={"model": "gemini-3.1-pro-preview"},
        is_stream=False,
        key=key,  # type: ignore[arg-type] - test stub
        decrypted_auth_config={"project_id": "demo-project", "region": "global"},
    )

    assert (
        url == "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/"
        "publishers/google/models/gemini-3.1-pro-preview:generateContent"
    )
