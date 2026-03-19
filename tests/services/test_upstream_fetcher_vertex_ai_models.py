from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.core.vertex_auth import VertexAuthService
from src.services.model.upstream_fetcher import (
    EndpointFetchConfig,
    UpstreamModelsFetchContext,
    fetch_models_for_key,
)


@pytest.mark.asyncio
async def test_fetch_models_for_key_vertex_api_key_custom_fetcher() -> None:
    ctx = UpstreamModelsFetchContext(
        provider_type="vertex_ai",
        api_key_value="test-api-key",
        format_to_endpoint={
            "gemini:chat": EndpointFetchConfig(base_url="https://aiplatform.googleapis.com"),
        },
        proxy_config=None,
        auth_config={},
    )

    mocked_models = [
        {
            "id": "gemini-2.5-pro",
            "owned_by": "google",
            "display_name": "Gemini 2.5 Pro",
            "api_format": "gemini:chat",
        }
    ]

    with (
        patch(
            "src.services.provider.adapters.vertex_ai.plugin._fetch_models_from_url",
            AsyncMock(return_value=(mocked_models, None, True)),
        ),
        patch(
            "src.services.proxy_node.resolver.build_proxy_client_kwargs",
            return_value={"timeout": 1.0},
        ),
    ):
        models, errors, ok, meta = await fetch_models_for_key(ctx, timeout_seconds=1.0)

    assert ok is True
    assert errors == []
    assert meta is None
    assert [m.get("id") for m in models] == ["gemini-2.5-pro"]


@pytest.mark.asyncio
async def test_fetch_models_for_key_vertex_service_account_fetches_google_and_claude() -> None:
    auth_config = {
        "project_id": "demo-project",
        "client_email": "svc@example.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n",
        "region": "global",
    }
    ctx = UpstreamModelsFetchContext(
        provider_type="vertex_ai",
        api_key_value="__placeholder__",
        format_to_endpoint={
            "gemini:chat": EndpointFetchConfig(base_url="https://aiplatform.googleapis.com"),
            "claude:chat": EndpointFetchConfig(base_url="https://aiplatform.googleapis.com"),
        },
        proxy_config=None,
        auth_config=auth_config,
    )

    fetch_side_effect = [
        (
            [
                {
                    "id": "gemini-3.1-pro-preview",
                    "owned_by": "google",
                    "display_name": "Gemini 3.1 Pro Preview",
                    "api_format": "gemini:chat",
                }
            ],
            None,
            True,
        ),
        (
            [
                {
                    "id": "claude-3-7-sonnet@20250219",
                    "owned_by": "anthropic",
                    "display_name": "Claude 3.7 Sonnet",
                    "api_format": "claude:chat",
                }
            ],
            None,
            True,
        ),
    ]

    with (
        patch.object(
            VertexAuthService,
            "get_access_token",
            AsyncMock(return_value="ya29.test-token"),
        ),
        patch(
            "src.services.provider.adapters.vertex_ai.plugin._iter_regions",
            return_value=["global"],
        ),
        patch(
            "src.services.provider.adapters.vertex_ai.plugin._fetch_models_from_url",
            AsyncMock(side_effect=fetch_side_effect),
        ),
        patch(
            "src.services.proxy_node.resolver.build_proxy_client_kwargs",
            return_value={"timeout": 1.0},
        ),
    ):
        models, errors, ok, meta = await fetch_models_for_key(ctx, timeout_seconds=1.0)

    assert ok is True
    assert errors == []
    assert meta is None
    ids = {m.get("id") for m in models}
    assert "gemini-3.1-pro-preview" in ids
    assert "claude-3-7-sonnet@20250219" in ids


@pytest.mark.asyncio
async def test_fetch_models_for_key_vertex_api_key_returns_soft_404_when_all_failed() -> None:
    ctx = UpstreamModelsFetchContext(
        provider_type="vertex_ai",
        api_key_value="test-api-key",
        format_to_endpoint={
            "gemini:chat": EndpointFetchConfig(base_url="https://aiplatform.googleapis.com"),
        },
        proxy_config=None,
        auth_config={},
    )

    with (
        patch(
            "src.services.provider.adapters.vertex_ai.plugin._fetch_models_from_url",
            AsyncMock(
                side_effect=[
                    ([], "HTTP 404: not found", False),
                ]
            ),
        ),
        patch(
            "src.services.proxy_node.resolver.build_proxy_client_kwargs",
            return_value={"timeout": 1.0},
        ),
    ):
        models, errors, ok, meta = await fetch_models_for_key(ctx, timeout_seconds=1.0)

    assert ok is False
    assert models == []
    assert meta is None
    assert errors
    assert "HTTP 404" in errors[0]
