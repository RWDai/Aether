from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.database import get_db


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pipeline_result: Any,
) -> tuple[TestClient, list[dict[str, Any]]]:
    from src.api.public import system_catalog as mod

    app = FastAPI()
    app.include_router(mod.router)
    app.dependency_overrides[get_db] = lambda: MagicMock()

    calls: list[dict[str, Any]] = []

    async def _fake_pipeline_run(
        *,
        adapter: Any,
        http_request: object,
        db: object,
        mode: object,
        api_format_hint: str | None = None,
        path_params: dict[str, Any] | None = None,
    ) -> Any:
        del http_request, db, api_format_hint, path_params
        calls.append(
            {
                "adapter_type": type(adapter).__name__,
                "mode": getattr(mode, "value", mode),
                "adapter_state": dict(getattr(adapter, "__dict__", {})),
            }
        )
        return pipeline_result

    monkeypatch.setattr(mod.pipeline, "run", _fake_pipeline_run)
    return TestClient(app), calls


def test_system_catalog_health_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"status": "ok"})

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert calls == [
        {
            "adapter_type": "PublicServiceHealthAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_system_catalog_simple_health_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"status": "healthy"})

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
    assert calls == [
        {
            "adapter_type": "PublicSimpleHealthCheckAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_system_catalog_root_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"status": "running"})

    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"status": "running"}
    assert calls == [
        {
            "adapter_type": "PublicRootCatalogAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_system_catalog_provider_list_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"providers": []})

    response = client.get("/v1/providers?include_models=true&include_endpoints=true&active_only=false")

    assert response.status_code == 200
    assert response.json() == {"providers": []}
    assert calls == [
        {
            "adapter_type": "PublicProvidersListAdapter",
            "mode": "public",
            "adapter_state": {
                "include_models": True,
                "include_endpoints": True,
                "active_only": False,
            },
        }
    ]


def test_system_catalog_provider_detail_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"id": "provider-1"})

    response = client.get("/v1/providers/provider-1?include_models=true")

    assert response.status_code == 200
    assert response.json() == {"id": "provider-1"}
    assert calls == [
        {
            "adapter_type": "PublicProviderDetailAdapter",
            "mode": "public",
            "adapter_state": {
                "provider_identifier": "provider-1",
                "include_models": True,
                "include_endpoints": False,
            },
        }
    ]


def test_system_catalog_test_connection_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"status": "success"})

    response = client.get("/v1/test-connection?provider=openai&model=gpt-5&api_format=openai:chat")

    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    assert calls == [
        {
            "adapter_type": "PublicTestConnectionAdapter",
            "mode": "public",
            "adapter_state": {
                "provider": "openai",
                "model": "gpt-5",
                "api_format": "openai:chat",
            },
        }
    ]
