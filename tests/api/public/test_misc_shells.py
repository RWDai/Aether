from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.database import get_db


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    *,
    pipeline_result: Any,
) -> tuple[TestClient, list[dict[str, Any]]]:
    module = __import__(module_name, fromlist=["router", "pipeline"])

    app = FastAPI()
    app.include_router(module.router)
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

    monkeypatch.setattr(module.pipeline, "run", _fake_pipeline_run)
    return TestClient(app), calls


def test_public_site_info_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(
        monkeypatch,
        "src.api.public.catalog",
        pipeline_result={"site_name": "Aether", "site_subtitle": "AI Gateway"},
    )

    response = client.get("/api/public/site-info")

    assert response.status_code == 200
    assert response.json() == {"site_name": "Aether", "site_subtitle": "AI Gateway"}
    assert calls == [
        {
            "adapter_type": "PublicSiteInfoAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_public_modules_auth_status_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        "src.api.public.modules",
        pipeline_result=[{"name": "oauth", "display_name": "OAuth", "active": True}],
    )

    response = client.get("/api/modules/auth-status")

    assert response.status_code == 200
    assert response.json() == [{"name": "oauth", "display_name": "OAuth", "active": True}]
    assert calls == [
        {
            "adapter_type": "PublicAuthModulesStatusAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_public_capabilities_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(
        monkeypatch,
        "src.api.public.capabilities",
        pipeline_result={"capabilities": []},
    )

    response = client.get("/api/capabilities")

    assert response.status_code == 200
    assert response.json() == {"capabilities": []}
    assert calls == [
        {
            "adapter_type": "PublicCapabilitiesListAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_public_user_configurable_capabilities_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        "src.api.public.capabilities",
        pipeline_result={"capabilities": [{"name": "vision"}]},
    )

    response = client.get("/api/capabilities/user-configurable")

    assert response.status_code == 200
    assert response.json() == {"capabilities": [{"name": "vision"}]}
    assert calls == [
        {
            "adapter_type": "PublicUserConfigurableCapabilitiesAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_public_model_capabilities_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        "src.api.public.capabilities",
        pipeline_result={"model": "gpt-5", "supported_capabilities": []},
    )

    response = client.get("/api/capabilities/model/gpt-5")

    assert response.status_code == 200
    assert response.json() == {"model": "gpt-5", "supported_capabilities": []}
    assert calls == [
        {
            "adapter_type": "PublicModelCapabilitiesAdapter",
            "mode": "public",
            "adapter_state": {"model_name": "gpt-5"},
        }
    ]
