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
    from src.api.public import models as mod

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


def test_public_openai_models_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"object": "list", "data": []})

    response = client.get("/v1/models?after_id=model-a&limit=12")

    assert response.status_code == 200
    assert response.json() == {"object": "list", "data": []}
    assert calls == [
        {
            "adapter_type": "PublicModelsListAdapter",
            "mode": "public",
            "adapter_state": {
                "before_id": None,
                "after_id": "model-a",
                "limit": 12,
                "page_size": 50,
                "page_token": None,
            },
        }
    ]


def test_public_model_detail_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"id": "gpt-5"})

    response = client.get("/v1/models/gpt-5")

    assert response.status_code == 200
    assert response.json() == {"id": "gpt-5"}
    assert calls == [
        {
            "adapter_type": "PublicModelDetailAdapter",
            "mode": "public",
            "adapter_state": {
                "model_id": "gpt-5",
                "force_gemini_name": False,
            },
        }
    ]


def test_public_gemini_models_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"models": []})

    response = client.get("/v1beta/models?pageSize=25&pageToken=next-1")

    assert response.status_code == 200
    assert response.json() == {"models": []}
    assert calls == [
        {
            "adapter_type": "PublicModelsListAdapter",
            "mode": "public",
            "adapter_state": {
                "before_id": None,
                "after_id": None,
                "limit": 20,
                "page_size": 25,
                "page_token": "next-1",
            },
        }
    ]


def test_public_gemini_model_detail_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"name": "models/gemini-2.5-pro"})

    response = client.get("/v1beta/models/models/gemini-2.5-pro")

    assert response.status_code == 200
    assert response.json() == {"name": "models/gemini-2.5-pro"}
    assert calls == [
        {
            "adapter_type": "PublicModelDetailAdapter",
            "mode": "public",
            "adapter_state": {
                "model_id": "models/gemini-2.5-pro",
                "force_gemini_name": True,
            },
        }
    ]
