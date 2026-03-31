from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.database import get_db


def _build_app(monkeypatch: pytest.MonkeyPatch, *, pipeline_result: Any) -> tuple[TestClient, list[dict[str, Any]]]:
    from src.api.public import videos as mod

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
        api_format_hint: str,
        path_params: dict[str, Any] | None = None,
    ) -> Any:
        del http_request, db
        calls.append(
            {
                "adapter_type": type(adapter).__name__,
                "mode": getattr(mode, "value", mode),
                "api_format_hint": api_format_hint,
                "path_params": path_params,
            }
        )
        return pipeline_result

    monkeypatch.setattr(mod.pipeline, "run", _fake_pipeline_run)
    return TestClient(app), calls


def test_openai_video_create_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"ok": True})

    response = client.post("/v1/videos", json={"model": "sora", "prompt": "hello"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert calls == [
        {
            "adapter_type": "OpenAIVideoAdapter",
            "mode": "standard",
            "api_format_hint": "openai:video",
            "path_params": None,
        }
    ]


def test_openai_video_download_route_passes_task_id_to_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"download": True})

    response = client.get("/v1/videos/task-123/content")

    assert response.status_code == 200
    assert response.json() == {"download": True}
    assert calls == [
        {
            "adapter_type": "OpenAIVideoAdapter",
            "mode": "standard",
            "api_format_hint": "openai:video",
            "path_params": {"task_id": "task-123"},
        }
    ]


def test_gemini_video_create_route_passes_model_to_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"ok": True})

    response = client.post(
        "/v1beta/models/veo-3:predictLongRunning",
        json={"prompt": "hello"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert calls == [
        {
            "adapter_type": "GeminiVeoAdapter",
            "mode": "standard",
            "api_format_hint": "gemini:video",
            "path_params": {"model": "veo-3"},
        }
    ]


def test_gemini_video_cancel_route_reconstructs_operation_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"ok": True})

    response = client.post("/v1beta/models/veo-3/operations/op-1:cancel")

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert calls == [
        {
            "adapter_type": "GeminiVeoAdapter",
            "mode": "standard",
            "api_format_hint": "gemini:video",
            "path_params": {
                "task_id": "models/veo-3/operations/op-1",
                "action": "cancel",
            },
        }
    ]
