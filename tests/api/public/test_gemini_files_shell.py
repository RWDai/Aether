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
    from src.api.public import gemini_files as mod

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


def test_gemini_files_upload_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"file": {"name": "files/1"}})

    response = client.post("/upload/v1beta/files")

    assert response.status_code == 200
    assert response.json() == {"file": {"name": "files/1"}}
    assert calls == [
        {
            "adapter_type": "PublicGeminiFilesUploadAdapter",
            "mode": "public",
            "adapter_state": {},
        }
    ]


def test_gemini_files_list_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"files": []})

    response = client.get("/v1beta/files?pageSize=20&pageToken=next-1")

    assert response.status_code == 200
    assert response.json() == {"files": []}
    assert calls == [
        {
            "adapter_type": "PublicGeminiFilesListAdapter",
            "mode": "public",
            "adapter_state": {
                "page_size": 20,
                "page_token": "next-1",
            },
        }
    ]


def test_gemini_files_download_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"download": "ok"})

    response = client.get("/v1beta/files/file-1:download?alt=media")

    assert response.status_code == 200
    assert response.json() == {"download": "ok"}
    assert calls == [
        {
            "adapter_type": "PublicGeminiFilesDownloadAdapter",
            "mode": "public",
            "adapter_state": {"file_id": "file-1"},
        }
    ]


def test_gemini_files_get_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"name": "files/file-1"})

    response = client.get("/v1beta/files/file-1")

    assert response.status_code == 200
    assert response.json() == {"name": "files/file-1"}
    assert calls == [
        {
            "adapter_type": "PublicGeminiFilesGetAdapter",
            "mode": "public",
            "adapter_state": {"file_name": "file-1"},
        }
    ]


def test_gemini_files_delete_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"deleted": True})

    response = client.delete("/v1beta/files/file-1")

    assert response.status_code == 200
    assert response.json() == {"deleted": True}
    assert calls == [
        {
            "adapter_type": "PublicGeminiFilesDeleteAdapter",
            "mode": "public",
            "adapter_state": {"file_name": "file-1"},
        }
    ]
