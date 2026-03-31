from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.database import get_db


def _normalize_state(adapter: Any) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for key, value in dict(getattr(adapter, "__dict__", {})).items():
        if hasattr(value, "model_dump"):
            state[key] = value.model_dump()
        elif hasattr(value, "filename"):
            state[key] = {
                "filename": value.filename,
                "content_type": getattr(value, "content_type", None),
            }
        else:
            state[key] = value
    return state


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pipeline_result: Any,
) -> tuple[TestClient, list[dict[str, Any]]]:
    from src.api.admin import gemini_files as mod

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
                "adapter_state": _normalize_state(adapter),
            }
        )
        return pipeline_result

    monkeypatch.setattr(mod.pipeline, "run", _fake_pipeline_run)
    return TestClient(app), calls


def test_admin_gemini_files_list_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={"items": [], "total": 0, "page": 2, "page_size": 50},
    )

    response = client.get(
        "/api/admin/gemini-files/mappings?page=2&page_size=50&include_expired=true&search=demo"
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "page": 2, "page_size": 50}
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesListMappingsAdapter",
            "mode": "admin",
            "adapter_state": {
                "page": 2,
                "page_size": 50,
                "include_expired": True,
                "search": "demo",
            },
        }
    ]


def test_admin_gemini_files_stats_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={
            "total_mappings": 1,
            "active_mappings": 1,
            "expired_mappings": 0,
            "by_mime_type": {"text/plain": 1},
            "capable_keys_count": 2,
        },
    )

    response = client.get("/api/admin/gemini-files/stats")

    assert response.status_code == 200
    assert response.json() == {
        "total_mappings": 1,
        "active_mappings": 1,
        "expired_mappings": 0,
        "by_mime_type": {"text/plain": 1},
        "capable_keys_count": 2,
    }
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesStatsAdapter",
            "mode": "admin",
            "adapter_state": {},
        }
    ]


def test_admin_gemini_files_delete_mapping_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={"message": "Mapping deleted successfully", "file_name": "files/1"},
    )

    response = client.delete("/api/admin/gemini-files/mappings/mapping-1")

    assert response.status_code == 200
    assert response.json() == {
        "message": "Mapping deleted successfully",
        "file_name": "files/1",
    }
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesDeleteMappingAdapter",
            "mode": "admin",
            "adapter_state": {"mapping_id": "mapping-1"},
        }
    ]


def test_admin_gemini_files_cleanup_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={"message": "Cleaned up 3 expired mappings", "deleted_count": 3},
    )

    response = client.delete("/api/admin/gemini-files/mappings")

    assert response.status_code == 200
    assert response.json() == {
        "message": "Cleaned up 3 expired mappings",
        "deleted_count": 3,
    }
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesCleanupMappingsAdapter",
            "mode": "admin",
            "adapter_state": {},
        }
    ]


def test_admin_gemini_files_capable_keys_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result=[{"id": "key-1", "name": "Key 1", "provider_name": "Gemini"}],
    )

    response = client.get("/api/admin/gemini-files/capable-keys")

    assert response.status_code == 200
    assert response.json() == [{"id": "key-1", "name": "Key 1", "provider_name": "Gemini"}]
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesCapableKeysAdapter",
            "mode": "admin",
            "adapter_state": {},
        }
    ]


def test_admin_gemini_files_upload_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={
            "display_name": "example.txt",
            "mime_type": "text/plain",
            "size_bytes": 5,
            "results": [],
            "success_count": 0,
            "fail_count": 0,
        },
    )

    response = client.post(
        "/api/admin/gemini-files/upload?key_ids=key-1,key-2",
        files={"file": ("example.txt", b"hello", "text/plain")},
    )

    assert response.status_code == 200
    assert response.json() == {
        "display_name": "example.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
        "results": [],
        "success_count": 0,
        "fail_count": 0,
    }
    assert calls == [
        {
            "adapter_type": "AdminGeminiFilesUploadAdapter",
            "mode": "admin",
            "adapter_state": {
                "file": {
                    "filename": "example.txt",
                    "content_type": "text/plain",
                },
                "key_ids": "key-1,key-2",
            },
        }
    ]
