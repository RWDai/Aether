from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import src.services.task.video.cancel as cancel_mod
import src.services.request.rust_executor_client as rust_client_mod
from src.core.api_format.conversion.internal_video import VideoStatus
from src.services.request.rust_executor_client import RustExecutorSyncResult
from src.services.task.video.cancel import VideoTaskCancelService


class _Query:
    def __init__(self, result: object) -> None:
        self._result = result

    def filter(self, *args: object, **kwargs: object) -> "_Query":
        del args, kwargs
        return self

    def first(self) -> object:
        return self._result


class _FakeDB:
    def __init__(self, *results: object) -> None:
        self._results = list(results)
        self.committed = False

    def query(self, model: object) -> _Query:
        del model
        return _Query(self._results.pop(0))

    def commit(self) -> None:
        self.committed = True


@pytest.mark.asyncio
async def test_video_cancel_service_uses_rust_for_openai_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="openai",
        endpoint_kind="video",
        base_url="https://api.openai.com",
    )
    key = SimpleNamespace(id="key-1", api_key="encrypted")
    db = _FakeDB(endpoint, key)
    service = VideoTaskCancelService(db)
    task = SimpleNamespace(
        id="task-1",
        status=VideoStatus.PROCESSING.value,
        external_task_id="ext-1",
        endpoint_id="ep-1",
        key_id="key-1",
        request_id="req-1",
        model="sora-2",
        completed_at=None,
        updated_at=None,
    )

    monkeypatch.setattr(cancel_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(
            return_value=RustExecutorSyncResult(
                status_code=204,
                headers={},
                response_json=None,
                response_body_bytes=None,
            )
        ),
    )
    monkeypatch.setattr(
        "src.core.crypto.crypto_service.decrypt",
        lambda value: "upstream-key",
    )
    monkeypatch.setattr(
        "src.core.api_format.build_upstream_headers_for_endpoint",
        lambda *args, **kwargs: {"authorization": "Bearer upstream-key"},
    )
    monkeypatch.setattr(
        "src.services.provider.transport.build_provider_url",
        lambda endpoint, is_stream=False, key=None: "https://api.openai.com/v1/videos",
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_default_client_async",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    monkeypatch.setattr(cancel_mod.UsageService, "finalize_void", lambda *args, **kwargs: True)

    result = await service.cancel_task(
        task=task,
        task_id="task-1",
        original_headers={"x-test": "1"},
    )

    assert result is None
    assert db.committed is True
    assert task.status == VideoStatus.CANCELLED.value


@pytest.mark.asyncio
async def test_video_cancel_service_uses_rust_for_gemini_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="gemini",
        endpoint_kind="video",
        base_url="https://generativelanguage.googleapis.com",
    )
    key = SimpleNamespace(id="key-1", api_key="encrypted")
    db = _FakeDB(endpoint, key)
    service = VideoTaskCancelService(db)
    task = SimpleNamespace(
        id="task-1",
        status=VideoStatus.PROCESSING.value,
        external_task_id="operations/ext-1",
        endpoint_id="ep-1",
        key_id="key-1",
        request_id="req-1",
        model="veo-3",
        completed_at=None,
        updated_at=None,
    )

    monkeypatch.setattr(cancel_mod.config, "executor_backend", "rust")
    execute_sync = AsyncMock(
        return_value=RustExecutorSyncResult(
            status_code=200,
            headers={"content-type": "application/json"},
            response_json={"ok": True},
            response_body_bytes=None,
        )
    )
    monkeypatch.setattr(rust_client_mod.RustExecutorClient, "execute_sync_json", execute_sync)
    monkeypatch.setattr(
        "src.core.crypto.crypto_service.decrypt",
        lambda value: "upstream-key",
    )
    monkeypatch.setattr(
        "src.core.api_format.build_upstream_headers_for_endpoint",
        lambda *args, **kwargs: {"x-goog-api-key": "upstream-key"},
    )
    monkeypatch.setattr(
        "src.services.provider.auth.get_provider_auth",
        AsyncMock(return_value=SimpleNamespace(auth_header="authorization", auth_value="Bearer token")),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_default_client_async",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )
    monkeypatch.setattr(cancel_mod.UsageService, "finalize_void", lambda *args, **kwargs: True)

    result = await service.cancel_task(
        task=task,
        task_id="task-1",
        original_headers={"x-test": "1"},
    )

    assert result is None
    assert db.committed is True
    assert task.status == VideoStatus.CANCELLED.value
    plan = execute_sync.await_args.args[0]
    assert plan.method == "POST"
    assert plan.body.json_body == {}


@pytest.mark.asyncio
async def test_video_cancel_service_returns_503_when_rust_backend_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="openai",
        endpoint_kind="video",
        base_url="https://api.openai.com",
    )
    key = SimpleNamespace(id="key-1", api_key="encrypted")
    db = _FakeDB(endpoint, key)
    service = VideoTaskCancelService(db)
    task = SimpleNamespace(
        id="task-1",
        status=VideoStatus.PROCESSING.value,
        external_task_id="ext-1",
        endpoint_id="ep-1",
        key_id="key-1",
        request_id="req-1",
        model="sora-2",
        completed_at=None,
        updated_at=None,
    )

    monkeypatch.setattr(cancel_mod.config, "executor_backend", "python")
    monkeypatch.setattr(
        "src.core.crypto.crypto_service.decrypt",
        lambda value: "upstream-key",
    )
    monkeypatch.setattr(
        "src.core.api_format.build_upstream_headers_for_endpoint",
        lambda *args, **kwargs: {"authorization": "Bearer upstream-key"},
    )
    monkeypatch.setattr(
        "src.services.provider.transport.build_provider_url",
        lambda endpoint, is_stream=False, key=None: "https://api.openai.com/v1/videos",
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_default_client_async",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    response = await service.cancel_task(
        task=task,
        task_id="task-1",
        original_headers={"x-test": "1"},
    )

    assert isinstance(response, httpx.Response)
    assert response.status_code == 503
    assert response.json() == {"error": {"message": "Video 取消仅支持 Rust executor"}}
    assert db.committed is False
    assert task.status == VideoStatus.PROCESSING.value


@pytest.mark.asyncio
async def test_video_cancel_service_returns_503_when_rust_executor_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = SimpleNamespace(
        id="ep-1",
        provider_id="prov-1",
        api_family="gemini",
        endpoint_kind="video",
        base_url="https://generativelanguage.googleapis.com",
    )
    key = SimpleNamespace(id="key-1", api_key="encrypted")
    db = _FakeDB(endpoint, key)
    service = VideoTaskCancelService(db)
    task = SimpleNamespace(
        id="task-1",
        status=VideoStatus.PROCESSING.value,
        external_task_id="operations/ext-1",
        endpoint_id="ep-1",
        key_id="key-1",
        request_id="req-1",
        model="veo-3",
        completed_at=None,
        updated_at=None,
    )

    monkeypatch.setattr(cancel_mod.config, "executor_backend", "rust")
    monkeypatch.setattr(
        rust_client_mod.RustExecutorClient,
        "execute_sync_json",
        AsyncMock(side_effect=rust_client_mod.RustExecutorClientError("executor down")),
    )
    monkeypatch.setattr(
        "src.core.crypto.crypto_service.decrypt",
        lambda value: "upstream-key",
    )
    monkeypatch.setattr(
        "src.core.api_format.build_upstream_headers_for_endpoint",
        lambda *args, **kwargs: {"x-goog-api-key": "upstream-key"},
    )
    monkeypatch.setattr(
        "src.services.provider.auth.get_provider_auth",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.clients.http_client.HTTPClientPool.get_default_client_async",
        AsyncMock(side_effect=AssertionError("python fallback should not run")),
    )

    response = await service.cancel_task(
        task=task,
        task_id="task-1",
        original_headers={"x-test": "1"},
    )

    assert isinstance(response, httpx.Response)
    assert response.status_code == 503
    assert response.json() == {"error": {"message": "执行器暂时不可用，请稍后重试"}}
    assert db.committed is False
    assert task.status == VideoStatus.PROCESSING.value
