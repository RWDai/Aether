from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.api_format.conversion.internal_video import InternalVideoPollResult, VideoStatus
from src.models.database import VideoTask
from src.services.request.rust_executor_client import RustExecutorSyncResult
from src.services.task.video.poller_adapter import VideoPollContext, VideoTaskPollerAdapter


@pytest.mark.asyncio
async def test_poll_task_status_routes_gemini_video_to_gemini(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = VideoTaskPollerAdapter()

    task = cast(
        VideoTask,
        SimpleNamespace(
            endpoint_id="e1",
            key_id="k1",
            provider_api_format="gemini:video",
            external_task_id="operations/123",
        ),
    )
    prepared_ctx = VideoPollContext(
        task_id="task-1",
        external_task_id="operations/123",
        provider_api_format="gemini:video",
        base_url="https://example.com",
        upstream_key="decrypted",
        headers={"authorization": "Bearer x"},
        poll_count=0,
        retry_count=0,
        poll_interval_seconds=15,
        max_poll_count=10,
        current_status=VideoStatus.PROCESSING.value,
    )
    poll_http = AsyncMock(return_value=InternalVideoPollResult(status=VideoStatus.PROCESSING))
    monkeypatch.setattr(adapter, "prepare_poll_context", AsyncMock(return_value=prepared_ctx))
    monkeypatch.setattr(adapter, "poll_task_http", poll_http)

    result = await adapter._poll_task_status(MagicMock(), task)
    assert result.status == VideoStatus.PROCESSING
    poll_http.assert_awaited_once_with(prepared_ctx)


@pytest.mark.asyncio
async def test_update_task_after_poll_skips_terminal_cancelled_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finalize = AsyncMock()
    adapter = VideoTaskPollerAdapter(finalize_video_task_fn=finalize)

    cancelled_task = SimpleNamespace(
        id="t1",
        status=VideoStatus.CANCELLED.value,
    )

    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = None
    session.get.return_value = cancelled_task

    monkeypatch.setattr("src.services.task.video.poller_adapter.create_session", lambda: session)

    await adapter.update_task_after_poll(
        task_id="t1",
        result=InternalVideoPollResult(status=VideoStatus.COMPLETED),
        ctx=None,
        redis_client=None,
    )

    finalize.assert_not_awaited()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_video_poller_try_rust_payload_passes_proxy_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.services.request import rust_executor_client as rust_mod
    from src.services.task.video import poller_adapter as mod

    adapter = VideoTaskPollerAdapter()
    monkeypatch.setattr(mod.config, "executor_backend", "rust")

    proxy_snapshot = object()
    captured: dict[str, object] = {}

    async def _fake_execute_sync_json(self: object, plan: object) -> RustExecutorSyncResult:
        captured["plan"] = plan
        return RustExecutorSyncResult(status_code=200, response_json={"id": "op_1", "done": False})

    monkeypatch.setattr(rust_mod.RustExecutorClient, "execute_sync_json", _fake_execute_sync_json)

    ctx = VideoPollContext(
        task_id="task-1",
        external_task_id="vid_1",
        provider_api_format="openai:video",
        base_url="https://api.openai.com",
        upstream_key="upstream",
        headers={"authorization": "Bearer test"},
        poll_count=0,
        retry_count=0,
        poll_interval_seconds=15,
        max_poll_count=10,
        current_status=VideoStatus.PROCESSING.value,
        proxy_snapshot=proxy_snapshot,
    )

    payload = await adapter._try_rust_poll_payload(
        ctx=ctx,
        url="https://api.openai.com/v1/videos/vid_1",
    )

    assert payload == {"id": "op_1", "done": False}
    assert getattr(captured["plan"], "method") == "GET"
    assert getattr(captured["plan"], "proxy") is proxy_snapshot


@pytest.mark.asyncio
async def test_video_poller_openai_poll_prefers_rust_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = VideoTaskPollerAdapter()

    rust_poll = AsyncMock(return_value={"id": "vid_1", "status": "processing"})
    monkeypatch.setattr(adapter, "_try_rust_poll_payload", rust_poll)

    normalized = InternalVideoPollResult(status=VideoStatus.PROCESSING, progress_percent=42)
    normalizer = MagicMock(return_value=normalized)
    monkeypatch.setattr(adapter._openai_normalizer, "video_poll_to_internal", normalizer)

    ctx = VideoPollContext(
        task_id="task-1",
        external_task_id="vid_1",
        provider_api_format="openai:video",
        base_url="https://api.openai.com",
        upstream_key="upstream",
        headers={"authorization": "Bearer test"},
        poll_count=0,
        retry_count=0,
        poll_interval_seconds=15,
        max_poll_count=10,
        current_status=VideoStatus.PROCESSING.value,
    )

    result = await adapter._poll_openai_with_context(ctx)

    assert result is normalized
    normalizer.assert_called_once_with({"id": "vid_1", "status": "processing"})
    rust_poll.assert_awaited_once()


@pytest.mark.asyncio
async def test_video_poller_openai_poll_requires_rust_executor_when_payload_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = VideoTaskPollerAdapter()

    monkeypatch.setattr(adapter, "_try_rust_poll_payload", AsyncMock(return_value=None))

    ctx = VideoPollContext(
        task_id="task-2",
        external_task_id="vid_2",
        provider_api_format="openai:video",
        base_url="https://api.openai.com",
        upstream_key="upstream",
        headers={"authorization": "Bearer test"},
        poll_count=0,
        retry_count=0,
        poll_interval_seconds=15,
        max_poll_count=10,
        current_status=VideoStatus.PROCESSING.value,
        proxy_config={"enabled": True, "url": "http://proxy.test:8080"},
        delegate_config={"node_id": "node-1", "tunnel": True},
    )

    with pytest.raises(Exception) as exc_info:
        await adapter._poll_openai_with_context(ctx)

    assert "Rust executor" in str(exc_info.value)
