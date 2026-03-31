from __future__ import annotations

from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.responses import StreamingResponse

import src.api.handlers.openai.video_handler as video_mod
from src.api.handlers.openai.video_handler import OpenAIVideoHandler
from src.core.api_format.conversion.internal_video import VideoStatus
from src.core.exceptions import ProviderNotAvailableException
from src.services.request.rust_executor_client import (
    RustExecutorClientError,
    RustExecutorStreamResult,
)


class _DummyStreamResponseCtx:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.closed = True


async def _iter_chunks(chunks: list[bytes]) -> AsyncGenerator[bytes]:
    for chunk in chunks:
        yield chunk


def _make_handler() -> OpenAIVideoHandler:
    return OpenAIVideoHandler(
        db=SimpleNamespace(),
        user=SimpleNamespace(id="user-1"),
        api_key=SimpleNamespace(id="api-key-1"),
        request_id="req-video-test",
        client_ip="127.0.0.1",
        user_agent="pytest",
        start_time=0.0,
    )


@pytest.mark.asyncio
async def test_handle_download_content_uses_rust_executor_for_direct_video_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(video_mod.config, "executor_backend", "rust")
    handler = _make_handler()
    dummy_ctx = _DummyStreamResponseCtx()

    monkeypatch.setattr(
        handler,
        "_get_task",
        lambda task_id: SimpleNamespace(
            id=task_id,
            status=VideoStatus.COMPLETED.value,
            video_url="https://cdn.example.com/video.mp4",
            model="sora-2",
        ),
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "method") == "GET"
        assert getattr(plan, "url") == "https://cdn.example.com/video.mp4"
        assert getattr(plan, "body").json_body is None
        assert getattr(plan, "body").body_bytes_b64 is None
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "video/mp4", "x-rust-download": "true"},
            byte_iterator=_iter_chunks([b"video-", b"bytes"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(video_mod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    response = await handler.handle_download_content(
        task_id="task-1",
        http_request=SimpleNamespace(),
        original_headers={},
        query_params={"variant": "video"},
    )

    assert isinstance(response, StreamingResponse)
    assert response.headers["x-rust-download"] == "true"
    body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"video-bytes"
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_handle_download_content_uses_rust_executor_for_upstream_content_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(video_mod.config, "executor_backend", "rust")
    handler = _make_handler()
    dummy_ctx = _DummyStreamResponseCtx()

    monkeypatch.setattr(
        handler,
        "_get_task",
        lambda task_id: SimpleNamespace(
            id=task_id,
            status=VideoStatus.COMPLETED.value,
            video_url=None,
            external_task_id="ext-1",
            model="sora-2",
        ),
    )
    monkeypatch.setattr(
        handler,
        "_get_endpoint_and_key",
        lambda task: (
            SimpleNamespace(id="ep-1", provider_id="prov-1", base_url="https://api.openai.com"),
            SimpleNamespace(id="key-1", api_key="encrypted"),
        ),
    )
    monkeypatch.setattr(video_mod.crypto_service, "decrypt", lambda _: "upstream-key")
    monkeypatch.setattr(
        handler,
        "_build_upstream_url",
        lambda base_url, suffix=None: "https://api.openai.com/v1/videos/ext-1/content",
    )
    monkeypatch.setattr(
        handler,
        "_build_upstream_headers",
        lambda original_headers, upstream_key, endpoint: {"authorization": f"Bearer {upstream_key}"},
    )

    async def _fake_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        assert getattr(plan, "method") == "GET"
        assert getattr(plan, "url") == "https://api.openai.com/v1/videos/ext-1/content"
        assert getattr(plan, "headers") == {"authorization": "Bearer upstream-key"}
        assert getattr(plan, "provider_id") == "prov-1"
        assert getattr(plan, "endpoint_id") == "ep-1"
        assert getattr(plan, "key_id") == "key-1"
        return RustExecutorStreamResult(
            status_code=200,
            headers={"content-type": "video/mp4", "x-rust-download": "true"},
            byte_iterator=_iter_chunks([b"upstream-", b"video"]),
            response_ctx=dummy_ctx,
        )

    monkeypatch.setattr(video_mod.RustExecutorClient, "execute_stream", _fake_execute_stream)

    response = await handler.handle_download_content(
        task_id="task-1",
        http_request=SimpleNamespace(),
        original_headers={},
        query_params=None,
    )

    assert isinstance(response, StreamingResponse)
    body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"upstream-video"
    assert dummy_ctx.closed is True


@pytest.mark.asyncio
async def test_handle_download_content_raises_when_rust_executor_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(video_mod.config, "executor_backend", "rust")
    handler = _make_handler()

    monkeypatch.setattr(
        handler,
        "_get_task",
        lambda task_id: SimpleNamespace(
            id=task_id,
            status=VideoStatus.COMPLETED.value,
            video_url="https://cdn.example.com/video.mp4",
            model="sora-2",
        ),
    )

    async def _failing_execute_stream(self: object, plan: object) -> RustExecutorStreamResult:
        del self, plan
        raise RustExecutorClientError("executor down")

    monkeypatch.setattr(video_mod.RustExecutorClient, "execute_stream", _failing_execute_stream)
    with pytest.raises(ProviderNotAvailableException):
        await handler.handle_download_content(
            task_id="task-1",
            http_request=SimpleNamespace(),
            original_headers={},
            query_params={"variant": "video"},
        )
