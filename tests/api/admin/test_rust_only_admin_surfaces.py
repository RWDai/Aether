from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from starlette.datastructures import UploadFile


class _FakeQuery:
    def __init__(self, result: object) -> None:
        self._result = result

    def filter(self, *_args: object, **_kwargs: object) -> "_FakeQuery":
        return self

    def first(self) -> object:
        return self._result


class _FakeDB:
    def __init__(self, result: object) -> None:
        self._result = result

    def query(self, *_args: object, **_kwargs: object) -> _FakeQuery:
        return _FakeQuery(self._result)


@pytest.mark.asyncio
async def test_admin_system_check_update_returns_unavailable_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import system as mod

    monkeypatch.setattr(mod, "_get_current_version", lambda: "1.2.3")

    result = mod._build_check_update_unavailable_response()

    assert result == {
        "current_version": "1.2.3",
        "latest_version": None,
        "has_update": False,
        "release_url": None,
        "release_notes": None,
        "published_at": None,
        "error": "检查更新需要 Rust 管理后端",
    }


@pytest.mark.asyncio
async def test_admin_system_aws_regions_uses_local_cache_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import system as mod
    from src.core.cache_service import CacheService

    monkeypatch.setattr(CacheService, "get", AsyncMock(return_value=["us-east-1", "us-west-2"]))
    mod._aws_regions_mem_cache = None

    result = await mod._get_aws_regions_response()

    assert result == {"regions": ["us-east-1", "us-west-2"]}


@pytest.mark.asyncio
async def test_admin_system_aws_regions_raises_without_local_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import system as mod
    from src.core.cache_service import CacheService

    monkeypatch.setattr(CacheService, "get", AsyncMock(return_value=None))
    mod._aws_regions_mem_cache = None

    with pytest.raises(HTTPException) as exc_info:
        await mod._get_aws_regions_response()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "AWS regions requires Rust admin backend"


@pytest.mark.asyncio
async def test_admin_gemini_file_upload_requires_rust_uploader() -> None:
    from src.api.admin import gemini_files as mod

    upload = UploadFile(filename="example.txt", file=BytesIO(b"hello"), headers=None)

    with pytest.raises(HTTPException) as exc_info:
        await mod._upload_file_response(file=upload, key_ids="key_1")

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Admin Gemini file upload requires Rust uploader"


@pytest.mark.asyncio
async def test_admin_external_models_returns_cached_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.models import external as mod

    monkeypatch.setattr(
        mod,
        "_get_cached_data",
        AsyncMock(return_value={"openai": {"official": True, "models": []}}),
    )

    response = await mod._get_external_models_response()

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert b'"official":true' in response.body


@pytest.mark.asyncio
async def test_admin_external_models_raise_without_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.models import external as mod

    monkeypatch.setattr(mod, "_get_cached_data", AsyncMock(return_value=None))

    with pytest.raises(HTTPException) as exc_info:
        await mod._get_external_models_response()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "External models catalog requires Rust admin backend"


@pytest.mark.asyncio
async def test_admin_video_proxy_raises_when_google_proxy_is_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.video_tasks import routes as mod
    from src.utils import auth_utils

    task = SimpleNamespace(
        id="task_1",
        user_id="user_1",
        video_url="https://generativelanguage.googleapis.com/v1/media/video.mp4",
    )
    request = SimpleNamespace(cookies={}, headers={"Authorization": "Bearer token"})
    db = _FakeDB(task)

    monkeypatch.setattr(
        auth_utils,
        "authenticate_user_from_bearer_token",
        AsyncMock(return_value=SimpleNamespace(id="admin_1", role=mod.UserRole.ADMIN)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await mod._proxy_video_stream_response(task_id="task_1", request=request, token=None, db=db)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Admin video proxy requires Rust/public download path"


@pytest.mark.asyncio
async def test_admin_usage_replay_requires_rust_maintenance_backend() -> None:
    from src.api.admin.usage import routes as mod

    adapter = mod.AdminUsageReplayAdapter(usage_id="usage_1")

    with pytest.raises(HTTPException) as exc_info:
        await adapter.handle(SimpleNamespace())

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Admin usage replay requires Rust maintenance backend"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("call_factory"),
    [
        lambda mod: mod.start_oauth("key_1", request=SimpleNamespace(), db=None, _=None),
        lambda mod: mod.complete_oauth(
            "key_1",
            mod.CompleteOAuthRequest(callback_url="http://localhost/?code=x&state=y"),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.refresh_oauth("key_1", request=SimpleNamespace(), db=None, _=None),
        lambda mod: mod.start_provider_oauth(
            "provider_1", request=SimpleNamespace(), db=None, _=None
        ),
        lambda mod: mod.complete_provider_oauth(
            "provider_1",
            mod.ProviderCompleteOAuthRequest(
                callback_url="http://localhost/?code=x&state=y",
            ),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.import_refresh_token(
            "provider_1",
            mod.ImportRefreshTokenRequest(refresh_token="refresh-token"),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.batch_import_oauth(
            "provider_1",
            mod.BatchImportRequest(credentials="refresh-token"),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.start_batch_import_oauth_task(
            "provider_1",
            mod.BatchImportRequest(credentials="refresh-token"),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.get_batch_import_oauth_task_status(
            "provider_1",
            "task_1",
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.device_authorize(
            "provider_1",
            mod.DeviceAuthorizeRequest(),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
        lambda mod: mod.device_poll(
            "provider_1",
            mod.DevicePollRequest(session_id="session_1"),
            request=SimpleNamespace(),
            db=None,
            _=None,
        ),
    ],
)
async def test_admin_provider_oauth_routes_require_rust_maintenance_backend(
    call_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import provider_oauth as mod

    async def _fake_pipeline_run(*, adapter: object, http_request: object, db: object, mode: object):
        _ = http_request, db, mode
        context = SimpleNamespace(add_audit_metadata=lambda **_kwargs: None)
        return await adapter.handle(context)  # type: ignore[attr-defined]

    monkeypatch.setattr(mod.pipeline, "run", _fake_pipeline_run)

    with pytest.raises(HTTPException) as exc_info:
        await call_factory(mod)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Admin provider OAuth requires Rust maintenance backend"
