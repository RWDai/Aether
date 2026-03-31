from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route_name", "expected_adapter", "expected_state"),
    [
        ("supported_types", "AdminProviderOAuthUnavailableAdapter", {"operation": "supported_types"}),
        ("start_oauth", "AdminProviderOAuthUnavailableAdapter", {"operation": "start_oauth"}),
        ("complete_oauth", "AdminProviderOAuthUnavailableAdapter", {"operation": "complete_oauth"}),
        ("refresh_oauth", "AdminProviderOAuthUnavailableAdapter", {"operation": "refresh_oauth"}),
        (
            "start_provider_oauth",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "start_provider_oauth"},
        ),
        (
            "complete_provider_oauth",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "complete_provider_oauth"},
        ),
        (
            "import_refresh_token",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "import_refresh_token"},
        ),
        (
            "batch_import_oauth",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "batch_import_oauth"},
        ),
        (
            "start_batch_import_oauth_task",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "start_batch_import_oauth_task"},
        ),
        (
            "get_batch_import_oauth_task_status",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "get_batch_import_oauth_task_status"},
        ),
        (
            "device_authorize",
            "AdminProviderOAuthUnavailableAdapter",
            {"operation": "device_authorize"},
        ),
        ("device_poll", "AdminProviderOAuthUnavailableAdapter", {"operation": "device_poll"}),
    ],
)
async def test_admin_provider_oauth_routes_use_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
    route_name: str,
    expected_adapter: str,
    expected_state: dict[str, Any],
) -> None:
    from src.api.admin import provider_oauth as mod

    captured: dict[str, Any] = {}

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.update(
            {
                "adapter": adapter,
                "request": http_request,
                "db": db,
                "mode": mode,
            }
        )
        if route_name == "supported_types":
            return []
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    request = SimpleNamespace(state=SimpleNamespace())
    db = object()

    if route_name == "supported_types":
        result = await mod.supported_types(request=request, db=db, _=None)
    elif route_name == "start_oauth":
        result = await mod.start_oauth("key_1", request=request, db=db, _=None)
    elif route_name == "complete_oauth":
        result = await mod.complete_oauth(
            "key_1",
            mod.CompleteOAuthRequest(callback_url="http://localhost/?code=x&state=y"),
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "refresh_oauth":
        result = await mod.refresh_oauth("key_1", request=request, db=db, _=None)
    elif route_name == "start_provider_oauth":
        result = await mod.start_provider_oauth("provider_1", request=request, db=db, _=None)
    elif route_name == "complete_provider_oauth":
        result = await mod.complete_provider_oauth(
            "provider_1",
            mod.ProviderCompleteOAuthRequest(callback_url="http://localhost/?code=x&state=y"),
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "import_refresh_token":
        result = await mod.import_refresh_token(
            "provider_1",
            mod.ImportRefreshTokenRequest(refresh_token="refresh-token"),
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "batch_import_oauth":
        result = await mod.batch_import_oauth(
            "provider_1",
            mod.BatchImportRequest(credentials="refresh-token"),
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "start_batch_import_oauth_task":
        result = await mod.start_batch_import_oauth_task(
            "provider_1",
            mod.BatchImportRequest(credentials="refresh-token"),
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "get_batch_import_oauth_task_status":
        result = await mod.get_batch_import_oauth_task_status(
            "provider_1",
            "task_1",
            request=request,
            db=db,
            _=None,
        )
    elif route_name == "device_authorize":
        result = await mod.device_authorize(
            "provider_1",
            mod.DeviceAuthorizeRequest(),
            request=request,
            db=db,
            _=None,
        )
    else:
        result = await mod.device_poll(
            "provider_1",
            mod.DevicePollRequest(session_id="session_1"),
            request=request,
            db=db,
            _=None,
        )

    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode
    assert type(captured["adapter"]).__name__ == expected_adapter
    assert getattr(captured["adapter"], "__dict__", {}) == expected_state
    if route_name == "supported_types":
        assert result == []
    else:
        assert result == {"ok": True}
