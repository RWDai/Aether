from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request


def _make_request(path: str, method: str = "POST") -> Request:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_admin_external_models_route_uses_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.models import external as mod

    captured: dict[str, object] = {}

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.update(
            {"adapter": adapter, "request": http_request, "db": db, "mode": mode},
        )
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = SimpleNamespace(name="db")
    request = _make_request("/api/admin/models/external", method="GET")

    result = await mod.get_external_models(request=request, db=db, _=SimpleNamespace())

    assert result == {"ok": True}
    assert isinstance(captured["adapter"], mod.AdminGetExternalModelsAdapter)
    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "adapter_type"),
    [
        ("/api/admin/system/version", "AdminSystemVersionAdapter"),
        ("/api/admin/system/check-update", "AdminSystemCheckUpdateAdapter"),
        ("/api/admin/system/aws-regions", "AdminAwsRegionsAdapter"),
    ],
)
async def test_admin_system_routes_use_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    adapter_type: str,
) -> None:
    from src.api.admin import system as mod

    captured: dict[str, object] = {}

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.update(
            {"adapter": adapter, "request": http_request, "db": db, "mode": mode},
        )
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = SimpleNamespace(name="db")
    request = _make_request(path, method="GET")

    if path.endswith("/version"):
        result = await mod.get_system_version(request=request, db=db)
    elif path.endswith("/check-update"):
        result = await mod.check_update(request=request, db=db)
    else:
        result = await mod.get_aws_regions(request=request, db=db)

    assert result == {"ok": True}
    assert type(captured["adapter"]).__name__ == adapter_type
    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode


@pytest.mark.asyncio
async def test_admin_video_proxy_route_uses_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.video_tasks import routes as mod

    captured: dict[str, object] = {}

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.update(
            {"adapter": adapter, "request": http_request, "db": db, "mode": mode},
        )
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = SimpleNamespace(name="db")
    request = _make_request("/api/admin/video-tasks/task_1/video", method="GET")

    result = await mod.proxy_video_stream(task_id="task_1", request=request, token="query-token", db=db)

    assert result == {"ok": True}
    assert isinstance(captured["adapter"], mod.VideoTaskProxyVideoAdapter)
    assert captured["adapter"].task_id == "task_1"
    assert captured["adapter"].token == "query-token"
    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode


@pytest.mark.asyncio
async def test_admin_external_models_cache_route_uses_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.models import external as mod

    captured: dict[str, object] = {}

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.update(
            {"adapter": adapter, "request": http_request, "db": db, "mode": mode},
        )
        return {"cleared": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = SimpleNamespace(name="db")
    request = _make_request("/api/admin/models/external/cache", method="DELETE")

    result = await mod.clear_external_models_cache(request=request, db=db, _=SimpleNamespace())

    assert result == {"cleared": True}
    assert isinstance(captured["adapter"], mod.AdminClearExternalModelsCacheAdapter)
    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode


@pytest.mark.asyncio
async def test_admin_external_models_adapters_delegate_to_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin.models import external as mod

    get_cached = AsyncMock(return_value={"ok": True})
    clear_cached = AsyncMock(return_value={"cleared": True})
    monkeypatch.setattr(mod, "_get_external_models_response", get_cached)
    monkeypatch.setattr(mod, "_clear_external_models_cache_response", clear_cached)

    get_result = await mod.AdminGetExternalModelsAdapter().handle(SimpleNamespace())
    clear_result = await mod.AdminClearExternalModelsCacheAdapter().handle(SimpleNamespace())

    assert get_result == {"ok": True}
    assert clear_result == {"cleared": True}
    get_cached.assert_awaited_once()
    clear_cached.assert_awaited_once()


@pytest.mark.asyncio
async def test_provider_query_routes_use_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import provider_query as mod
    from fastapi import HTTPException

    captured: list[dict[str, object]] = []

    async def fake_run(*, adapter, http_request, db, mode, **_kwargs):
        captured.append({"adapter": adapter, "request": http_request, "db": db, "mode": mode})
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = SimpleNamespace(name="db")

    models_payload = mod.ModelsQueryRequest(provider_id="provider_1")
    models_request = _make_request("/api/admin/provider-query/models")
    with pytest.raises(HTTPException) as models_exc:
        await mod.query_available_models(models_payload, models_request, db=db)

    test_payload = mod.TestModelRequest(provider_id="provider_1", model_name="gpt-4o")
    test_request = _make_request("/api/admin/provider-query/test-model")
    with pytest.raises(HTTPException) as test_exc:
        await mod.test_model(test_payload, test_request, db=db)

    failover_payload = mod.TestModelFailoverRequest(
        provider_id="provider_1",
        mode="direct",
        model_name="gpt-4o",
    )
    failover_request = _make_request("/api/admin/provider-query/test-model-failover")
    with pytest.raises(HTTPException) as failover_exc:
        await mod.test_model_failover(failover_payload, failover_request, db=db)

    assert models_exc.value.status_code == 503
    assert test_exc.value.status_code == 503
    assert failover_exc.value.status_code == 503
    assert "requires Rust maintenance backend" in str(models_exc.value.detail)
    assert captured == []


@pytest.mark.asyncio
async def test_provider_query_adapters_delegate_to_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.admin import provider_query as mod

    models_helper = AsyncMock(return_value={"kind": "models"})
    test_helper = AsyncMock(return_value={"kind": "test"})
    failover_helper = AsyncMock(return_value={"kind": "failover"})
    monkeypatch.setattr(mod, "_query_available_models_response", models_helper)
    monkeypatch.setattr(mod, "_test_model_response", test_helper)
    monkeypatch.setattr(mod, "_test_model_failover_response", failover_helper)

    db = SimpleNamespace(name="db")
    user = SimpleNamespace(id="user_1")
    request = _make_request("/api/admin/provider-query/test-model-failover")
    context = SimpleNamespace(db=db, user=user, request=request)

    models_payload = mod.ModelsQueryRequest(provider_id="provider_1")
    test_payload = mod.TestModelRequest(provider_id="provider_1", model_name="gpt-4o")
    failover_payload = mod.TestModelFailoverRequest(
        provider_id="provider_1",
        mode="direct",
        model_name="gpt-4o",
    )

    models_result = await mod.ProviderQueryModelsAdapter(payload=models_payload).handle(context)
    test_result = await mod.ProviderQueryTestModelAdapter(payload=test_payload).handle(context)
    failover_result = await mod.ProviderQueryTestModelFailoverAdapter(
        payload=failover_payload
    ).handle(context)

    assert models_result == {"kind": "models"}
    assert test_result == {"kind": "test"}
    assert failover_result == {"kind": "failover"}

    models_helper.assert_awaited_once_with(models_payload, db)
    test_helper.assert_awaited_once_with(test_payload, db, user)
    failover_helper.assert_awaited_once_with(failover_payload, request, db, user)
