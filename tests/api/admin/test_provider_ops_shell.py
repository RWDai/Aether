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
        else:
            state[key] = value
    return state


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pipeline_result: Any,
) -> tuple[TestClient, list[dict[str, Any]]]:
    from src.api.admin.provider_ops import routes as mod

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


def _architecture_payload() -> dict[str, Any]:
    return {
        "architecture_id": "generic_api",
        "display_name": "Generic API",
        "description": "generic",
        "credentials_schema": {},
        "supported_auth_types": [],
        "supported_actions": [],
        "default_connector": None,
    }


def _status_payload() -> dict[str, Any]:
    return {
        "provider_id": "provider-1",
        "is_configured": True,
        "architecture_id": "generic_api",
        "connection_status": {
            "status": "connected",
            "auth_type": "api_key",
            "connected_at": None,
            "expires_at": None,
            "last_error": None,
        },
        "enabled_actions": ["balance"],
    }


def _config_payload() -> dict[str, Any]:
    return {
        "provider_id": "provider-1",
        "is_configured": True,
        "architecture_id": "generic_api",
        "base_url": "https://example.com",
        "connector": {
            "auth_type": "api_key",
            "config": {},
            "credentials": {},
        },
    }


def _verify_payload() -> dict[str, Any]:
    return {
        "success": True,
        "message": "ok",
        "data": {"verified": True},
        "updated_credentials": {"token": "masked"},
    }


def _action_payload() -> dict[str, Any]:
    return {
        "status": "success",
        "action_type": "balance",
        "data": {"balance": "1.23"},
        "message": "ok",
        "executed_at": "2026-03-26T00:00:00+00:00",
        "response_time_ms": 12,
        "cache_ttl_seconds": 60,
    }


def test_provider_ops_architectures_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=[_architecture_payload()])

    response = client.get("/api/admin/provider-ops/architectures")

    assert response.status_code == 200
    assert response.json() == [_architecture_payload()]
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsListArchitecturesAdapter",
            "mode": "admin",
            "adapter_state": {},
        }
    ]


def test_provider_ops_architecture_detail_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_architecture_payload())

    response = client.get("/api/admin/provider-ops/architectures/generic_api")

    assert response.status_code == 200
    assert response.json() == _architecture_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsGetArchitectureAdapter",
            "mode": "admin",
            "adapter_state": {"architecture_id": "generic_api"},
        }
    ]


def test_provider_ops_status_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_status_payload())

    response = client.get("/api/admin/provider-ops/providers/provider-1/status")

    assert response.status_code == 200
    assert response.json() == _status_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsStatusAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_config_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_config_payload())

    response = client.get("/api/admin/provider-ops/providers/provider-1/config")

    assert response.status_code == 200
    assert response.json() == _config_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsConfigAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_save_config_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"success": True, "message": "ok"})
    payload = {
        "architecture_id": "generic_api",
        "base_url": "https://example.com",
        "connector": {
            "auth_type": "api_key",
            "config": {"region": "us"},
            "credentials": {"api_key": "secret"},
        },
        "actions": {"balance": {"enabled": True, "config": {"refresh": True}}},
        "schedule": {"balance": "0 * * * *"},
    }

    response = client.put("/api/admin/provider-ops/providers/provider-1/config", json=payload)

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "ok"}
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsSaveConfigAdapter",
            "mode": "admin",
            "adapter_state": {
                "provider_id": "provider-1",
                "payload": payload,
            },
        }
    ]


def test_provider_ops_verify_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_verify_payload())
    payload = {
        "architecture_id": "generic_api",
        "base_url": "https://example.com",
        "connector": {
            "auth_type": "api_key",
            "config": {},
            "credentials": {"api_key": "secret"},
        },
        "actions": {},
        "schedule": {},
    }

    response = client.post("/api/admin/provider-ops/providers/provider-1/verify", json=payload)

    assert response.status_code == 200
    assert response.json() == _verify_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsVerifyAuthAdapter",
            "mode": "admin",
            "adapter_state": {
                "provider_id": "provider-1",
                "payload": payload,
            },
        }
    ]


def test_provider_ops_delete_config_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"success": True, "message": "ok"})

    response = client.delete("/api/admin/provider-ops/providers/provider-1/config")

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "ok"}
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsDeleteConfigAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_connect_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"success": True, "message": "ok"})

    response = client.post(
        "/api/admin/provider-ops/providers/provider-1/connect",
        json={"credentials": {"api_key": "secret"}},
    )

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "ok"}
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsConnectAdapter",
            "mode": "admin",
            "adapter_state": {
                "provider_id": "provider-1",
                "payload": {"credentials": {"api_key": "secret"}},
            },
        }
    ]


def test_provider_ops_disconnect_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result={"success": True, "message": "ok"})

    response = client.post("/api/admin/provider-ops/providers/provider-1/disconnect")

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "ok"}
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsDisconnectAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_execute_action_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_action_payload())

    response = client.post(
        "/api/admin/provider-ops/providers/provider-1/actions/balance",
        json={"config": {"refresh": True}},
    )

    assert response.status_code == 200
    assert response.json() == _action_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsExecuteActionAdapter",
            "mode": "admin",
            "adapter_state": {
                "provider_id": "provider-1",
                "action_type": "balance",
                "payload": {"config": {"refresh": True}},
            },
        }
    ]


def test_provider_ops_get_balance_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_action_payload())

    response = client.get("/api/admin/provider-ops/providers/provider-1/balance?refresh=false")

    assert response.status_code == 200
    assert response.json() == _action_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsGetBalanceAdapter",
            "mode": "admin",
            "adapter_state": {
                "provider_id": "provider-1",
                "refresh": False,
            },
        }
    ]


def test_provider_ops_refresh_balance_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_action_payload())

    response = client.post("/api/admin/provider-ops/providers/provider-1/balance")

    assert response.status_code == 200
    assert response.json() == _action_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsRefreshBalanceAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_checkin_route_is_pipeline_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _build_app(monkeypatch, pipeline_result=_action_payload())

    response = client.post("/api/admin/provider-ops/providers/provider-1/checkin")

    assert response.status_code == 200
    assert response.json() == _action_payload()
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsCheckinAdapter",
            "mode": "admin",
            "adapter_state": {"provider_id": "provider-1"},
        }
    ]


def test_provider_ops_batch_balance_route_is_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _build_app(
        monkeypatch,
        pipeline_result={"provider-1": _action_payload(), "provider-2": _action_payload()},
    )

    response = client.post(
        "/api/admin/provider-ops/batch/balance?provider_ids=provider-1&provider_ids=provider-2"
    )

    assert response.status_code == 200
    assert response.json() == {"provider-1": _action_payload(), "provider-2": _action_payload()}
    assert calls == [
        {
            "adapter_type": "AdminProviderOpsBatchBalanceAdapter",
            "mode": "admin",
            "adapter_state": {"provider_ids": ["provider-1", "provider-2"]},
        }
    ]
