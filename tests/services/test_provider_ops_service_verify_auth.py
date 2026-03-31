from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.config import config as runtime_config
from src.services.provider_ops.service import ProviderOpsService
from src.services.provider_ops.types import (
    ActionStatus,
    ConnectorAuthType,
    ProviderActionType,
    ProviderOpsConfig,
)
from src.services.request.rust_executor_client import RustExecutorSyncResult


class _FakeDB:
    new: tuple[Any, ...] = ()
    dirty: tuple[Any, ...] = ()
    deleted: tuple[Any, ...] = ()

    def in_transaction(self) -> bool:
        return False

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _FailingArchitecture:
    def get_verify_endpoint(self) -> str:
        return "/verify"

    async def prepare_verify_config(
        self,
        _base_url: str,
        _config: dict[str, Any],
        _credentials: dict[str, Any],
    ) -> dict[str, Any]:
        raise ValueError("invalid refresh token")

    def build_verify_headers(
        self,
        _config: dict[str, Any],
        _credentials: dict[str, Any],
    ) -> dict[str, str]:
        raise AssertionError("build_verify_headers should not be reached")


class _FakeRegistry:
    def __init__(self, architecture: Any) -> None:
        self._architecture = architecture

    def get_or_default(self, _architecture_id: str) -> Any:
        return self._architecture


class _ExplodingConnector:
    async def is_authenticated(self) -> bool:
        raise AssertionError("Python connector auth check should not run")

    def get_client(self) -> Any:
        raise AssertionError("Python connector client should not be created")


class _SuccessResult:
    def __init__(self) -> None:
        self.success = True
        self.quota = 200.0
        self.extra = {"window": "day"}

    def to_dict(self) -> dict[str, Any]:
        return {"success": True, "quota": self.quota}


class _SuccessArchitecture:
    default_action_configs = {ProviderActionType.QUERY_BALANCE: {"quota_divisor": 100}}

    def get_verify_endpoint(self) -> str:
        return "/verify"

    async def prepare_verify_config(
        self,
        _base_url: str,
        _config: dict[str, Any],
        _credentials: dict[str, Any],
    ) -> dict[str, Any]:
        return {}

    def build_verify_headers(
        self,
        _config: dict[str, Any],
        _credentials: dict[str, Any],
    ) -> dict[str, str]:
        return {"authorization": "Bearer test"}

    def parse_verify_response(self, status_code: int, data: dict[str, Any]) -> _SuccessResult:
        assert status_code == 200
        assert data == {"ok": True}
        return _SuccessResult()


@pytest.mark.asyncio
async def test_verify_auth_returns_failure_when_prepare_verify_config_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ProviderOpsService(_FakeDB())
    architecture = _FailingArchitecture()

    monkeypatch.setattr(
        "src.services.provider_ops.service.get_registry",
        lambda: _FakeRegistry(architecture),
    )

    result = await service.verify_auth(
        base_url="https://example.com",
        architecture_id="sub2api",
        auth_type=ConnectorAuthType.SESSION_LOGIN,
        config={},
        credentials={"refresh_token": "stale-token"},
    )

    assert result == {"success": False, "message": "invalid refresh token"}


@pytest.mark.asyncio
async def test_verify_auth_prefers_rust_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.services.provider_ops import service as module
    from src.services.request import rust_executor_client as rust_module

    service = ProviderOpsService(_FakeDB())
    architecture = _SuccessArchitecture()

    monkeypatch.setattr(
        module,
        "get_registry",
        lambda: _FakeRegistry(architecture),
    )
    monkeypatch.setattr(module.config, "executor_backend", "rust")
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_ops_proxy_config_async",
        AsyncMock(return_value=(None, "node-1")),
    )

    cache_balance = AsyncMock()
    monkeypatch.setattr(service, "_cache_balance_from_verify", cache_balance)

    captured: dict[str, Any] = {}

    async def _fake_execute_sync_json(self: object, plan: Any) -> RustExecutorSyncResult:
        captured["plan"] = plan
        return RustExecutorSyncResult(
            status_code=200,
            response_json={"ok": True},
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(rust_module.RustExecutorClient, "execute_sync_json", _fake_execute_sync_json)

    result = await service.verify_auth(
        base_url="https://example.com",
        architecture_id="sub2api",
        auth_type=ConnectorAuthType.SESSION_LOGIN,
        config={},
        credentials={"access_token": "token"},
        provider_id="provider-1",
    )

    assert result == {"success": True, "quota": 200.0}
    assert captured["plan"].method == "GET"
    assert captured["plan"].url == "https://example.com/verify"
    assert captured["plan"].proxy is not None
    assert captured["plan"].proxy.mode == "tunnel"
    assert captured["plan"].proxy.node_id == "node-1"
    cache_balance.assert_awaited_once_with("provider-1", 2.0, {"window": "day"})


@pytest.mark.asyncio
async def test_verify_auth_returns_explicit_failure_when_rust_verifier_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ProviderOpsService(_FakeDB())
    architecture = _SuccessArchitecture()

    monkeypatch.setattr(
        "src.services.provider_ops.service.get_registry",
        lambda: _FakeRegistry(architecture),
    )
    monkeypatch.setattr(
        service,
        "_try_rust_verify_response",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.proxy_node.resolver.resolve_ops_proxy_config_async",
        AsyncMock(return_value=(None, None)),
    )

    result = await service.verify_auth(
        base_url="https://example.com",
        architecture_id="sub2api",
        auth_type=ConnectorAuthType.SESSION_LOGIN,
        config={},
        credentials={"access_token": "token"},
    )

    assert result == {"success": False, "message": "认证验证仅支持 Rust executor"}


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["python", "rust"])
async def test_connect_returns_explicit_failure_without_python_connector(
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
) -> None:
    service = ProviderOpsService(_FakeDB())
    service._connectors["provider-1"] = object()  # type: ignore[assignment]
    monkeypatch.setattr(runtime_config, "executor_backend", backend)

    monkeypatch.setattr(
        service,
        "_get_provider",
        lambda _provider_id: SimpleNamespace(base_url="https://example.com"),
    )
    monkeypatch.setattr(
        service,
        "get_config",
        lambda _provider_id: ProviderOpsConfig(
            architecture_id="sub2api",
            base_url="https://example.com",
            connector_auth_type=ConnectorAuthType.API_KEY,
        ),
    )
    monkeypatch.setattr(
        "src.services.provider_ops.service.get_registry",
        lambda: (_ for _ in ()).throw(
            AssertionError("Python provider connector registry should not be used")
        ),
    )

    success, message = await service.connect("provider-1", {"api_key": "token"})

    assert success is False
    assert message == "Provider 连接仅支持 Rust executor"
    assert "provider-1" not in service._connectors


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["python", "rust"])
async def test_execute_action_returns_not_supported_without_python_connector(
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
) -> None:
    service = ProviderOpsService(_FakeDB())
    service._connectors["provider-1"] = _ExplodingConnector()  # type: ignore[assignment]
    monkeypatch.setattr(runtime_config, "executor_backend", backend)

    monkeypatch.setattr(
        service,
        "get_config",
        lambda _provider_id: ProviderOpsConfig(
            architecture_id="sub2api",
            base_url="https://example.com",
            connector_auth_type=ConnectorAuthType.API_KEY,
        ),
    )
    monkeypatch.setattr(
        "src.services.provider_ops.service.get_registry",
        lambda: (_ for _ in ()).throw(
            AssertionError("Python provider action architecture should not be used")
        ),
    )

    result = await service.execute_action("provider-1", ProviderActionType.QUERY_BALANCE)

    assert result.status == ActionStatus.NOT_SUPPORTED
    assert result.action_type == ProviderActionType.QUERY_BALANCE
    assert result.message == "Provider 操作仅支持 Rust executor"
