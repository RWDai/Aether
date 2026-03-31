from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.main as main_module
import src.services.system.maintenance_scheduler as maintenance_scheduler_module
from src.config.settings import config
from src.services.system.maintenance_scheduler import MaintenanceScheduler


@pytest.mark.asyncio
async def test_maintenance_scheduler_start_skips_startup_task_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "maintenance_startup_tasks_enabled", False)

    scheduler = MaintenanceScheduler()

    created = False

    def fake_create_task(coro):  # type: ignore[no-untyped-def]
        nonlocal created
        created = True
        if inspect.iscoroutine(coro):
            coro.close()
        return object()

    monkeypatch.setattr(maintenance_scheduler_module.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(scheduler, "_get_checkin_time", lambda: (1, 5))
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(
            add_cron_job=lambda *args, **kwargs: None,
            add_interval_job=lambda *args, **kwargs: None,
        ),
    )

    await scheduler.start()

    assert created is False


@pytest.mark.asyncio
async def test_maintenance_scheduler_start_skips_startup_task_when_no_python_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "maintenance_startup_tasks_enabled", True)
    monkeypatch.setattr(config, "pending_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", False)

    scheduler = MaintenanceScheduler()
    safe_create_task = MagicMock()

    monkeypatch.setattr("src.utils.async_utils.safe_create_task", safe_create_task)
    monkeypatch.setattr(scheduler, "_get_checkin_time", lambda: (1, 5))
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(
            add_cron_job=lambda *args, **kwargs: None,
            add_interval_job=lambda *args, **kwargs: None,
        ),
    )

    await scheduler.start()

    safe_create_task.assert_not_called()
    assert scheduler._startup_task is None


@pytest.mark.asyncio
async def test_maintenance_scheduler_stop_cancels_startup_task_and_removes_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()
    scheduler.running = True
    scheduler._startup_task = asyncio.create_task(asyncio.sleep(3600))

    expected_job_ids = [
        "stats_aggregation",
        "stats_hourly_aggregation",
        "wallet_daily_usage_aggregation",
        "usage_cleanup",
        "pool_monitor",
        "http_client_idle_cleanup",
        "pending_cleanup",
        "audit_cleanup",
        "gemini_file_mapping_cleanup",
        "candidate_cleanup",
        "db_maintenance",
        "antigravity_ua_refresh",
        scheduler.CHECKIN_JOB_ID,
    ]
    scheduler._registered_job_ids = list(expected_job_ids)
    removed_jobs: list[str] = []

    monkeypatch.setattr(
        maintenance_scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(remove_job=lambda job_id: removed_jobs.append(job_id)),
    )

    await scheduler.stop()

    assert scheduler.running is False
    assert scheduler._startup_task is None
    assert set(removed_jobs) == set(expected_job_ids)
    assert scheduler._registered_job_ids == []


@pytest.mark.asyncio
async def test_maintenance_scheduler_start_skips_rust_owned_maintenance_jobs_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "maintenance_startup_tasks_enabled", False)
    monkeypatch.setattr(config, "audit_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "db_maintenance_python_enabled", False)
    monkeypatch.setattr(config, "gemini_file_mapping_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "http_client_idle_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "pending_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "pool_monitor_python_enabled", False)
    monkeypatch.setattr(config, "provider_checkin_python_enabled", False)
    monkeypatch.setattr(config, "request_candidate_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "stats_aggregation_python_enabled", False)
    monkeypatch.setattr(config, "stats_hourly_aggregation_python_enabled", False)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", False)
    monkeypatch.setattr(config, "usage_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "wallet_daily_usage_aggregation_python_enabled", False)

    scheduler = MaintenanceScheduler()
    cron_job_ids: list[str] = []
    interval_job_ids: list[str] = []

    monkeypatch.setattr(scheduler, "_get_checkin_time", lambda: (1, 5))
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(
            add_cron_job=lambda *, job_id, **kwargs: cron_job_ids.append(job_id),
            add_interval_job=lambda *, job_id, **kwargs: interval_job_ids.append(job_id),
        ),
    )

    await scheduler.start()

    assert "stats_aggregation" not in cron_job_ids
    assert "audit_cleanup" not in cron_job_ids
    assert "candidate_cleanup" not in cron_job_ids
    assert "db_maintenance" not in cron_job_ids
    assert "stats_hourly_aggregation" not in cron_job_ids
    assert "pool_monitor" not in interval_job_ids
    assert "http_client_idle_cleanup" not in interval_job_ids
    assert "usage_cleanup" not in cron_job_ids
    assert "wallet_daily_usage_aggregation" not in cron_job_ids
    assert scheduler.CHECKIN_JOB_ID not in cron_job_ids
    assert "antigravity_ua_refresh" not in interval_job_ids
    assert "gemini_file_mapping_cleanup" not in interval_job_ids
    assert "pending_cleanup" not in interval_job_ids
    assert "stats_aggregation" not in scheduler._registered_job_ids
    assert "audit_cleanup" not in scheduler._registered_job_ids
    assert "candidate_cleanup" not in scheduler._registered_job_ids
    assert "db_maintenance" not in scheduler._registered_job_ids
    assert "stats_hourly_aggregation" not in scheduler._registered_job_ids
    assert "pool_monitor" not in scheduler._registered_job_ids
    assert "http_client_idle_cleanup" not in scheduler._registered_job_ids
    assert "usage_cleanup" not in scheduler._registered_job_ids
    assert "wallet_daily_usage_aggregation" not in scheduler._registered_job_ids
    assert scheduler.CHECKIN_JOB_ID not in scheduler._registered_job_ids
    assert "antigravity_ua_refresh" not in scheduler._registered_job_ids
    assert "gemini_file_mapping_cleanup" not in scheduler._registered_job_ids
    assert "pending_cleanup" not in scheduler._registered_job_ids


@pytest.mark.asyncio
async def test_maintenance_scheduler_start_registers_python_owned_maintenance_jobs_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "maintenance_startup_tasks_enabled", False)
    monkeypatch.setattr(config, "audit_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "db_maintenance_python_enabled", True)
    monkeypatch.setattr(config, "gemini_file_mapping_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "http_client_idle_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "pending_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "pool_monitor_python_enabled", True)
    monkeypatch.setattr(config, "provider_checkin_python_enabled", True)
    monkeypatch.setattr(config, "request_candidate_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "stats_aggregation_python_enabled", True)
    monkeypatch.setattr(config, "stats_hourly_aggregation_python_enabled", True)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", True)
    monkeypatch.setattr(config, "usage_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "wallet_daily_usage_aggregation_python_enabled", True)

    scheduler = MaintenanceScheduler()
    cron_job_ids: list[str] = []
    interval_job_ids: list[str] = []

    monkeypatch.setattr(scheduler, "_get_checkin_time", lambda: (1, 5))
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(
            add_cron_job=lambda *, job_id, **kwargs: cron_job_ids.append(job_id),
            add_interval_job=lambda *, job_id, **kwargs: interval_job_ids.append(job_id),
        ),
    )

    await scheduler.start()

    assert "stats_aggregation" in cron_job_ids
    assert "audit_cleanup" in cron_job_ids
    assert "candidate_cleanup" in cron_job_ids
    assert "db_maintenance" in cron_job_ids
    assert "stats_hourly_aggregation" in cron_job_ids
    assert "pool_monitor" in interval_job_ids
    assert "http_client_idle_cleanup" in interval_job_ids
    assert "usage_cleanup" in cron_job_ids
    assert "wallet_daily_usage_aggregation" in cron_job_ids
    assert scheduler.CHECKIN_JOB_ID in cron_job_ids
    assert "antigravity_ua_refresh" in interval_job_ids
    assert "gemini_file_mapping_cleanup" in interval_job_ids
    assert "pending_cleanup" in interval_job_ids
    assert "stats_aggregation" in scheduler._registered_job_ids
    assert "audit_cleanup" in scheduler._registered_job_ids
    assert "candidate_cleanup" in scheduler._registered_job_ids
    assert "db_maintenance" in scheduler._registered_job_ids
    assert "stats_hourly_aggregation" in scheduler._registered_job_ids
    assert "pool_monitor" in scheduler._registered_job_ids
    assert "http_client_idle_cleanup" in scheduler._registered_job_ids
    assert "usage_cleanup" in scheduler._registered_job_ids
    assert "wallet_daily_usage_aggregation" in scheduler._registered_job_ids
    assert scheduler.CHECKIN_JOB_ID in scheduler._registered_job_ids
    assert "antigravity_ua_refresh" in scheduler._registered_job_ids
    assert "gemini_file_mapping_cleanup" in scheduler._registered_job_ids
    assert "pending_cleanup" in scheduler._registered_job_ids


@pytest.mark.asyncio
async def test_maintenance_scheduler_startup_tasks_skip_python_pending_cleanup_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()
    refresh_user_agent = AsyncMock()
    perform_pending_cleanup = AsyncMock()

    monkeypatch.setattr(config, "pending_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", False)
    monkeypatch.setattr(maintenance_scheduler_module.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.client.refresh_user_agent",
        refresh_user_agent,
    )
    monkeypatch.setattr(scheduler, "_perform_pending_cleanup", perform_pending_cleanup)

    await scheduler._run_startup_tasks()

    refresh_user_agent.assert_not_awaited()
    perform_pending_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_maintenance_scheduler_startup_tasks_run_python_pending_cleanup_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()
    refresh_user_agent = AsyncMock()
    perform_pending_cleanup = AsyncMock()

    monkeypatch.setattr(config, "pending_cleanup_python_enabled", True)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", True)
    monkeypatch.setattr(maintenance_scheduler_module.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        "src.services.provider.adapters.antigravity.client.refresh_user_agent",
        refresh_user_agent,
    )
    monkeypatch.setattr(scheduler, "_perform_pending_cleanup", perform_pending_cleanup)

    await scheduler._run_startup_tasks()

    refresh_user_agent.assert_awaited_once()
    perform_pending_cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_background_services_skips_python_maintenance_scheduler_when_unused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, _task_poller, _model_fetch_scheduler = (
        _patch_background_services_dependencies(monkeypatch)
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)
    monkeypatch.setattr(config, "model_fetch_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "audit_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "antigravity_ua_refresh_python_enabled", False)
    monkeypatch.setattr(config, "db_maintenance_python_enabled", False)
    monkeypatch.setattr(config, "gemini_file_mapping_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "http_client_idle_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "pending_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "pool_monitor_python_enabled", False)
    monkeypatch.setattr(config, "provider_checkin_python_enabled", False)
    monkeypatch.setattr(config, "request_candidate_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "stats_aggregation_python_enabled", False)
    monkeypatch.setattr(config, "stats_hourly_aggregation_python_enabled", False)
    monkeypatch.setattr(config, "usage_cleanup_python_enabled", False)
    monkeypatch.setattr(config, "wallet_daily_usage_aggregation_python_enabled", False)

    await main_module._start_background_services(state)

    assert "maintenance_scheduler" not in coordinator.acquire_calls
    assert state.maintenance_scheduler is None


@pytest.mark.asyncio
async def test_start_background_services_starts_python_maintenance_scheduler_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, _task_poller, _model_fetch_scheduler = (
        _patch_background_services_dependencies(
            monkeypatch,
            acquire_results={"maintenance_scheduler": True},
        )
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)
    monkeypatch.setattr(config, "model_fetch_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "http_client_idle_cleanup_python_enabled", True)

    await main_module._start_background_services(state)

    assert "maintenance_scheduler" in coordinator.acquire_calls
    assert "maintenance_scheduler" in coordinator.registered_callbacks
    assert state.maintenance_scheduler is not None


def _patch_core_infrastructure_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "log_startup_warnings", lambda: None)
    monkeypatch.setattr(main_module, "init_db", lambda: None)
    monkeypatch.setattr(main_module, "initialize_providers", AsyncMock())
    monkeypatch.setattr(
        "src.clients.redis_client.get_redis_client",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.rate_limit.concurrency_manager.get_concurrency_manager",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "src.services.rate_limit.user_rpm_limiter.get_user_rpm_limiter",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr("src.core.batch_committer.init_batch_committer", AsyncMock())
    monkeypatch.setattr(
        "src.services.provider_keys.codex_quota_sync_dispatcher.init_codex_quota_sync_dispatcher",
        AsyncMock(),
    )


def _patch_python_host_shutdown_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.services.provider_keys.codex_quota_sync_dispatcher.shutdown_codex_quota_sync_dispatcher",
        AsyncMock(),
    )
    monkeypatch.setattr("src.core.batch_committer.shutdown_batch_committer", AsyncMock())
    monkeypatch.setattr("src.clients.redis_client.close_redis_client", AsyncMock())
    monkeypatch.setattr(main_module, "close_http_clients", AsyncMock())


class _FakeStartupTaskCoordinator:
    def __init__(self, acquire_results: dict[str, bool] | None = None) -> None:
        self.acquire_results = acquire_results or {}
        self.acquire_calls: list[str] = []
        self.registered_callbacks: list[str] = []
        self.released: list[str] = []

    async def acquire(self, name: str) -> bool:
        self.acquire_calls.append(name)
        return self.acquire_results.get(name, False)

    async def release(self, name: str) -> None:
        self.released.append(name)

    def register_lock_lost_callback(self, name: str, _callback: Any) -> None:
        self.registered_callbacks.append(name)


def _patch_background_services_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    acquire_results: dict[str, bool] | None = None,
) -> tuple[_FakeStartupTaskCoordinator, Any, Any, Any]:
    coordinator = _FakeStartupTaskCoordinator(acquire_results)
    quota_scheduler = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    maintenance_scheduler = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    model_fetch_scheduler = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    pool_quota_probe_scheduler = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    task_poller = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    task_scheduler = SimpleNamespace(start=MagicMock())

    monkeypatch.setattr(
        "src.utils.task_coordinator.StartupTaskCoordinator",
        lambda _redis: coordinator,
    )
    monkeypatch.setattr(
        "src.services.usage.quota_scheduler.get_quota_scheduler",
        lambda: quota_scheduler,
    )
    monkeypatch.setattr(
        "src.services.system.maintenance_scheduler.get_maintenance_scheduler",
        lambda: maintenance_scheduler,
    )
    monkeypatch.setattr(
        "src.services.model.fetch_scheduler.get_model_fetch_scheduler",
        lambda: model_fetch_scheduler,
    )
    monkeypatch.setattr(
        "src.services.provider_keys.pool_quota_probe_scheduler.get_pool_quota_probe_scheduler",
        lambda: pool_quota_probe_scheduler,
    )
    monkeypatch.setattr(
        "src.services.task.polling.task_poller.get_task_poller",
        lambda: task_poller,
    )
    monkeypatch.setattr(
        "src.services.system.scheduler.get_scheduler",
        lambda: task_scheduler,
    )

    return coordinator, quota_scheduler, task_poller, model_fetch_scheduler


@pytest.mark.asyncio
async def test_initialize_core_infrastructure_skips_python_usage_consumer_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_core_infrastructure_dependencies(monkeypatch)
    started = AsyncMock()

    monkeypatch.setattr(config, "require_redis", False)
    monkeypatch.setattr(config, "usage_queue_enabled", True)
    monkeypatch.setattr(config, "usage_queue_python_consumer_enabled", False)
    monkeypatch.setattr("src.services.usage.consumer_streams.start_usage_queue_consumer", started)

    await main_module._initialize_core_infrastructure(main_module.LifecycleState())

    started.assert_not_awaited()


@pytest.mark.asyncio
async def test_initialize_core_infrastructure_starts_python_usage_consumer_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_core_infrastructure_dependencies(monkeypatch)
    started = AsyncMock()

    monkeypatch.setattr(config, "require_redis", False)
    monkeypatch.setattr(config, "usage_queue_enabled", True)
    monkeypatch.setattr(config, "usage_queue_python_consumer_enabled", True)
    monkeypatch.setattr("src.services.usage.consumer_streams.start_usage_queue_consumer", started)

    await main_module._initialize_core_infrastructure(main_module.LifecycleState())

    started.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_python_host_shutdown_skips_python_usage_consumer_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_python_host_shutdown_dependencies(monkeypatch)
    stopped = AsyncMock()

    monkeypatch.setattr(config, "usage_queue_enabled", True)
    monkeypatch.setattr(config, "usage_queue_python_consumer_enabled", False)
    monkeypatch.setattr("src.services.usage.consumer_streams.stop_usage_queue_consumer", stopped)

    await main_module._run_python_host_shutdown(main_module.LifecycleState())

    stopped.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_python_host_shutdown_stops_python_usage_consumer_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_python_host_shutdown_dependencies(monkeypatch)
    stopped = AsyncMock()

    monkeypatch.setattr(config, "usage_queue_enabled", True)
    monkeypatch.setattr(config, "usage_queue_python_consumer_enabled", True)
    monkeypatch.setattr("src.services.usage.consumer_streams.stop_usage_queue_consumer", stopped)

    await main_module._run_python_host_shutdown(main_module.LifecycleState())

    stopped.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_background_services_skips_python_video_poller_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, task_poller, _model_fetch_scheduler = _patch_background_services_dependencies(
        monkeypatch
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)

    await main_module._start_background_services(state)

    assert "task_poller:video" not in coordinator.acquire_calls
    task_poller.start.assert_not_awaited()
    assert state.task_poller is None


@pytest.mark.asyncio
async def test_start_background_services_starts_python_video_poller_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, task_poller, _model_fetch_scheduler = _patch_background_services_dependencies(
        monkeypatch,
        acquire_results={"task_poller:video": True},
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "video_task_python_poller_enabled", True)

    await main_module._start_background_services(state)

    assert "task_poller:video" in coordinator.acquire_calls
    assert "task_poller:video" in coordinator.registered_callbacks
    task_poller.start.assert_awaited_once()
    assert state.task_poller is task_poller


@pytest.mark.asyncio
async def test_start_background_services_skips_python_quota_scheduler_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, quota_scheduler, _task_poller, _model_fetch_scheduler = _patch_background_services_dependencies(
        monkeypatch
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)

    await main_module._start_background_services(state)

    assert "quota_scheduler" not in coordinator.acquire_calls
    quota_scheduler.start.assert_not_awaited()
    assert state.quota_scheduler is None


@pytest.mark.asyncio
async def test_start_background_services_starts_python_quota_scheduler_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, quota_scheduler, _task_poller, _model_fetch_scheduler = _patch_background_services_dependencies(
        monkeypatch,
        acquire_results={"quota_scheduler": True},
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", True)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)

    await main_module._start_background_services(state)

    assert "quota_scheduler" in coordinator.acquire_calls
    assert "quota_scheduler" in coordinator.registered_callbacks
    quota_scheduler.start.assert_awaited_once()
    assert state.quota_scheduler is quota_scheduler


@pytest.mark.asyncio
async def test_start_background_services_skips_python_model_fetch_scheduler_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, _task_poller, model_fetch_scheduler = (
        _patch_background_services_dependencies(monkeypatch)
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "model_fetch_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)

    await main_module._start_background_services(state)

    assert "model_fetch_scheduler" not in coordinator.acquire_calls
    model_fetch_scheduler.start.assert_not_awaited()
    assert state.model_fetch_scheduler is None


@pytest.mark.asyncio
async def test_start_background_services_starts_python_model_fetch_scheduler_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _quota_scheduler, _task_poller, model_fetch_scheduler = (
        _patch_background_services_dependencies(
            monkeypatch,
            acquire_results={"model_fetch_scheduler": True},
        )
    )
    state = main_module.LifecycleState()

    monkeypatch.setattr(config, "quota_scheduler_python_enabled", False)
    monkeypatch.setattr(config, "model_fetch_scheduler_python_enabled", True)
    monkeypatch.setattr(config, "video_task_python_poller_enabled", False)

    await main_module._start_background_services(state)

    assert "model_fetch_scheduler" in coordinator.acquire_calls
    assert "model_fetch_scheduler" in coordinator.registered_callbacks
    model_fetch_scheduler.start.assert_awaited_once()
    assert state.model_fetch_scheduler is model_fetch_scheduler


@pytest.mark.asyncio
async def test_stop_service_on_lock_lost_keeps_state_when_stop_fails() -> None:
    state = main_module.LifecycleState()
    service = SimpleNamespace()
    state.quota_scheduler = cast(Any, service)

    async def fail_stop() -> None:
        raise RuntimeError("boom")

    await main_module._stop_service_on_lock_lost(
        state,
        lock_name="quota_scheduler",
        service_name="月卡额度重置调度器",
        state_attr="quota_scheduler",
        stop=fail_stop,
    )

    assert state.quota_scheduler is service


@pytest.mark.asyncio
async def test_stop_service_on_lock_lost_clears_state_after_success() -> None:
    state = main_module.LifecycleState()
    service = SimpleNamespace()
    state.quota_scheduler = cast(Any, service)
    stopped = False

    async def stop() -> None:
        nonlocal stopped
        stopped = True

    await main_module._stop_service_on_lock_lost(
        state,
        lock_name="quota_scheduler",
        service_name="月卡额度重置调度器",
        state_attr="quota_scheduler",
        stop=stop,
    )

    assert stopped is True
    assert state.quota_scheduler is None


def test_http_client_idle_cleanup_interval_env_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_CLIENT_IDLE_CLEANUP_INTERVAL_MINUTES", "bad")
    assert MaintenanceScheduler._get_http_client_idle_cleanup_interval_minutes() == 5


@pytest.mark.asyncio
async def test_candidate_cleanup_uses_dedicated_retention_and_batch_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()

    class _FakeLoop:
        async def run_in_executor(self, _executor, func):  # type: ignore[no-untyped-def]
            return func()

    class _ConfigSession:
        def close(self) -> None:
            return None

    class _BatchSession:
        def __init__(self, ids: list[str]) -> None:
            self.ids = ids
            self.closed = False
            self.committed = False
            self.query_obj = MagicMock()
            filtered = self.query_obj.filter.return_value
            filtered.order_by.return_value.limit.return_value.all.return_value = [
                SimpleNamespace(id=value) for value in ids
            ]

        def query(self, _model):  # type: ignore[no-untyped-def]
            return self.query_obj

        def execute(self, _statement):  # type: ignore[no-untyped-def]
            return SimpleNamespace(rowcount=len(self.ids))

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            raise AssertionError("rollback should not be called")

        def close(self) -> None:
            self.closed = True

    config_session = _ConfigSession()
    batch_one = _BatchSession(["candidate-1", "candidate-2"])
    batch_two = _BatchSession([])
    sessions = iter([config_session, batch_one, batch_two])

    def fake_create_session():  # type: ignore[no-untyped-def]
        return next(sessions)

    config_values = {
        "enable_auto_cleanup": True,
        "request_candidates_retention_days": 21,
        "request_candidates_cleanup_batch_size": 2,
    }

    monkeypatch.setattr(maintenance_scheduler_module, "create_session", fake_create_session)
    monkeypatch.setattr(
        maintenance_scheduler_module.SystemConfigService,
        "get_config",
        lambda _db, key, default=None: config_values.get(key, default),
    )
    monkeypatch.setattr(
        maintenance_scheduler_module.asyncio, "get_running_loop", lambda: _FakeLoop()
    )

    await scheduler._perform_candidate_cleanup()

    batch_one.query_obj.filter.return_value.order_by.return_value.limit.return_value.all.assert_called_once()
    batch_one.query_obj.filter.return_value.order_by.return_value.limit.assert_called_once_with(2)
    assert batch_one.committed is True
    assert batch_one.closed is True
    assert batch_two.closed is True


def test_cleanup_body_fields_batches_records_with_single_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()

    class _BatchSession:
        def __init__(self, records: list[SimpleNamespace]) -> None:
            self.records = records
            self.closed = False
            self.committed = False
            self.executed = 0
            self.query_obj = MagicMock()
            filtered = self.query_obj.filter.return_value
            filtered.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
                records
            )

        def query(self, *args):  # type: ignore[no-untyped-def]
            self.query_args = args
            return self.query_obj

        def execute(self, _statement):  # type: ignore[no-untyped-def]
            self.executed += 1
            return SimpleNamespace(rowcount=1)

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            raise AssertionError("rollback should not be called")

        def close(self) -> None:
            self.closed = True

    batch_one = _BatchSession(
        [
            SimpleNamespace(
                id="usage-1",
                request_body={"hello": "world"},
                response_body=None,
                provider_request_body=None,
                client_response_body=None,
            ),
            SimpleNamespace(
                id="usage-2",
                request_body=None,
                response_body={"ok": True},
                provider_request_body=None,
                client_response_body=None,
            ),
        ]
    )
    batch_two = _BatchSession([])
    sessions = iter([batch_one, batch_two])

    monkeypatch.setattr(
        maintenance_scheduler_module,
        "create_session",
        lambda: next(sessions),
    )
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "compress_json",
        lambda payload: f"compressed:{payload}".encode(),
    )

    compressed = scheduler._cleanup_body_fields(
        cutoff_time=SimpleNamespace(),  # type: ignore[arg-type]
        batch_size=1000,
    )

    assert compressed == 2
    assert len(batch_one.query_args) == 5
    assert batch_one.executed == 2
    assert batch_one.committed is True
    assert batch_one.closed is True
    assert batch_two.closed is True


def test_cleanup_header_fields_clears_client_response_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()

    class _BatchSession:
        def __init__(self, ids: list[str]) -> None:
            self.ids = ids
            self.closed = False
            self.committed = False
            self.query_obj = MagicMock()
            self.filtered_by_time = MagicMock()
            self.filtered_by_headers = MagicMock()
            self.query_obj.filter.return_value = self.filtered_by_time
            self.filtered_by_time.filter.return_value = self.filtered_by_headers
            self.filtered_by_headers.order_by.return_value.limit.return_value.all.return_value = [
                SimpleNamespace(id=value) for value in ids
            ]
            self.executed_statements: list[str] = []

        def query(self, *args):  # type: ignore[no-untyped-def]
            self.query_args = args
            return self.query_obj

        def execute(self, statement):  # type: ignore[no-untyped-def]
            self.executed_statements.append(str(statement))
            return SimpleNamespace(rowcount=len(self.ids))

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            raise AssertionError("rollback should not be called")

        def close(self) -> None:
            self.closed = True

    batch_one = _BatchSession(["usage-1"])
    batch_two = _BatchSession([])
    sessions = iter([batch_one, batch_two])

    monkeypatch.setattr(
        maintenance_scheduler_module,
        "create_session",
        lambda: next(sessions),
    )

    cleaned = scheduler._cleanup_header_fields(
        cutoff_time=SimpleNamespace(),  # type: ignore[arg-type]
        batch_size=1000,
    )

    header_filter = str(batch_one.filtered_by_time.filter.call_args.args[0])

    assert cleaned == 1
    assert batch_one.query_args == (maintenance_scheduler_module.Usage.id,)
    assert "client_response_headers" in header_filter
    assert "client_response_headers" in batch_one.executed_statements[0]
    assert batch_one.committed is True
    assert batch_one.closed is True
    assert batch_two.closed is True


@pytest.mark.asyncio
async def test_perform_cleanup_deletes_first_and_uses_non_overlapping_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = MaintenanceScheduler()
    fixed_now = datetime(2026, 3, 18, 3, 0, 0, tzinfo=timezone.utc)

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:  # type: ignore[override]
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    class _FakeLoop:
        async def run_in_executor(self, _executor, func):  # type: ignore[no-untyped-def]
            return func()

    class _ConfigSession:
        def close(self) -> None:
            return None

    calls: list[tuple[str, datetime, int, datetime | None]] = []

    def _record(name: str, count: int) -> Callable[..., int]:
        def _inner(
            cutoff_time: datetime,
            batch_size: int,
            *,
            newer_than: datetime | None = None,
        ) -> int:
            calls.append((name, cutoff_time, batch_size, newer_than))
            return count

        return _inner

    config_values = {
        "enable_auto_cleanup": True,
        "detail_log_retention_days": 7,
        "compressed_log_retention_days": 30,
        "header_retention_days": 90,
        "log_retention_days": 365,
        "cleanup_batch_size": 123,
        "auto_delete_expired_keys": False,
    }

    def _delete_old_records(cutoff_time: datetime, batch_size: int) -> int:
        calls.append(("delete", cutoff_time, batch_size, None))
        return 5

    monkeypatch.setattr(maintenance_scheduler_module, "datetime", _FakeDateTime)
    monkeypatch.setattr(
        maintenance_scheduler_module.asyncio, "get_running_loop", lambda: _FakeLoop()
    )
    monkeypatch.setattr(
        maintenance_scheduler_module,
        "create_session",
        lambda: _ConfigSession(),
    )
    monkeypatch.setattr(
        maintenance_scheduler_module.SystemConfigService,
        "get_config",
        lambda _db, key, default=None: config_values.get(key, default),
    )
    monkeypatch.setattr(
        scheduler,
        "_delete_old_records",
        _delete_old_records,
    )
    monkeypatch.setattr(
        scheduler,
        "_cleanup_header_fields",
        _record("header", 4),
    )
    monkeypatch.setattr(
        scheduler,
        "_cleanup_stale_body_fields",
        _record("body", 3),
    )
    monkeypatch.setattr(
        scheduler,
        "_cleanup_body_fields",
        _record("compress", 2),
    )
    monkeypatch.setattr(
        maintenance_scheduler_module.ApiKeyService,
        "cleanup_expired_keys",
        lambda _db, auto_delete=False: 0,
    )

    await scheduler._perform_cleanup()

    detail_cutoff = fixed_now - timedelta(days=7)
    compressed_cutoff = fixed_now - timedelta(days=30)
    header_cutoff = fixed_now - timedelta(days=90)
    log_cutoff = fixed_now - timedelta(days=365)

    assert calls == [
        ("delete", log_cutoff, 123, None),
        ("header", header_cutoff, 123, log_cutoff),
        ("body", compressed_cutoff, 123, log_cutoff),
        ("compress", detail_cutoff, 123, compressed_cutoff),
    ]
