from __future__ import annotations

from typing import Any

import pytest

from src.utils.task_coordinator import StartupTaskCoordinator


class _FakeRedis:
    def __init__(self, eval_results: list[int] | None = None, set_result: bool = True) -> None:
        self.eval_results = list(eval_results or [])
        self.eval_calls: list[tuple[Any, ...]] = []
        self.set_result = set_result
        self.set_calls: list[tuple[Any, ...]] = []

    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        self.eval_calls.append((script, numkeys, *args))
        if self.eval_results:
            return self.eval_results.pop(0)
        return 1

    async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool:
        self.set_calls.append((key, value, nx, ex))
        return self.set_result


class _FakeTask:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


@pytest.mark.asyncio
async def test_startup_task_coordinator_acquire_starts_refresh_for_redis_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis(eval_results=[1])
    coordinator = StartupTaskCoordinator(redis)
    started: list[tuple[str, int]] = []

    monkeypatch.setattr(
        coordinator,
        "_start_refresh_task",
        lambda name, ttl: started.append((name, ttl)),
    )

    acquired = await coordinator.acquire("maintenance_scheduler", ttl=120)

    assert acquired is True
    assert "maintenance_scheduler" in coordinator._tokens
    assert started == [("maintenance_scheduler", 120)]


@pytest.mark.asyncio
async def test_startup_task_coordinator_refresh_lock_extends_matching_token() -> None:
    redis = _FakeRedis(eval_results=[1])
    coordinator = StartupTaskCoordinator(redis)
    coordinator._tokens["maintenance_scheduler"] = "token-1"

    refreshed = await coordinator._refresh_lock("maintenance_scheduler", ttl=180)

    assert refreshed is True
    assert len(redis.eval_calls) == 1
    _script, numkeys, key, token, ttl = redis.eval_calls[0]
    assert numkeys == 1
    assert key == "task_lock:maintenance_scheduler"
    assert token == "token-1"
    assert ttl == 180


@pytest.mark.asyncio
async def test_startup_task_coordinator_release_cancels_refresh_task() -> None:
    redis = _FakeRedis(eval_results=[1])
    coordinator = StartupTaskCoordinator(redis)
    coordinator._tokens["maintenance_scheduler"] = "token-1"
    refresh_task = _FakeTask()
    coordinator._refresh_tasks["maintenance_scheduler"] = refresh_task  # type: ignore[assignment]

    await coordinator.release("maintenance_scheduler")

    assert refresh_task.cancelled is True
    assert "maintenance_scheduler" not in coordinator._refresh_tasks
    assert "maintenance_scheduler" not in coordinator._tokens
    assert len(redis.eval_calls) == 1
    _script, numkeys, key, token = redis.eval_calls[0]
    assert numkeys == 1
    assert key == "task_lock:maintenance_scheduler"
    assert token == "token-1"


@pytest.mark.asyncio
async def test_startup_task_coordinator_notify_lock_lost_runs_registered_callback() -> None:
    coordinator = StartupTaskCoordinator()
    called: list[str] = []

    async def on_lock_lost(name: str) -> None:
        called.append(name)

    coordinator.register_lock_lost_callback("maintenance_scheduler", on_lock_lost)

    await coordinator._notify_lock_lost("maintenance_scheduler")

    assert called == ["maintenance_scheduler"]
