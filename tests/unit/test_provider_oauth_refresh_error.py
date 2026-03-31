from __future__ import annotations

import httpx
import pytest
from types import SimpleNamespace

from src.api.admin import provider_oauth as module


class _SingleKeyQuery:
    def __init__(self, key: object) -> None:
        self._key = key

    def filter(self, *_args: object, **_kwargs: object) -> "_SingleKeyQuery":
        return self

    def first(self) -> object:
        return self._key


class _SingleKeyDB:
    def __init__(self, key: object) -> None:
        self._key = key

    def query(self, _model: object) -> _SingleKeyQuery:
        return _SingleKeyQuery(self._key)


class _FakeDBContext:
    def __init__(self, db: _SingleKeyDB) -> None:
        self._db = db

    def __enter__(self) -> _SingleKeyDB:
        return self._db

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        _ = exc_type, exc, tb
        return False


def test_extract_oauth_refresh_error_reason_for_reused_refresh_token() -> None:
    response = httpx.Response(
        400,
        json={
            "error": {
                "message": (
                    "Your refresh token has already been used to generate a new access token. "
                    "Please try signing in again."
                ),
                "type": "invalid_request_error",
                "param": None,
                "code": "refresh_token_reused",
            }
        },
        request=httpx.Request("POST", "https://example.com/oauth/token"),
    )

    assert (
        module._extract_oauth_refresh_error_reason(response)
        == "refresh_token 已被使用并轮换，请重新登录授权"
    )


def test_extract_oauth_refresh_error_reason_prefers_nested_message() -> None:
    response = httpx.Response(
        401,
        json={
            "error": {
                "message": "refresh token expired",
                "type": "invalid_request_error",
            }
        },
        request=httpx.Request("POST", "https://example.com/oauth/token"),
    )

    assert (
        module._extract_oauth_refresh_error_reason(response)
        == "refresh_token 无效、已过期或已撤销，请重新登录授权"
    )


def test_merge_refresh_failure_reason_keeps_account_block_and_appends_refresh_failure() -> None:
    current_reason = "[ACCOUNT_BLOCK] 工作区已停用 (deactivated_workspace)"
    refresh_reason = "[REFRESH_FAILED] Token 续期失败 (400): refresh_token_reused"

    assert module._merge_refresh_failure_reason(current_reason, refresh_reason) == (
        "[ACCOUNT_BLOCK] 工作区已停用 (deactivated_workspace)\n"
        "[REFRESH_FAILED] Token 续期失败 (400): refresh_token_reused"
    )


def test_merge_refresh_failure_reason_keeps_oauth_expired_sticky() -> None:
    current_reason = "[OAUTH_EXPIRED] Token 已过期且续期失败"
    refresh_reason = "[REFRESH_FAILED] Token 续期失败 (400): refresh_token_reused"

    assert module._merge_refresh_failure_reason(current_reason, refresh_reason) is None


@pytest.mark.parametrize(
    ("initial_reason", "should_clear"),
    [
        ("[REFRESH_FAILED] Token 续期失败 (401): refresh_token_reused", True),
        ("[ACCOUNT_BLOCK] Google requires verification", False),
    ],
)
def test_store_refreshed_oauth_sync_only_clears_recoverable_invalid_markers(
    monkeypatch: pytest.MonkeyPatch,
    initial_reason: str,
    should_clear: bool,
) -> None:
    key = SimpleNamespace(
        id="key-1",
        api_key="old-api",
        auth_config="old-config",
        oauth_invalid_at="old-invalid-at",
        oauth_invalid_reason=initial_reason,
    )
    db = _SingleKeyDB(key)

    monkeypatch.setattr(module, "get_db_context", lambda: _FakeDBContext(db))
    monkeypatch.setattr(module.crypto_service, "encrypt", lambda value: f"enc:{value}")

    module._store_refreshed_oauth_sync(
        "key-1",
        "new-token",
        {"refresh_token": "rt-2"},
    )

    assert key.api_key == "enc:new-token"
    assert key.auth_config == 'enc:{"refresh_token": "rt-2"}'
    if should_clear:
        assert key.oauth_invalid_at is None
        assert key.oauth_invalid_reason is None
    else:
        assert key.oauth_invalid_at == "old-invalid-at"
        assert key.oauth_invalid_reason == initial_reason
