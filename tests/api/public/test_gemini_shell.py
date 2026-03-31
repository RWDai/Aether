from __future__ import annotations

from typing import Any

import pytest
from starlette.requests import Request


def _make_request(path: str, method: str = "POST", headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": headers or [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "model", "stream"),
    [
        ("v1beta_generate", "gemini-2.5-flash", False),
        ("v1beta_stream", "gemini-2.5-flash", True),
        ("v1_generate", "gemini-2.5-flash", False),
        ("v1_stream", "gemini-2.5-flash", True),
    ],
)
async def test_public_gemini_routes_use_pipeline_shell(
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    model: str,
    stream: bool,
) -> None:
    from src.api.public import gemini as mod

    captured: dict[str, Any] = {}

    async def fake_run(*, adapter, http_request, db, mode, api_format_hint, path_params, **_kwargs):
        captured.update(
            {
                "adapter": adapter,
                "request": http_request,
                "db": db,
                "mode": mode,
                "api_format_hint": api_format_hint,
                "path_params": path_params,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    db = object()
    request = _make_request(f"/{route}")

    if route == "v1beta_generate":
        result = await mod.generate_content(model=model, http_request=request, db=db)
    elif route == "v1beta_stream":
        result = await mod.stream_generate_content(model=model, http_request=request, db=db)
    elif route == "v1_generate":
        result = await mod.generate_content_v1(model=model, http_request=request, db=db)
    else:
        result = await mod.stream_generate_content_v1(model=model, http_request=request, db=db)

    assert result == {"ok": True}
    assert isinstance(captured["adapter"], mod.PublicGeminiContentAdapter)
    assert captured["adapter"].model == model
    assert captured["adapter"].stream is stream
    assert captured["request"] is request
    assert captured["db"] is db
    assert captured["mode"] == captured["adapter"].mode
    assert captured["api_format_hint"] == "gemini:chat"
    assert captured["path_params"] == {"model": model, "stream": stream}


@pytest.mark.asyncio
async def test_public_gemini_shell_detects_cli_request_for_api_format_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.public import gemini as mod

    captured: dict[str, Any] = {}

    async def fake_run(*, adapter, http_request, db, mode, api_format_hint, path_params, **_kwargs):
        captured.update(
            {
                "adapter": adapter,
                "request": http_request,
                "db": db,
                "mode": mode,
                "api_format_hint": api_format_hint,
                "path_params": path_params,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(mod.pipeline, "run", fake_run)

    request = _make_request(
        "/v1beta/models/gemini-2.5-flash:generateContent",
        headers=[(b"x-app", b"gemini-cli")],
    )

    result = await mod.generate_content(model="gemini-2.5-flash", http_request=request, db=object())

    assert result == {"ok": True}
    assert isinstance(captured["adapter"], mod.PublicGeminiContentAdapter)
    assert captured["api_format_hint"] == "gemini:cli"
    assert captured["path_params"] == {"model": "gemini-2.5-flash", "stream": False}
