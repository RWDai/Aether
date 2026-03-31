from __future__ import annotations

import importlib
import sys


def test_python_host_import_does_not_load_public_compat_modules() -> None:
    targets = [
        "src.main",
        "src.api.public",
        "src.api.public.compat",
        "src.api.public.support",
        "src.api.public.models",
        "src.api.public.capabilities",
        "src.api.public.modules",
        "src.api.public.openai",
        "src.api.public.claude",
        "src.api.public.gemini",
        "src.api.public.videos",
        "src.api.public.gemini_files",
        "src.api.public.system_catalog",
    ]
    for name in targets:
        sys.modules.pop(name, None)

    importlib.import_module("src.main")

    assert "src.api.public.support" in sys.modules
    assert "src.api.public.compat" not in sys.modules
    assert "src.api.public.models" not in sys.modules
    assert "src.api.public.capabilities" not in sys.modules
    assert "src.api.public.modules" not in sys.modules
    assert "src.api.public.openai" not in sys.modules
    assert "src.api.public.claude" not in sys.modules
    assert "src.api.public.gemini" not in sys.modules
    assert "src.api.public.videos" not in sys.modules
    assert "src.api.public.gemini_files" not in sys.modules
    assert "src.api.public.system_catalog" not in sys.modules
