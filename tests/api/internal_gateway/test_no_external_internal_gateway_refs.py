from __future__ import annotations

from pathlib import Path


def test_no_python_modules_outside_internal_gateway_reference_internal_gateway_urls() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    scan_roots = [repo_root / "src" / "api", repo_root / "src" / "services"]
    needle = "/api/internal/gateway"
    offenders: list[str] = []

    for root in scan_roots:
        for path in root.rglob("*.py"):
            rel = path.relative_to(repo_root).as_posix()
            if rel.startswith("src/api/internal/"):
                continue
            text = path.read_text(encoding="utf-8")
            if needle in text:
                offenders.append(rel)

    assert offenders == []
