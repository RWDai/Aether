"""
Claude API 端点

- /v1/messages - Claude Messages API
- /v1/messages/count_tokens - Token Count API

注意: /v1/models 端点由 models.py 统一处理，根据请求头返回对应格式
"""

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.api.base.pipeline import ApiRequestPipeline
from src.api.handlers.claude import (
    ClaudeTokenCountAdapter,
    build_claude_adapter,
)
from src.core.api_format_metadata import get_api_format_definition
from src.core.enums import APIFormat
from src.database import get_db

_claude_def = get_api_format_definition(APIFormat.CLAUDE)
router = APIRouter(tags=["Claude API"], prefix=_claude_def.path_prefix)
pipeline = ApiRequestPipeline()


@router.post("/v1/messages")
async def create_message(
    http_request: Request,
    db: Session = Depends(get_db),
):
    """
    Claude Messages API

    兼容 Anthropic Claude Messages API 格式的代理接口。
    根据请求头 `x-app` 自动在标准 API 和 Claude Code CLI 模式之间切换。

    **认证方式**: x-api-key 请求头

    **请求格式**:
    ```json
    {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": "Hello"}]
    }
    ```

    **必需请求头**:
    - `x-api-key`: API 密钥
    - `anthropic-version`: API 版本（如 2023-06-01）
    """
    adapter = build_claude_adapter(http_request.headers.get("x-app", ""))
    return await pipeline.run(
        adapter=adapter,
        http_request=http_request,
        db=db,
        mode=adapter.mode,
        api_format_hint=adapter.allowed_api_formats[0],
    )


@router.post("/v1/messages/count_tokens")
async def count_tokens(
    http_request: Request,
    db: Session = Depends(get_db),
):
    """
    Claude Token Count API

    计算消息的 Token 数量，用于预估请求成本。

    **认证方式**: x-api-key 请求头
    """
    adapter = ClaudeTokenCountAdapter()
    return await pipeline.run(
        adapter=adapter,
        http_request=http_request,
        db=db,
        mode=adapter.mode,
    )
