"""
Endpoint 并发控制管理 API
"""

from dataclasses import dataclass
from typing import Optional

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.api.base.admin_adapter import AdminApiAdapter
from src.api.base.pipeline import ApiRequestPipeline
from src.core.exceptions import NotFoundException
from src.database import get_db
from src.models.database import ProviderAPIKey, ProviderEndpoint
from src.models.endpoint_models import (
    ConcurrencyStatusResponse,
    ResetConcurrencyRequest,
)
from src.services.rate_limit.concurrency_manager import get_concurrency_manager

router = APIRouter(tags=["Concurrency Control"])
pipeline = ApiRequestPipeline()


@router.get("/concurrency/endpoint/{endpoint_id}", response_model=ConcurrencyStatusResponse)
async def get_endpoint_concurrency(
    endpoint_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> ConcurrencyStatusResponse:
    """
    获取 Endpoint 当前并发状态

    查询指定 Endpoint 的实时并发使用情况，包括当前并发数和最大并发限制。

    **路径参数**:
    - `endpoint_id`: Endpoint ID

    **返回字段**:
    - `endpoint_id`: Endpoint ID
    - `endpoint_current_concurrency`: 当前并发数
    - `endpoint_max_concurrent`: 最大并发限制
    """
    adapter = AdminEndpointConcurrencyAdapter(endpoint_id=endpoint_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/concurrency/key/{key_id}", response_model=ConcurrencyStatusResponse)
async def get_key_concurrency(
    key_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> ConcurrencyStatusResponse:
    """
    获取 Key 当前并发状态

    查询指定 API Key 的实时并发使用情况，包括当前并发数和最大并发限制。

    **路径参数**:
    - `key_id`: API Key ID

    **返回字段**:
    - `key_id`: API Key ID
    - `key_current_concurrency`: 当前并发数
    - `key_max_concurrent`: 最大并发限制
    """
    adapter = AdminKeyConcurrencyAdapter(key_id=key_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.delete("/concurrency")
async def reset_concurrency(
    request: ResetConcurrencyRequest,
    http_request: Request,
    db: Session = Depends(get_db),
) -> dict:
    """
    重置并发计数器

    重置指定 Endpoint 或 Key 的并发计数器，用于解决计数不准确的问题。
    管理员功能，请谨慎使用。

    **请求体字段**:
    - `endpoint_id`: Endpoint ID（可选）
    - `key_id`: API Key ID（可选）

    **返回字段**:
    - `message`: 操作结果消息
    """
    adapter = AdminResetConcurrencyAdapter(endpoint_id=request.endpoint_id, key_id=request.key_id)
    return await pipeline.run(adapter=adapter, http_request=http_request, db=db, mode=adapter.mode)


# -------- Adapters --------


@dataclass
class AdminEndpointConcurrencyAdapter(AdminApiAdapter):
    endpoint_id: str

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        endpoint = (
            db.query(ProviderEndpoint).filter(ProviderEndpoint.id == self.endpoint_id).first()
        )
        if not endpoint:
            raise NotFoundException(f"Endpoint {self.endpoint_id} 不存在")

        concurrency_manager = await get_concurrency_manager()
        endpoint_count, _ = await concurrency_manager.get_current_concurrency(
            endpoint_id=self.endpoint_id
        )

        return ConcurrencyStatusResponse(
            endpoint_id=self.endpoint_id,
            endpoint_current_concurrency=endpoint_count,
            endpoint_max_concurrent=endpoint.max_concurrent,
        )


@dataclass
class AdminKeyConcurrencyAdapter(AdminApiAdapter):
    key_id: str

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        key = db.query(ProviderAPIKey).filter(ProviderAPIKey.id == self.key_id).first()
        if not key:
            raise NotFoundException(f"Key {self.key_id} 不存在")

        concurrency_manager = await get_concurrency_manager()
        _, key_count = await concurrency_manager.get_current_concurrency(key_id=self.key_id)

        return ConcurrencyStatusResponse(
            key_id=self.key_id,
            key_current_concurrency=key_count,
            key_max_concurrent=key.max_concurrent,
        )


@dataclass
class AdminResetConcurrencyAdapter(AdminApiAdapter):
    endpoint_id: Optional[str]
    key_id: Optional[str]

    async def handle(self, context):  # type: ignore[override]
        concurrency_manager = await get_concurrency_manager()
        await concurrency_manager.reset_concurrency(
            endpoint_id=self.endpoint_id, key_id=self.key_id
        )
        return {"message": "并发计数已重置"}
