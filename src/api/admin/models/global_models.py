"""
GlobalModel Admin API

提供 GlobalModel 的 CRUD 操作接口
"""

from dataclasses import dataclass
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from src.api.base.admin_adapter import AdminApiAdapter
from src.api.base.pipeline import ApiRequestPipeline
from src.core.logger import logger
from src.database import get_db
from src.models.pydantic_models import (
    BatchAssignToProvidersRequest,
    BatchAssignToProvidersResponse,
    GlobalModelCreate,
    GlobalModelListResponse,
    GlobalModelProvidersResponse,
    GlobalModelResponse,
    GlobalModelUpdate,
    GlobalModelWithStats,
    ModelCatalogProviderDetail,
)
from src.services.model.global_model import GlobalModelService

router = APIRouter(prefix="/global", tags=["Admin - Global Models"])
pipeline = ApiRequestPipeline()


@router.get("", response_model=GlobalModelListResponse)
async def list_global_models(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    is_active: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> GlobalModelListResponse:
    """获取 GlobalModel 列表"""
    adapter = AdminListGlobalModelsAdapter(
        skip=skip,
        limit=limit,
        is_active=is_active,
        search=search,
    )
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/{global_model_id}", response_model=GlobalModelWithStats)
async def get_global_model(
    request: Request,
    global_model_id: str,
    db: Session = Depends(get_db),
) -> GlobalModelWithStats:
    """获取单个 GlobalModel 详情（含统计信息）"""
    adapter = AdminGetGlobalModelAdapter(global_model_id=global_model_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.post("", response_model=GlobalModelResponse, status_code=201)
async def create_global_model(
    request: Request,
    payload: GlobalModelCreate,
    db: Session = Depends(get_db),
) -> GlobalModelResponse:
    """创建 GlobalModel"""
    adapter = AdminCreateGlobalModelAdapter(payload=payload)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.patch("/{global_model_id}", response_model=GlobalModelResponse)
async def update_global_model(
    request: Request,
    global_model_id: str,
    payload: GlobalModelUpdate,
    db: Session = Depends(get_db),
) -> GlobalModelResponse:
    """更新 GlobalModel"""
    adapter = AdminUpdateGlobalModelAdapter(global_model_id=global_model_id, payload=payload)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.delete("/{global_model_id}", status_code=204)
async def delete_global_model(
    request: Request,
    global_model_id: str,
    db: Session = Depends(get_db),
):
    """删除 GlobalModel（级联删除所有关联的 Provider 模型实现）"""
    adapter = AdminDeleteGlobalModelAdapter(global_model_id=global_model_id)
    await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)
    return None


@router.post(
    "/{global_model_id}/assign-to-providers", response_model=BatchAssignToProvidersResponse
)
async def batch_assign_to_providers(
    request: Request,
    global_model_id: str,
    payload: BatchAssignToProvidersRequest,
    db: Session = Depends(get_db),
) -> BatchAssignToProvidersResponse:
    """批量为多个 Provider 添加 GlobalModel 实现"""
    adapter = AdminBatchAssignToProvidersAdapter(global_model_id=global_model_id, payload=payload)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/{global_model_id}/providers", response_model=GlobalModelProvidersResponse)
async def get_global_model_providers(
    request: Request,
    global_model_id: str,
    db: Session = Depends(get_db),
) -> GlobalModelProvidersResponse:
    """获取 GlobalModel 的所有关联提供商（包括非活跃的）"""
    adapter = AdminGetGlobalModelProvidersAdapter(global_model_id=global_model_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


# ========== Adapters ==========


@dataclass
class AdminListGlobalModelsAdapter(AdminApiAdapter):
    """列出 GlobalModel"""

    skip: int
    limit: int
    is_active: Optional[bool]
    search: Optional[str]

    async def handle(self, context):  # type: ignore[override]
        from sqlalchemy import func

        from src.models.database import Model

        models = GlobalModelService.list_global_models(
            db=context.db,
            skip=self.skip,
            limit=self.limit,
            is_active=self.is_active,
            search=self.search,
        )

        # 一次性查询所有 GlobalModel 的 provider_count（优化 N+1 问题）
        model_ids = [gm.id for gm in models]
        provider_counts = {}
        if model_ids:
            count_results = (
                context.db.query(
                    Model.global_model_id, func.count(func.distinct(Model.provider_id))
                )
                .filter(Model.global_model_id.in_(model_ids))
                .group_by(Model.global_model_id)
                .all()
            )
            provider_counts = {gm_id: count for gm_id, count in count_results}

        # 构建响应
        model_responses = []
        for gm in models:
            response = GlobalModelResponse.model_validate(gm)
            response.provider_count = provider_counts.get(gm.id, 0)
            model_responses.append(response)

        return GlobalModelListResponse(
            models=model_responses,
            total=len(models),
        )


@dataclass
class AdminGetGlobalModelAdapter(AdminApiAdapter):
    """获取单个 GlobalModel"""

    global_model_id: str

    async def handle(self, context):  # type: ignore[override]
        global_model = GlobalModelService.get_global_model(context.db, self.global_model_id)
        stats = GlobalModelService.get_global_model_stats(context.db, self.global_model_id)

        return GlobalModelWithStats(
            **GlobalModelResponse.model_validate(global_model).model_dump(),
            total_models=stats["total_models"],
            total_providers=stats["total_providers"],
            price_range=stats["price_range"],
        )


@dataclass
class AdminCreateGlobalModelAdapter(AdminApiAdapter):
    """创建 GlobalModel"""

    payload: GlobalModelCreate

    async def handle(self, context):  # type: ignore[override]
        # 将 TieredPricingConfig 转换为 dict
        tiered_pricing_dict = self.payload.default_tiered_pricing.model_dump()

        global_model = GlobalModelService.create_global_model(
            db=context.db,
            name=self.payload.name,
            display_name=self.payload.display_name,
            is_active=self.payload.is_active,
            # 按次计费配置
            default_price_per_request=self.payload.default_price_per_request,
            # 阶梯计费配置
            default_tiered_pricing=tiered_pricing_dict,
            # Key 能力配置
            supported_capabilities=self.payload.supported_capabilities,
            # 模型配置（JSON）
            config=self.payload.config,
        )

        logger.info(f"GlobalModel 已创建: id={global_model.id} name={global_model.name}")

        return GlobalModelResponse.model_validate(global_model)


@dataclass
class AdminUpdateGlobalModelAdapter(AdminApiAdapter):
    """更新 GlobalModel"""

    global_model_id: str
    payload: GlobalModelUpdate

    async def handle(self, context):  # type: ignore[override]
        global_model = GlobalModelService.update_global_model(
            db=context.db,
            global_model_id=self.global_model_id,
            update_data=self.payload,
        )

        logger.info(f"GlobalModel 已更新: id={global_model.id} name={global_model.name}")

        # 失效相关缓存
        from src.services.cache.invalidation import get_cache_invalidation_service

        cache_service = get_cache_invalidation_service()
        cache_service.on_global_model_changed(global_model.name)

        return GlobalModelResponse.model_validate(global_model)


@dataclass
class AdminDeleteGlobalModelAdapter(AdminApiAdapter):
    """删除 GlobalModel（级联删除所有关联的 Provider 模型实现）"""

    global_model_id: str

    async def handle(self, context):  # type: ignore[override]
        # 先获取 GlobalModel 信息（用于失效缓存）
        from src.models.database import GlobalModel

        global_model = (
            context.db.query(GlobalModel).filter(GlobalModel.id == self.global_model_id).first()
        )
        model_name = global_model.name if global_model else None

        GlobalModelService.delete_global_model(context.db, self.global_model_id)

        logger.info(f"GlobalModel 已删除: id={self.global_model_id}")

        # 失效相关缓存
        if model_name:
            from src.services.cache.invalidation import get_cache_invalidation_service

            cache_service = get_cache_invalidation_service()
            cache_service.on_global_model_changed(model_name)

        return None


@dataclass
class AdminBatchAssignToProvidersAdapter(AdminApiAdapter):
    """批量为 Provider 添加 GlobalModel 实现"""

    global_model_id: str
    payload: BatchAssignToProvidersRequest

    async def handle(self, context):  # type: ignore[override]
        result = GlobalModelService.batch_assign_to_providers(
            db=context.db,
            global_model_id=self.global_model_id,
            provider_ids=self.payload.provider_ids,
            create_models=self.payload.create_models,
        )

        logger.info(f"批量为 Provider 添加 GlobalModel: global_model_id={self.global_model_id} success={len(result['success'])} errors={len(result['errors'])}")

        return BatchAssignToProvidersResponse(**result)


@dataclass
class AdminGetGlobalModelProvidersAdapter(AdminApiAdapter):
    """获取 GlobalModel 的所有关联提供商（包括非活跃的）"""

    global_model_id: str

    async def handle(self, context):  # type: ignore[override]
        from sqlalchemy.orm import joinedload

        from src.models.database import Model

        global_model = GlobalModelService.get_global_model(context.db, self.global_model_id)

        # 获取所有关联的 Model（包括非活跃的）
        models = (
            context.db.query(Model)
            .options(joinedload(Model.provider), joinedload(Model.global_model))
            .filter(Model.global_model_id == global_model.id)
            .all()
        )

        provider_entries = []
        for model in models:
            provider = model.provider
            if not provider:
                continue

            effective_tiered = model.get_effective_tiered_pricing()
            tier_count = len(effective_tiered.get("tiers", [])) if effective_tiered else 1

            provider_entries.append(
                ModelCatalogProviderDetail(
                    provider_id=provider.id,
                    provider_name=provider.name,
                    provider_display_name=provider.display_name,
                    model_id=model.id,
                    target_model=model.provider_model_name,
                    input_price_per_1m=model.get_effective_input_price(),
                    output_price_per_1m=model.get_effective_output_price(),
                    cache_creation_price_per_1m=model.get_effective_cache_creation_price(),
                    cache_read_price_per_1m=model.get_effective_cache_read_price(),
                    cache_1h_creation_price_per_1m=model.get_effective_1h_cache_creation_price(),
                    price_per_request=model.get_effective_price_per_request(),
                    effective_tiered_pricing=effective_tiered,
                    tier_count=tier_count,
                    supports_vision=model.get_effective_supports_vision(),
                    supports_function_calling=model.get_effective_supports_function_calling(),
                    supports_streaming=model.get_effective_supports_streaming(),
                    is_active=bool(model.is_active),
                )
            )

        return GlobalModelProvidersResponse(
            providers=provider_entries,
            total=len(provider_entries),
        )
