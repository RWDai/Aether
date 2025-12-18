"""
ProviderEndpoint CRUD 管理 API
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from src.api.base.admin_adapter import AdminApiAdapter
from src.api.base.pipeline import ApiRequestPipeline
from src.core.exceptions import InvalidRequestException, NotFoundException
from src.core.logger import logger
from src.database import get_db
from src.models.database import Provider, ProviderAPIKey, ProviderEndpoint
from src.models.endpoint_models import (
    ProviderEndpointCreate,
    ProviderEndpointResponse,
    ProviderEndpointUpdate,
)

router = APIRouter(tags=["Endpoint Management"])
pipeline = ApiRequestPipeline()


def mask_proxy_password(proxy_config: Optional[dict]) -> Optional[dict]:
    """对代理配置中的密码进行脱敏处理"""
    if not proxy_config:
        return None
    masked = dict(proxy_config)
    if masked.get("password"):
        masked["password"] = "***"
    return masked


@router.get("/providers/{provider_id}/endpoints", response_model=List[ProviderEndpointResponse])
async def list_provider_endpoints(
    provider_id: str,
    request: Request,
    skip: int = Query(0, ge=0, description="跳过的记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回的最大记录数"),
    db: Session = Depends(get_db),
) -> List[ProviderEndpointResponse]:
    """获取指定 Provider 的所有 Endpoints"""
    adapter = AdminListProviderEndpointsAdapter(
        provider_id=provider_id,
        skip=skip,
        limit=limit,
    )
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.post("/providers/{provider_id}/endpoints", response_model=ProviderEndpointResponse)
async def create_provider_endpoint(
    provider_id: str,
    endpoint_data: ProviderEndpointCreate,
    request: Request,
    db: Session = Depends(get_db),
) -> ProviderEndpointResponse:
    """为 Provider 创建新的 Endpoint"""
    adapter = AdminCreateProviderEndpointAdapter(
        provider_id=provider_id,
        endpoint_data=endpoint_data,
    )
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/{endpoint_id}", response_model=ProviderEndpointResponse)
async def get_endpoint(
    endpoint_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> ProviderEndpointResponse:
    """获取 Endpoint 详情"""
    adapter = AdminGetProviderEndpointAdapter(endpoint_id=endpoint_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.put("/{endpoint_id}", response_model=ProviderEndpointResponse)
async def update_endpoint(
    endpoint_id: str,
    endpoint_data: ProviderEndpointUpdate,
    request: Request,
    db: Session = Depends(get_db),
) -> ProviderEndpointResponse:
    """更新 Endpoint"""
    adapter = AdminUpdateProviderEndpointAdapter(
        endpoint_id=endpoint_id,
        endpoint_data=endpoint_data,
    )
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.delete("/{endpoint_id}")
async def delete_endpoint(
    endpoint_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> dict:
    """删除 Endpoint（级联删除所有关联的 Keys）"""
    adapter = AdminDeleteProviderEndpointAdapter(endpoint_id=endpoint_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


# -------- Adapters --------


@dataclass
class AdminListProviderEndpointsAdapter(AdminApiAdapter):
    provider_id: str
    skip: int
    limit: int

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        provider = db.query(Provider).filter(Provider.id == self.provider_id).first()
        if not provider:
            raise NotFoundException(f"Provider {self.provider_id} 不存在")

        endpoints = (
            db.query(ProviderEndpoint)
            .filter(ProviderEndpoint.provider_id == self.provider_id)
            .order_by(ProviderEndpoint.created_at.desc())
            .offset(self.skip)
            .limit(self.limit)
            .all()
        )

        endpoint_ids = [ep.id for ep in endpoints]
        total_keys_map = {}
        active_keys_map = {}
        if endpoint_ids:
            total_rows = (
                db.query(ProviderAPIKey.endpoint_id, func.count(ProviderAPIKey.id).label("total"))
                .filter(ProviderAPIKey.endpoint_id.in_(endpoint_ids))
                .group_by(ProviderAPIKey.endpoint_id)
                .all()
            )
            total_keys_map = {row.endpoint_id: row.total for row in total_rows}

            active_rows = (
                db.query(ProviderAPIKey.endpoint_id, func.count(ProviderAPIKey.id).label("active"))
                .filter(
                    and_(
                        ProviderAPIKey.endpoint_id.in_(endpoint_ids),
                        ProviderAPIKey.is_active.is_(True),
                    )
                )
                .group_by(ProviderAPIKey.endpoint_id)
                .all()
            )
            active_keys_map = {row.endpoint_id: row.active for row in active_rows}

        result: List[ProviderEndpointResponse] = []
        for endpoint in endpoints:
            endpoint_dict = {
                **endpoint.__dict__,
                "provider_name": provider.name,
                "api_format": endpoint.api_format,
                "total_keys": total_keys_map.get(endpoint.id, 0),
                "active_keys": active_keys_map.get(endpoint.id, 0),
                "proxy": mask_proxy_password(endpoint.proxy),
            }
            endpoint_dict.pop("_sa_instance_state", None)
            result.append(ProviderEndpointResponse(**endpoint_dict))

        return result


@dataclass
class AdminCreateProviderEndpointAdapter(AdminApiAdapter):
    provider_id: str
    endpoint_data: ProviderEndpointCreate

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        provider = db.query(Provider).filter(Provider.id == self.provider_id).first()
        if not provider:
            raise NotFoundException(f"Provider {self.provider_id} 不存在")

        if self.endpoint_data.provider_id != self.provider_id:
            raise InvalidRequestException("provider_id 不匹配")

        existing = (
            db.query(ProviderEndpoint)
            .filter(
                and_(
                    ProviderEndpoint.provider_id == self.provider_id,
                    ProviderEndpoint.api_format == self.endpoint_data.api_format,
                )
            )
            .first()
        )
        if existing:
            raise InvalidRequestException(
                f"Provider {provider.name} 已存在 {self.endpoint_data.api_format} 格式的 Endpoint"
            )

        now = datetime.now(timezone.utc)
        new_endpoint = ProviderEndpoint(
            id=str(uuid.uuid4()),
            provider_id=self.provider_id,
            api_format=self.endpoint_data.api_format,
            base_url=self.endpoint_data.base_url,
            headers=self.endpoint_data.headers,
            timeout=self.endpoint_data.timeout,
            max_retries=self.endpoint_data.max_retries,
            max_concurrent=self.endpoint_data.max_concurrent,
            rate_limit=self.endpoint_data.rate_limit,
            is_active=True,
            config=self.endpoint_data.config,
            proxy=self.endpoint_data.proxy.model_dump() if self.endpoint_data.proxy else None,
            created_at=now,
            updated_at=now,
        )

        db.add(new_endpoint)
        db.commit()
        db.refresh(new_endpoint)

        logger.info(f"[OK] 创建 Endpoint: Provider={provider.name}, Format={self.endpoint_data.api_format}, ID={new_endpoint.id}")

        endpoint_dict = {
            k: v
            for k, v in new_endpoint.__dict__.items()
            if k not in {"api_format", "_sa_instance_state", "proxy"}
        }
        return ProviderEndpointResponse(
            **endpoint_dict,
            provider_name=provider.name,
            api_format=new_endpoint.api_format,
            proxy=mask_proxy_password(new_endpoint.proxy),
            total_keys=0,
            active_keys=0,
        )


@dataclass
class AdminGetProviderEndpointAdapter(AdminApiAdapter):
    endpoint_id: str

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        endpoint = (
            db.query(ProviderEndpoint, Provider)
            .join(Provider, ProviderEndpoint.provider_id == Provider.id)
            .filter(ProviderEndpoint.id == self.endpoint_id)
            .first()
        )
        if not endpoint:
            raise NotFoundException(f"Endpoint {self.endpoint_id} 不存在")

        endpoint_obj, provider = endpoint
        total_keys = (
            db.query(ProviderAPIKey).filter(ProviderAPIKey.endpoint_id == self.endpoint_id).count()
        )
        active_keys = (
            db.query(ProviderAPIKey)
            .filter(
                and_(
                    ProviderAPIKey.endpoint_id == self.endpoint_id,
                    ProviderAPIKey.is_active.is_(True),
                )
            )
            .count()
        )

        endpoint_dict = {
            k: v
            for k, v in endpoint_obj.__dict__.items()
            if k not in {"api_format", "_sa_instance_state", "proxy"}
        }
        return ProviderEndpointResponse(
            **endpoint_dict,
            provider_name=provider.name,
            api_format=endpoint_obj.api_format,
            proxy=mask_proxy_password(endpoint_obj.proxy),
            total_keys=total_keys,
            active_keys=active_keys,
        )


@dataclass
class AdminUpdateProviderEndpointAdapter(AdminApiAdapter):
    endpoint_id: str
    endpoint_data: ProviderEndpointUpdate

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        endpoint = (
            db.query(ProviderEndpoint).filter(ProviderEndpoint.id == self.endpoint_id).first()
        )
        if not endpoint:
            raise NotFoundException(f"Endpoint {self.endpoint_id} 不存在")

        update_data = self.endpoint_data.model_dump(exclude_unset=True)
        # 把 proxy 转换为 dict 存储，支持显式设置为 None 清除代理
        if "proxy" in update_data:
            if update_data["proxy"] is not None:
                new_proxy = dict(update_data["proxy"])
                # 只有当密码字段未提供时才保留原密码（空字符串视为显式清除）
                if "password" not in new_proxy and endpoint.proxy:
                    old_password = endpoint.proxy.get("password")
                    if old_password:
                        new_proxy["password"] = old_password
                update_data["proxy"] = new_proxy
            # proxy 为 None 时保留，用于清除代理配置
        for field, value in update_data.items():
            setattr(endpoint, field, value)
        endpoint.updated_at = datetime.now(timezone.utc)

        db.commit()
        db.refresh(endpoint)

        provider = db.query(Provider).filter(Provider.id == endpoint.provider_id).first()
        logger.info(f"[OK] 更新 Endpoint: ID={self.endpoint_id}, Updates={list(update_data.keys())}")

        total_keys = (
            db.query(ProviderAPIKey).filter(ProviderAPIKey.endpoint_id == self.endpoint_id).count()
        )
        active_keys = (
            db.query(ProviderAPIKey)
            .filter(
                and_(
                    ProviderAPIKey.endpoint_id == self.endpoint_id,
                    ProviderAPIKey.is_active.is_(True),
                )
            )
            .count()
        )

        endpoint_dict = {
            k: v
            for k, v in endpoint.__dict__.items()
            if k not in {"api_format", "_sa_instance_state", "proxy"}
        }
        return ProviderEndpointResponse(
            **endpoint_dict,
            provider_name=provider.name if provider else "Unknown",
            api_format=endpoint.api_format,
            proxy=mask_proxy_password(endpoint.proxy),
            total_keys=total_keys,
            active_keys=active_keys,
        )


@dataclass
class AdminDeleteProviderEndpointAdapter(AdminApiAdapter):
    endpoint_id: str

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        endpoint = (
            db.query(ProviderEndpoint).filter(ProviderEndpoint.id == self.endpoint_id).first()
        )
        if not endpoint:
            raise NotFoundException(f"Endpoint {self.endpoint_id} 不存在")

        keys_count = (
            db.query(ProviderAPIKey).filter(ProviderAPIKey.endpoint_id == self.endpoint_id).count()
        )
        db.delete(endpoint)
        db.commit()

        logger.warning(f"[DELETE] 删除 Endpoint: ID={self.endpoint_id}, 同时删除了 {keys_count} 个 Keys")

        return {"message": f"Endpoint {self.endpoint_id} 已删除", "deleted_keys_count": keys_count}
