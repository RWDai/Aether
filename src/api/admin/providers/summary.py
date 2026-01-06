"""
Provider 摘要与健康监控 API
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.api.base.admin_adapter import AdminApiAdapter
from src.api.base.pipeline import ApiRequestPipeline
from src.core.enums import ProviderBillingType
from src.core.exceptions import NotFoundException
from src.core.logger import logger
from src.database import get_db
from src.models.database import (
    Model,
    Provider,
    ProviderAPIKey,
    ProviderEndpoint,
    RequestCandidate,
)
from src.models.endpoint_models import (
    EndpointHealthEvent,
    EndpointHealthMonitor,
    ProviderEndpointHealthMonitorResponse,
    ProviderUpdateRequest,
    ProviderWithEndpointsSummary,
)

router = APIRouter(tags=["Provider Summary"])
pipeline = ApiRequestPipeline()


@router.get("/summary", response_model=List[ProviderWithEndpointsSummary])
async def get_providers_summary(
    request: Request,
    db: Session = Depends(get_db),
) -> List[ProviderWithEndpointsSummary]:
    """获取所有 Providers 的摘要信息(包含 Endpoints 和 Keys 统计)"""
    adapter = AdminProviderSummaryAdapter()
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/{provider_id}/summary", response_model=ProviderWithEndpointsSummary)
async def get_provider_summary(
    provider_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> ProviderWithEndpointsSummary:
    """获取单个 Provider 的摘要信息(包含 Endpoints 和 Keys 统计)"""
    provider = db.query(Provider).filter(Provider.id == provider_id).first()
    if not provider:
        raise NotFoundException(f"Provider {provider_id} not found")

    return _build_provider_summary(db, provider)


@router.get("/{provider_id}/health-monitor", response_model=ProviderEndpointHealthMonitorResponse)
async def get_provider_health_monitor(
    provider_id: str,
    request: Request,
    lookback_hours: int = Query(6, ge=1, le=72, description="回溯的小时数"),
    per_endpoint_limit: int = Query(48, ge=10, le=200, description="每个端点的事件数量"),
    db: Session = Depends(get_db),
) -> ProviderEndpointHealthMonitorResponse:
    """获取 Provider 下所有端点的健康监控时间线"""

    adapter = AdminProviderHealthMonitorAdapter(
        provider_id=provider_id,
        lookback_hours=lookback_hours,
        per_endpoint_limit=per_endpoint_limit,
    )
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.patch("/{provider_id}", response_model=ProviderWithEndpointsSummary)
async def update_provider_settings(
    provider_id: str,
    update_data: ProviderUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> ProviderWithEndpointsSummary:
    """更新 Provider 基础配置（display_name, description, priority, weight 等）"""

    adapter = AdminUpdateProviderSettingsAdapter(provider_id=provider_id, update_data=update_data)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


def _build_provider_summary(db: Session, provider: Provider) -> ProviderWithEndpointsSummary:
    endpoints = db.query(ProviderEndpoint).filter(ProviderEndpoint.provider_id == provider.id).all()

    total_endpoints = len(endpoints)
    active_endpoints = sum(1 for e in endpoints if e.is_active)
    endpoint_ids = [e.id for e in endpoints]

    # Key 统计（合并为单个查询）
    total_keys = 0
    active_keys = 0
    if endpoint_ids:
        key_stats = db.query(
            func.count(ProviderAPIKey.id).label("total"),
            func.sum(case((ProviderAPIKey.is_active == True, 1), else_=0)).label("active"),
        ).filter(ProviderAPIKey.endpoint_id.in_(endpoint_ids)).first()
        total_keys = key_stats.total or 0
        active_keys = int(key_stats.active or 0)

    # Model 统计（合并为单个查询）
    model_stats = db.query(
        func.count(Model.id).label("total"),
        func.sum(case((Model.is_active == True, 1), else_=0)).label("active"),
    ).filter(Model.provider_id == provider.id).first()
    total_models = model_stats.total or 0
    active_models = int(model_stats.active or 0)

    api_formats = [e.api_format for e in endpoints]

    # 优化: 一次性加载所有 endpoint 的 keys，避免 N+1 查询
    all_keys = []
    if endpoint_ids:
        all_keys = (
            db.query(ProviderAPIKey).filter(ProviderAPIKey.endpoint_id.in_(endpoint_ids)).all()
        )

    # 按 endpoint_id 分组 keys
    keys_by_endpoint: dict[str, list[ProviderAPIKey]] = {}
    for key in all_keys:
        if key.endpoint_id not in keys_by_endpoint:
            keys_by_endpoint[key.endpoint_id] = []
        keys_by_endpoint[key.endpoint_id].append(key)

    endpoint_health_map: dict[str, float] = {}
    for endpoint in endpoints:
        keys = keys_by_endpoint.get(endpoint.id, [])
        if keys:
            health_scores = [k.health_score for k in keys if k.health_score is not None]
            avg_health = sum(health_scores) / len(health_scores) if health_scores else 1.0
            endpoint_health_map[endpoint.id] = avg_health
        else:
            endpoint_health_map[endpoint.id] = 1.0

    all_health_scores = list(endpoint_health_map.values())
    avg_health_score = sum(all_health_scores) / len(all_health_scores) if all_health_scores else 1.0
    unhealthy_endpoints = sum(1 for score in all_health_scores if score < 0.5)

    # 计算每个端点的活跃密钥数量
    active_keys_by_endpoint: dict[str, int] = {}
    for endpoint_id, keys in keys_by_endpoint.items():
        active_keys_by_endpoint[endpoint_id] = sum(1 for k in keys if k.is_active)

    endpoint_health_details = [
        {
            "api_format": e.api_format,
            "health_score": endpoint_health_map.get(e.id, 1.0),
            "is_active": e.is_active,
            "active_keys": active_keys_by_endpoint.get(e.id, 0),
        }
        for e in endpoints
    ]

    return ProviderWithEndpointsSummary(
        id=provider.id,
        name=provider.name,
        display_name=provider.display_name,
        description=provider.description,
        website=provider.website,
        provider_priority=provider.provider_priority,
        is_active=provider.is_active,
        billing_type=provider.billing_type.value if provider.billing_type else None,
        monthly_quota_usd=provider.monthly_quota_usd,
        monthly_used_usd=provider.monthly_used_usd,
        quota_reset_day=provider.quota_reset_day,
        quota_last_reset_at=provider.quota_last_reset_at,
        quota_expires_at=provider.quota_expires_at,
        rpm_limit=provider.rpm_limit,
        rpm_used=provider.rpm_used,
        rpm_reset_at=provider.rpm_reset_at,
        total_endpoints=total_endpoints,
        active_endpoints=active_endpoints,
        total_keys=total_keys,
        active_keys=active_keys,
        total_models=total_models,
        active_models=active_models,
        avg_health_score=avg_health_score,
        unhealthy_endpoints=unhealthy_endpoints,
        api_formats=api_formats,
        endpoint_health_details=endpoint_health_details,
        config=provider.config,
        created_at=provider.created_at,
        updated_at=provider.updated_at,
    )


# -------- Adapters --------


@dataclass
class AdminProviderHealthMonitorAdapter(AdminApiAdapter):
    provider_id: str
    lookback_hours: int
    per_endpoint_limit: int

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        provider = db.query(Provider).filter(Provider.id == self.provider_id).first()
        if not provider:
            raise NotFoundException(f"Provider {self.provider_id} 不存在")

        endpoints = (
            db.query(ProviderEndpoint)
            .filter(ProviderEndpoint.provider_id == self.provider_id)
            .all()
        )

        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=self.lookback_hours)

        endpoint_ids = [endpoint.id for endpoint in endpoints]
        if not endpoint_ids:
            response = ProviderEndpointHealthMonitorResponse(
                provider_id=provider.id,
                provider_name=provider.display_name or provider.name,
                generated_at=now,
                endpoints=[],
            )
            context.add_audit_metadata(
                action="provider_health_monitor",
                provider_id=self.provider_id,
                endpoint_count=0,
                lookback_hours=self.lookback_hours,
            )
            return response

        limit_rows = max(200, self.per_endpoint_limit * max(1, len(endpoint_ids)) * 2)
        attempts_query = (
            db.query(RequestCandidate)
            .filter(
                RequestCandidate.endpoint_id.in_(endpoint_ids),
                RequestCandidate.created_at >= since,
            )
            .order_by(RequestCandidate.created_at.desc())
        )
        attempts = attempts_query.limit(limit_rows).all()

        buffered_attempts: Dict[str, List[RequestCandidate]] = {eid: [] for eid in endpoint_ids}
        counters: Dict[str, int] = {eid: 0 for eid in endpoint_ids}

        for attempt in attempts:
            if not attempt.endpoint_id or attempt.endpoint_id not in buffered_attempts:
                continue
            if counters[attempt.endpoint_id] >= self.per_endpoint_limit:
                continue
            buffered_attempts[attempt.endpoint_id].append(attempt)
            counters[attempt.endpoint_id] += 1

        endpoint_monitors: List[EndpointHealthMonitor] = []
        for endpoint in endpoints:
            attempt_list = list(reversed(buffered_attempts.get(endpoint.id, [])))
            events: List[EndpointHealthEvent] = []
            for attempt in attempt_list:
                event_timestamp = attempt.finished_at or attempt.started_at or attempt.created_at
                events.append(
                    EndpointHealthEvent(
                        timestamp=event_timestamp,
                        status=attempt.status,
                        status_code=attempt.status_code,
                        latency_ms=attempt.latency_ms,
                        error_type=attempt.error_type,
                        error_message=attempt.error_message,
                    )
                )

            success_count = sum(1 for event in events if event.status == "success")
            failed_count = sum(1 for event in events if event.status == "failed")
            skipped_count = sum(1 for event in events if event.status == "skipped")
            total_attempts = len(events)
            success_rate = success_count / total_attempts if total_attempts else 1.0
            last_event_at = events[-1].timestamp if events else None

            endpoint_monitors.append(
                EndpointHealthMonitor(
                    endpoint_id=endpoint.id,
                    api_format=endpoint.api_format,
                    is_active=endpoint.is_active,
                    total_attempts=total_attempts,
                    success_count=success_count,
                    failed_count=failed_count,
                    skipped_count=skipped_count,
                    success_rate=success_rate,
                    last_event_at=last_event_at,
                    events=events,
                )
            )

        response = ProviderEndpointHealthMonitorResponse(
            provider_id=provider.id,
            provider_name=provider.display_name or provider.name,
            generated_at=now,
            endpoints=endpoint_monitors,
        )
        context.add_audit_metadata(
            action="provider_health_monitor",
            provider_id=self.provider_id,
            endpoint_count=len(endpoint_monitors),
            lookback_hours=self.lookback_hours,
            per_endpoint_limit=self.per_endpoint_limit,
        )
        return response


class AdminProviderSummaryAdapter(AdminApiAdapter):
    async def handle(self, context):  # type: ignore[override]
        db = context.db
        providers = (
            db.query(Provider)
            .order_by(Provider.provider_priority.asc(), Provider.created_at.asc())
            .all()
        )
        return [_build_provider_summary(db, provider) for provider in providers]


@dataclass
class AdminUpdateProviderSettingsAdapter(AdminApiAdapter):
    provider_id: str
    update_data: ProviderUpdateRequest

    async def handle(self, context):  # type: ignore[override]
        db = context.db
        provider = db.query(Provider).filter(Provider.id == self.provider_id).first()
        if not provider:
            raise NotFoundException("Provider not found", "provider")

        update_dict = self.update_data.model_dump(exclude_unset=True)
        if "billing_type" in update_dict and update_dict["billing_type"] is not None:
            update_dict["billing_type"] = ProviderBillingType(update_dict["billing_type"])

        for key, value in update_dict.items():
            setattr(provider, key, value)

        db.commit()
        db.refresh(provider)

        admin_name = context.user.username if context.user else "admin"
        logger.info(f"Provider {provider.name} updated by {admin_name}: {update_dict}")

        return _build_provider_summary(db, provider)
