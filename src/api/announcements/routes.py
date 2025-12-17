"""公告系统 API 端点。"""

from dataclasses import dataclass
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from src.api.base.adapter import ApiAdapter, ApiMode
from src.api.base.admin_adapter import AdminApiAdapter
from src.api.base.authenticated_adapter import AuthenticatedApiAdapter
from src.api.base.pipeline import ApiRequestPipeline
from src.core.exceptions import InvalidRequestException, translate_pydantic_error
from src.core.logger import logger
from src.database import get_db
from src.models.api import CreateAnnouncementRequest, UpdateAnnouncementRequest
from src.models.database import User
from src.services.auth.service import AuthService
from src.services.system.announcement import AnnouncementService


router = APIRouter(prefix="/api/announcements", tags=["Announcements"])
pipeline = ApiRequestPipeline()


# ============== 公共端点（所有用户可访问） ==============


@router.get("")
async def list_announcements(
    request: Request,
    active_only: bool = Query(True, description="只返回有效公告"),
    limit: int = Query(50, description="返回数量限制"),
    offset: int = Query(0, description="偏移量"),
    db: Session = Depends(get_db),
):
    """获取公告列表（包含已读状态）"""
    adapter = ListAnnouncementsAdapter(active_only=active_only, limit=limit, offset=offset)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/active")
async def get_active_announcements(
    request: Request,
    db: Session = Depends(get_db),
):
    """获取当前有效的公告（首页展示）"""
    adapter = GetActiveAnnouncementsAdapter()
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.get("/{announcement_id}")
async def get_announcement(
    announcement_id: str,  # UUID
    request: Request,
    db: Session = Depends(get_db),
):
    """获取单个公告详情"""
    adapter = GetAnnouncementAdapter(announcement_id=announcement_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.patch("/{announcement_id}/read-status")
async def mark_announcement_as_read(
    announcement_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Mark announcement as read"""
    adapter = MarkAnnouncementReadAdapter(announcement_id=announcement_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


# ============== 管理员端点 ==============


@router.post("")
async def create_announcement(
    request: Request,
    db: Session = Depends(get_db),
):
    """创建公告（管理员权限）"""
    adapter = CreateAnnouncementAdapter()
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.put("/{announcement_id}")
async def update_announcement(
    announcement_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """更新公告（管理员权限）"""
    adapter = UpdateAnnouncementAdapter(announcement_id=announcement_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


@router.delete("/{announcement_id}")
async def delete_announcement(
    announcement_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """删除公告（管理员权限）"""
    adapter = DeleteAnnouncementAdapter(announcement_id=announcement_id)
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


# ============== 用户公告端点 ==============


@router.get("/users/me/unread-count")
async def get_my_unread_announcement_count(
    request: Request,
    db: Session = Depends(get_db),
):
    """获取我的未读公告数量"""
    adapter = UnreadAnnouncementCountAdapter()
    return await pipeline.run(adapter=adapter, http_request=request, db=db, mode=adapter.mode)


# ============== Pipeline 适配器 ==============


class AnnouncementOptionalAuthAdapter(ApiAdapter):
    """允许匿名访问，但可选解析Bearer以获取用户上下文。"""

    mode = ApiMode.PUBLIC

    async def authorize(self, context):  # type: ignore[override]
        context.extra["optional_user"] = await self._resolve_optional_user(context)
        return None

    async def _resolve_optional_user(self, context) -> Optional[User]:
        if context.user:
            return context.user

        authorization = context.request.headers.get("authorization")
        if not authorization or not authorization.lower().startswith("bearer "):
            return None

        token = authorization.replace("Bearer ", "").strip()
        try:
            payload = await AuthService.verify_token(token, token_type="access")
            user_id = payload.get("user_id")
            if not user_id:
                return None
            user = (
                context.db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
            )
            return user
        except Exception:
            return None

    def get_optional_user(self, context) -> Optional[User]:
        return context.extra.get("optional_user")


@dataclass
class ListAnnouncementsAdapter(AnnouncementOptionalAuthAdapter):
    active_only: bool
    limit: int
    offset: int

    async def handle(self, context):  # type: ignore[override]
        optional_user = self.get_optional_user(context)
        return AnnouncementService.get_announcements(
            db=context.db,
            user_id=optional_user.id if optional_user else None,
            active_only=self.active_only,
            include_read_status=True if optional_user else False,
            limit=self.limit,
            offset=self.offset,
        )


class GetActiveAnnouncementsAdapter(AnnouncementOptionalAuthAdapter):
    async def handle(self, context):  # type: ignore[override]
        optional_user = self.get_optional_user(context)
        return AnnouncementService.get_active_announcements(
            db=context.db,
            user_id=optional_user.id if optional_user else None,
        )


@dataclass
class GetAnnouncementAdapter(AnnouncementOptionalAuthAdapter):
    announcement_id: str

    async def handle(self, context):  # type: ignore[override]
        announcement = AnnouncementService.get_announcement(context.db, self.announcement_id)
        return {
            "id": announcement.id,
            "title": announcement.title,
            "content": announcement.content,
            "type": announcement.type,
            "priority": announcement.priority,
            "is_pinned": announcement.is_pinned,
            "author": {"id": announcement.author.id, "username": announcement.author.username},
            "start_time": announcement.start_time,
            "end_time": announcement.end_time,
            "created_at": announcement.created_at,
            "updated_at": announcement.updated_at,
        }


class AnnouncementUserAdapter(AuthenticatedApiAdapter):
    """需要登录但不要求管理员的公告适配器基类。"""

    pass


class MarkAnnouncementReadAdapter(AnnouncementUserAdapter):
    def __init__(self, announcement_id: str):
        self.announcement_id = announcement_id

    async def handle(self, context):  # type: ignore[override]
        AnnouncementService.mark_as_read(context.db, self.announcement_id, context.user.id)
        return {"message": "公告已标记为已读"}


class UnreadAnnouncementCountAdapter(AnnouncementUserAdapter):
    async def handle(self, context):  # type: ignore[override]
        result = AnnouncementService.get_announcements(
            db=context.db,
            user_id=context.user.id,
            active_only=True,
            include_read_status=True,
            limit=1,
            offset=0,
        )
        return {"unread_count": result.get("unread_count", 0)}


class CreateAnnouncementAdapter(AdminApiAdapter):
    async def handle(self, context):  # type: ignore[override]
        payload = context.ensure_json_body()
        try:
            req = CreateAnnouncementRequest.model_validate(payload)
        except ValidationError as e:
            errors = e.errors()
            if errors:
                raise InvalidRequestException(translate_pydantic_error(errors[0]))
            raise InvalidRequestException("请求数据验证失败")

        announcement = AnnouncementService.create_announcement(
            db=context.db,
            author_id=context.user.id,
            title=req.title,
            content=req.content,
            type=req.type,
            priority=req.priority,
            is_pinned=req.is_pinned,
            start_time=req.start_time,
            end_time=req.end_time,
        )
        return {"id": announcement.id, "title": announcement.title, "message": "公告创建成功"}


@dataclass
class UpdateAnnouncementAdapter(AdminApiAdapter):
    announcement_id: str

    async def handle(self, context):  # type: ignore[override]
        payload = context.ensure_json_body()
        try:
            req = UpdateAnnouncementRequest.model_validate(payload)
        except ValidationError as e:
            errors = e.errors()
            if errors:
                raise InvalidRequestException(translate_pydantic_error(errors[0]))
            raise InvalidRequestException("请求数据验证失败")

        AnnouncementService.update_announcement(
            db=context.db,
            announcement_id=self.announcement_id,
            user_id=context.user.id,
            title=req.title,
            content=req.content,
            type=req.type,
            priority=req.priority,
            is_active=req.is_active,
            is_pinned=req.is_pinned,
            start_time=req.start_time,
            end_time=req.end_time,
        )
        return {"message": "公告更新成功"}


@dataclass
class DeleteAnnouncementAdapter(AdminApiAdapter):
    announcement_id: str

    async def handle(self, context):  # type: ignore[override]
        AnnouncementService.delete_announcement(context.db, self.announcement_id, context.user.id)
        return {"message": "公告已删除"}
