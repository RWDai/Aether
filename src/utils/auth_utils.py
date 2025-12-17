"""
认证工具函数
提供统一的用户认证和授权功能
"""

from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from src.services.auth.service import AuthService

from ..core.exceptions import ForbiddenException
from src.core.logger import logger
from ..database import get_db
from ..models.database import User, UserRole

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
) -> User:
    """
    获取当前登录用户
    统一的认证依赖函数

    Args:
        credentials: Bearer token 凭据
        db: 数据库会话

    Returns:
        User: 当前用户对象

    Raises:
        HTTPException: 认证失败时抛出
    """
    token = credentials.credentials

    try:
        # 验证Token格式和签名
        try:
            payload = await AuthService.verify_token(token, token_type="access")
        except HTTPException as token_error:
            # 保持原始的HTTP状态码（如401 Unauthorized），不要转换为403
            logger.error(f"Token验证失败: {token_error.status_code}: {token_error.detail}, Token前10位: {token[:10]}...")
            raise  # 重新抛出原始异常，保持状态码
        except Exception as token_error:
            logger.error(f"Token验证失败: {token_error}, Token前10位: {token[:10]}...")
            raise ForbiddenException("无效的Token")

        user_id = payload.get("user_id")
        token_email = payload.get("email")
        token_created_at = payload.get("created_at")

        if not user_id:
            logger.error(f"Token缺少user_id字段: payload={payload}")
            raise ForbiddenException("无效的认证凭据")

        if not token_email:
            logger.error(f"Token缺少email字段: payload={payload}")
            raise ForbiddenException("无效的认证凭据")

        # 仅在DEBUG模式下记录详细信息
        logger.debug(f"尝试获取用户: user_id={user_id}, token前10位: {token[:10]}...")

        # 确保user_id是字符串格式（UUID）
        if not isinstance(user_id, str):
            logger.error(f"Token中user_id格式错误: {type(user_id)} - {user_id}")
            raise ForbiddenException("认证信息格式错误，请重新登录")

        # 使用新的数据库会话获取用户，避免会话状态问题
        try:
            from src.services.user.service import UserService

            user = UserService.get_user(db, user_id)
        except Exception as db_error:
            logger.error(f"数据库查询失败: user_id={user_id}, error={db_error}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据库查询失败，请稍后重试",
            )

        if not user:
            logger.error(f"用户不存在: user_id={user_id}")
            raise ForbiddenException("用户不存在或已禁用")

        if not user.is_active:
            logger.error(f"用户已禁用: user_id={user_id}")
            raise ForbiddenException("用户不存在或已禁用")

        # 验证邮箱是否匹配（防止用户ID重用导致的身份混淆）
        if user.email != token_email:
            logger.error(f"Token邮箱不匹配: Token中的邮箱={token_email}, 数据库中的邮箱={user.email}")
            raise ForbiddenException("身份验证失败")

        # 验证用户创建时间是否匹配（防止ID重用）
        if token_created_at and user.created_at:
            try:
                from datetime import datetime

                token_created = datetime.fromisoformat(token_created_at.replace("Z", "+00:00"))
                # 允许1秒的时间差异（考虑到时间精度问题）
                time_diff = abs((user.created_at - token_created).total_seconds())
                if time_diff > 1:
                    logger.error(f"Token创建时间不匹配: Token时间={token_created_at}, 用户创建时间={user.created_at}")
                    raise ForbiddenException("身份验证失败")
            except ValueError as e:
                logger.warning(f"Token时间格式解析失败: {e}")

        logger.debug(f"成功获取用户: user_id={user_id}, email={user.email}")
        return user

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"认证失败，未预期的错误: {e}")
        # 返回500而不是401，避免触发前端的退出逻辑
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="认证服务暂时不可用"
        )


async def get_current_user_from_header(
    authorization: Optional[str] = Header(None), db: Session = Depends(get_db)
) -> User:
    """
    从Header中获取当前用户（兼容性函数）

    Args:
        authorization: Authorization header
        db: 数据库会话

    Returns:
        User: 当前用户对象

    Raises:
        HTTPException: 认证失败时抛出
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise ForbiddenException("未提供认证令牌")

    token = authorization.replace("Bearer ", "")

    try:
        payload = await AuthService.verify_token(token, token_type="access")
        user_id = payload.get("user_id")

        if not user_id:
            raise ForbiddenException("无效的认证凭据")

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ForbiddenException("用户不存在")

        if not user.is_active:
            raise ForbiddenException("用户已被禁用")

        return user
    except HTTPException:
        # 保持原始的HTTPException (包括401)
        raise
    except Exception as e:
        logger.error(f"认证失败: {e}")
        raise ForbiddenException("认证失败")


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """
    要求管理员权限

    Args:
        current_user: 当前用户

    Returns:
        User: 管理员用户对象

    Raises:
        HTTPException: 非管理员时抛出403错误
    """
    if current_user.role != UserRole.ADMIN:
        raise ForbiddenException("需要管理员权限")
    return current_user


def require_role(required_role: UserRole):
    """
    要求特定角色权限的装饰器工厂

    Args:
        required_role: 需要的用户角色

    Returns:
        依赖函数
    """

    def check_role(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role != required_role:
            raise ForbiddenException(f"需要{required_role.value}权限")
        return current_user

    return check_role
