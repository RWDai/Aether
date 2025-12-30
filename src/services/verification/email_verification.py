"""
邮箱验证服务
提供验证码生成、发送、验证等功能
"""

import json
import secrets
from datetime import datetime, timezone
from typing import Optional, Tuple

from src.clients.redis_client import get_redis_client
from src.core.logger import logger


class EmailVerificationService:
    """邮箱验证码服务"""

    # Redis key 前缀
    VERIFICATION_PREFIX = "email:verification:"
    SEND_LIMIT_PREFIX = "email:send_limit:"
    VERIFIED_PREFIX = "email:verified:"

    # 默认配置
    DEFAULT_CODE_EXPIRE_MINUTES = 30
    DEFAULT_MAX_ATTEMPTS = 5
    SEND_COOLDOWN_SECONDS = 60
    SEND_LIMIT_PER_HOUR = 5

    @staticmethod
    def _generate_code() -> str:
        """
        生成 6 位数字验证码

        Returns:
            6 位数字字符串
        """
        # 使用 secrets 模块生成安全的随机数
        code = secrets.randbelow(1000000)
        return f"{code:06d}"

    @staticmethod
    async def send_verification_code(
        email: str, expire_minutes: Optional[int] = None
    ) -> Tuple[bool, str, Optional[str]]:
        """
        发送验证码（生成并存储到 Redis）

        Args:
            email: 目标邮箱地址
            expire_minutes: 验证码过期时间（分钟），None 则使用默认值

        Returns:
            (是否成功, 验证码/错误信息, 错误详情)
        """
        redis_client = await get_redis_client(require_redis=False)

        if redis_client is None:
            logger.error("Redis 不可用，无法发送验证码")
            return False, "系统错误", "Redis 服务不可用"

        try:
            # 检查发送频率限制
            send_limit_key = f"{EmailVerificationService.SEND_LIMIT_PREFIX}{email}"
            send_count = await redis_client.get(send_limit_key)

            if send_count:
                send_count = int(send_count)
                if send_count >= EmailVerificationService.SEND_LIMIT_PER_HOUR:
                    logger.warning(f"邮箱 {email} 发送验证码次数超限: {send_count}")
                    return False, "发送次数过多", "每小时最多发送 5 次验证码"

            # 检查冷却时间
            verification_key = f"{EmailVerificationService.VERIFICATION_PREFIX}{email}"
            existing_data = await redis_client.get(verification_key)

            if existing_data:
                data = json.loads(existing_data)
                created_at = datetime.fromisoformat(data["created_at"])
                elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()

                if elapsed < EmailVerificationService.SEND_COOLDOWN_SECONDS:
                    remaining = int(EmailVerificationService.SEND_COOLDOWN_SECONDS - elapsed)
                    logger.warning(f"邮箱 {email} 请求验证码过于频繁，需等待 {remaining} 秒")
                    return False, "请求过于频繁", f"请在 {remaining} 秒后重试"

            # 生成验证码
            code = EmailVerificationService._generate_code()
            expire_time = expire_minutes or EmailVerificationService.DEFAULT_CODE_EXPIRE_MINUTES

            # 存储验证码数据
            verification_data = {
                "code": code,
                "attempts": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            # 存储到 Redis（设置过期时间）
            await redis_client.setex(
                verification_key, expire_time * 60, json.dumps(verification_data)
            )

            # 更新发送计数器（1 小时过期）- 使用原子操作
            current_count = await redis_client.incr(send_limit_key)
            # 如果是第一次设置，需要设置过期时间
            if current_count == 1:
                await redis_client.expire(send_limit_key, 3600)

            logger.info(f"验证码已生成并存储: {email}, 有效期: {expire_time} 分钟")
            return True, code, None

        except Exception as e:
            logger.error(f"发送验证码失败: {e}")
            return False, "系统错误", str(e)

    @staticmethod
    async def verify_code(email: str, code: str) -> Tuple[bool, str]:
        """
        验证验证码

        Args:
            email: 邮箱地址
            code: 用户输入的验证码

        Returns:
            (是否验证成功, 错误信息)
        """
        redis_client = await get_redis_client(require_redis=False)

        if redis_client is None:
            logger.error("Redis 不可用，无法验证验证码")
            return False, "系统错误"

        try:
            verification_key = f"{EmailVerificationService.VERIFICATION_PREFIX}{email}"
            data_str = await redis_client.get(verification_key)

            if not data_str:
                logger.warning(f"验证码不存在或已过期: {email}")
                return False, "验证码不存在或已过期"

            data = json.loads(data_str)

            # 检查尝试次数
            if data["attempts"] >= EmailVerificationService.DEFAULT_MAX_ATTEMPTS:
                logger.warning(f"验证码尝试次数过多: {email}")
                await redis_client.delete(verification_key)
                return False, "验证码尝试次数过多，请重新发送"

            # 增加尝试次数
            data["attempts"] += 1

            # 验证码比对 - 使用常量时间比较防止时序攻击
            if not secrets.compare_digest(code, data["code"]):
                # 更新尝试次数
                ttl = await redis_client.ttl(verification_key)
                if ttl > 0:
                    await redis_client.setex(verification_key, ttl, json.dumps(data))

                remaining_attempts = EmailVerificationService.DEFAULT_MAX_ATTEMPTS - data["attempts"]
                logger.warning(f"验证码错误: {email}, 剩余尝试次数: {remaining_attempts}")
                return False, f"验证码错误，剩余尝试次数: {remaining_attempts}"

            # 验证成功：删除验证码，标记邮箱已验证
            await redis_client.delete(verification_key)

            verified_key = f"{EmailVerificationService.VERIFIED_PREFIX}{email}"
            # 已验证标记保留 1 小时，足够完成注册流程
            await redis_client.setex(verified_key, 3600, "verified")

            logger.info(f"验证码验证成功: {email}")
            return True, "验证成功"

        except Exception as e:
            logger.error(f"验证验证码失败: {e}")
            return False, "系统错误"

    @staticmethod
    async def is_email_verified(email: str) -> bool:
        """
        检查邮箱是否已验证

        Args:
            email: 邮箱地址

        Returns:
            是否已验证
        """
        redis_client = await get_redis_client(require_redis=False)

        if redis_client is None:
            logger.warning("Redis 不可用，跳过邮箱验证检查")
            return False

        try:
            verified_key = f"{EmailVerificationService.VERIFIED_PREFIX}{email}"
            verified = await redis_client.exists(verified_key)
            return bool(verified)

        except Exception as e:
            logger.error(f"检查邮箱验证状态失败: {e}")
            return False

    @staticmethod
    async def clear_verification(email: str) -> bool:
        """
        清除邮箱验证状态（注册成功后调用）

        Args:
            email: 邮箱地址

        Returns:
            是否清除成功
        """
        redis_client = await get_redis_client(require_redis=False)

        if redis_client is None:
            logger.warning("Redis 不可用，无法清除验证状态")
            return False

        try:
            verified_key = f"{EmailVerificationService.VERIFIED_PREFIX}{email}"
            verification_key = f"{EmailVerificationService.VERIFICATION_PREFIX}{email}"

            # 删除已验证标记和验证码（如果还存在）
            await redis_client.delete(verified_key)
            await redis_client.delete(verification_key)

            logger.info(f"邮箱验证状态已清除: {email}")
            return True

        except Exception as e:
            logger.error(f"清除邮箱验证状态失败: {e}")
            return False

    @staticmethod
    async def get_verification_status(email: str) -> dict:
        """
        获取邮箱验证状态（用于调试和管理）

        Args:
            email: 邮箱地址

        Returns:
            验证状态信息
        """
        redis_client = await get_redis_client(require_redis=False)

        if redis_client is None:
            return {"error": "Redis 不可用"}

        try:
            verification_key = f"{EmailVerificationService.VERIFICATION_PREFIX}{email}"
            verified_key = f"{EmailVerificationService.VERIFIED_PREFIX}{email}"
            send_limit_key = f"{EmailVerificationService.SEND_LIMIT_PREFIX}{email}"

            # 获取各个状态
            verification_data = await redis_client.get(verification_key)
            is_verified = await redis_client.exists(verified_key)
            send_count = await redis_client.get(send_limit_key)
            verification_ttl = await redis_client.ttl(verification_key)
            verified_ttl = await redis_client.ttl(verified_key)

            status = {
                "email": email,
                "has_pending_code": bool(verification_data),
                "is_verified": bool(is_verified),
                "send_count_this_hour": int(send_count) if send_count else 0,
                "code_expires_in": verification_ttl if verification_ttl > 0 else None,
                "verified_expires_in": verified_ttl if verified_ttl > 0 else None,
            }

            if verification_data:
                data = json.loads(verification_data)
                status["attempts"] = data.get("attempts", 0)
                status["created_at"] = data.get("created_at")

            return status

        except Exception as e:
            logger.error(f"获取邮箱验证状态失败: {e}")
            return {"error": str(e)}
