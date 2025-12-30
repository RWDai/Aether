"""
邮件发送服务
提供 SMTP 邮件发送功能
"""

import asyncio
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, Tuple

try:
    import aiosmtplib

    AIOSMTPLIB_AVAILABLE = True
except ImportError:
    AIOSMTPLIB_AVAILABLE = False
    aiosmtplib = None

from sqlalchemy.orm import Session

from src.core.logger import logger
from src.services.system.config import ConfigService

from .email_template import EmailTemplate


class EmailSenderService:
    """邮件发送服务"""

    @staticmethod
    def _get_smtp_config(db: Session) -> dict:
        """
        从数据库获取 SMTP 配置

        Args:
            db: 数据库会话

        Returns:
            SMTP 配置字典
        """
        config = {
            "smtp_host": ConfigService.get_config(db, "smtp_host"),
            "smtp_port": ConfigService.get_config(db, "smtp_port", default=587),
            "smtp_user": ConfigService.get_config(db, "smtp_user"),
            "smtp_password": ConfigService.get_config(db, "smtp_password"),
            "smtp_use_tls": ConfigService.get_config(db, "smtp_use_tls", default=True),
            "smtp_use_ssl": ConfigService.get_config(db, "smtp_use_ssl", default=False),
            "smtp_from_email": ConfigService.get_config(db, "smtp_from_email"),
            "smtp_from_name": ConfigService.get_config(db, "smtp_from_name", default="Aether"),
        }
        return config

    @staticmethod
    def _validate_smtp_config(config: dict) -> Tuple[bool, Optional[str]]:
        """
        验证 SMTP 配置

        Args:
            config: SMTP 配置字典

        Returns:
            (是否有效, 错误信息)
        """
        required_fields = ["smtp_host", "smtp_from_email"]

        for field in required_fields:
            if not config.get(field):
                return False, f"缺少必要的 SMTP 配置: {field}"

        return True, None

    @staticmethod
    async def send_verification_code(
        db: Session, to_email: str, code: str, expire_minutes: int = 30
    ) -> Tuple[bool, Optional[str]]:
        """
        发送验证码邮件

        Args:
            db: 数据库会话
            to_email: 收件人邮箱
            code: 验证码
            expire_minutes: 过期时间（分钟）

        Returns:
            (是否发送成功, 错误信息)
        """
        # 获取 SMTP 配置
        config = EmailSenderService._get_smtp_config(db)

        # 验证配置
        valid, error = EmailSenderService._validate_smtp_config(config)
        if not valid:
            logger.error(f"SMTP 配置无效: {error}")
            return False, error

        # 生成邮件内容
        app_name = ConfigService.get_config(db, "smtp_from_name", default="Aether")
        support_email = ConfigService.get_config(db, "smtp_support_email")

        html_body = EmailTemplate.get_verification_code_html(
            code=code, expire_minutes=expire_minutes, app_name=app_name, support_email=support_email
        )
        text_body = EmailTemplate.get_verification_code_text(
            code=code, expire_minutes=expire_minutes, app_name=app_name, support_email=support_email
        )
        subject = EmailTemplate.get_subject("verification")

        # 发送邮件
        return await EmailSenderService._send_email(
            config=config, to_email=to_email, subject=subject, html_body=html_body, text_body=text_body
        )

    @staticmethod
    async def _send_email(
        config: dict,
        to_email: str,
        subject: str,
        html_body: Optional[str] = None,
        text_body: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        发送邮件（内部方法）

        Args:
            config: SMTP 配置
            to_email: 收件人邮箱
            subject: 邮件主题
            html_body: HTML 邮件内容
            text_body: 纯文本邮件内容

        Returns:
            (是否发送成功, 错误信息)
        """
        if AIOSMTPLIB_AVAILABLE:
            return await EmailSenderService._send_email_async(
                config, to_email, subject, html_body, text_body
            )
        else:
            return await EmailSenderService._send_email_sync_wrapper(
                config, to_email, subject, html_body, text_body
            )

    @staticmethod
    async def _send_email_async(
        config: dict,
        to_email: str,
        subject: str,
        html_body: Optional[str] = None,
        text_body: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        异步发送邮件（使用 aiosmtplib）

        Args:
            config: SMTP 配置
            to_email: 收件人邮箱
            subject: 邮件主题
            html_body: HTML 邮件内容
            text_body: 纯文本邮件内容

        Returns:
            (是否发送成功, 错误信息)
        """
        try:
            # 构建邮件
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = f"{config['smtp_from_name']} <{config['smtp_from_email']}>"
            message["To"] = to_email

            # 添加纯文本部分
            if text_body:
                message.attach(MIMEText(text_body, "plain", "utf-8"))

            # 添加 HTML 部分
            if html_body:
                message.attach(MIMEText(html_body, "html", "utf-8"))

            # 发送邮件
            if config["smtp_use_ssl"]:
                await aiosmtplib.send(
                    message,
                    hostname=config["smtp_host"],
                    port=config["smtp_port"],
                    use_tls=True,
                    username=config["smtp_user"],
                    password=config["smtp_password"],
                )
            else:
                await aiosmtplib.send(
                    message,
                    hostname=config["smtp_host"],
                    port=config["smtp_port"],
                    start_tls=config["smtp_use_tls"],
                    username=config["smtp_user"],
                    password=config["smtp_password"],
                )

            logger.info(f"验证码邮件发送成功: {to_email}")
            return True, None

        except Exception as e:
            error_msg = f"发送邮件失败: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    @staticmethod
    async def _send_email_sync_wrapper(
        config: dict,
        to_email: str,
        subject: str,
        html_body: Optional[str] = None,
        text_body: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        同步邮件发送的异步包装器

        Args:
            config: SMTP 配置
            to_email: 收件人邮箱
            subject: 邮件主题
            html_body: HTML 邮件内容
            text_body: 纯文本邮件内容

        Returns:
            (是否发送成功, 错误信息)
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, EmailSenderService._send_email_sync, config, to_email, subject, html_body, text_body
        )

    @staticmethod
    def _send_email_sync(
        config: dict,
        to_email: str,
        subject: str,
        html_body: Optional[str] = None,
        text_body: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        同步发送邮件（使用标准库 smtplib）

        Args:
            config: SMTP 配置
            to_email: 收件人邮箱
            subject: 邮件主题
            html_body: HTML 邮件内容
            text_body: 纯文本邮件内容

        Returns:
            (是否发送成功, 错误信息)
        """
        try:
            # 构建邮件
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = f"{config['smtp_from_name']} <{config['smtp_from_email']}>"
            message["To"] = to_email

            # 添加纯文本部分
            if text_body:
                message.attach(MIMEText(text_body, "plain", "utf-8"))

            # 添加 HTML 部分
            if html_body:
                message.attach(MIMEText(html_body, "html", "utf-8"))

            # 连接 SMTP 服务器
            server = None
            try:
                if config["smtp_use_ssl"]:
                    server = smtplib.SMTP_SSL(config["smtp_host"], config["smtp_port"])
                else:
                    server = smtplib.SMTP(config["smtp_host"], config["smtp_port"])
                    if config["smtp_use_tls"]:
                        server.starttls()

                # 登录
                if config["smtp_user"] and config["smtp_password"]:
                    server.login(config["smtp_user"], config["smtp_password"])

                # 发送邮件
                server.send_message(message)

                logger.info(f"验证码邮件发送成功（同步方式）: {to_email}")
                return True, None
            finally:
                # 确保服务器连接被关闭
                if server is not None:
                    try:
                        server.quit()
                    except Exception as quit_error:
                        logger.warning(f"关闭 SMTP 连接时出错: {quit_error}")

        except Exception as e:
            error_msg = f"发送邮件失败: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    @staticmethod
    async def test_smtp_connection(
        db: Session, override_config: Optional[dict] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        测试 SMTP 连接

        Args:
            db: 数据库会话
            override_config: 可选的覆盖配置（通常来自未保存的前端表单）

        Returns:
            (是否连接成功, 错误信息)
        """
        config = EmailSenderService._get_smtp_config(db)

        # 用外部传入的配置覆盖（仅覆盖提供的字段）
        if override_config:
            config.update({k: v for k, v in override_config.items() if v is not None})

        # 验证配置
        valid, error = EmailSenderService._validate_smtp_config(config)
        if not valid:
            return False, error

        try:
            if AIOSMTPLIB_AVAILABLE:
                # 使用异步方式测试
                smtp = aiosmtplib.SMTP(
                    hostname=config["smtp_host"],
                    port=config["smtp_port"],
                    use_tls=config["smtp_use_ssl"],
                )
                await smtp.connect()

                if config["smtp_use_tls"] and not config["smtp_use_ssl"]:
                    await smtp.starttls()

                if config["smtp_user"] and config["smtp_password"]:
                    await smtp.login(config["smtp_user"], config["smtp_password"])

                await smtp.quit()
            else:
                # 使用同步方式测试
                if config["smtp_use_ssl"]:
                    server = smtplib.SMTP_SSL(config["smtp_host"], config["smtp_port"])
                else:
                    server = smtplib.SMTP(config["smtp_host"], config["smtp_port"])
                    if config["smtp_use_tls"]:
                        server.starttls()

                if config["smtp_user"] and config["smtp_password"]:
                    server.login(config["smtp_user"], config["smtp_password"])

                server.quit()

            logger.info("SMTP 连接测试成功")
            return True, None

        except Exception as e:
            error_msg = f"SMTP 连接测试失败: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
