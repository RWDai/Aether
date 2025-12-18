"""
全局HTTP客户端池管理
避免每次请求都创建新的AsyncClient,提高性能
"""

from contextlib import asynccontextmanager
from typing import Any, Dict, Optional
from urllib.parse import quote, urlparse

import httpx

from src.core.logger import logger


def build_proxy_url(proxy_config: Dict[str, Any]) -> Optional[str]:
    """
    根据代理配置构建完整的代理 URL

    Args:
        proxy_config: 代理配置字典，包含 url, username, password, enabled

    Returns:
        完整的代理 URL，如 socks5://user:pass@host:port
        如果 enabled=False 或无配置，返回 None
    """
    if not proxy_config:
        return None

    # 检查 enabled 字段，默认为 True（兼容旧数据）
    if not proxy_config.get("enabled", True):
        return None

    proxy_url = proxy_config.get("url")
    if not proxy_url:
        return None

    username = proxy_config.get("username")
    password = proxy_config.get("password")

    # 只要有用户名就添加认证信息（密码可以为空）
    if username:
        parsed = urlparse(proxy_url)
        # URL 编码用户名和密码，处理特殊字符（如 @, :, /）
        encoded_username = quote(username, safe="")
        encoded_password = quote(password, safe="") if password else ""
        # 重新构建带认证的代理 URL
        if encoded_password:
            auth_proxy = f"{parsed.scheme}://{encoded_username}:{encoded_password}@{parsed.netloc}"
        else:
            auth_proxy = f"{parsed.scheme}://{encoded_username}@{parsed.netloc}"
        if parsed.path:
            auth_proxy += parsed.path
        return auth_proxy

    return proxy_url


class HTTPClientPool:
    """
    全局HTTP客户端池单例

    管理可重用的httpx.AsyncClient实例,避免频繁创建/销毁连接
    """

    _instance: Optional["HTTPClientPool"] = None
    _default_client: Optional[httpx.AsyncClient] = None
    _clients: Dict[str, httpx.AsyncClient] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_default_client(cls) -> httpx.AsyncClient:
        """
        获取默认的HTTP客户端

        用于大多数HTTP请求,具有合理的默认配置
        """
        if cls._default_client is None:
            cls._default_client = httpx.AsyncClient(
                http2=False,  # 暂时禁用HTTP/2以提高兼容性
                verify=True,  # 启用SSL验证
                timeout=httpx.Timeout(
                    connect=10.0,  # 连接超时
                    read=300.0,  # 读取超时(5分钟,适合流式响应)
                    write=60.0,  # 写入超时(60秒,支持大请求体)
                    pool=5.0,  # 连接池超时
                ),
                limits=httpx.Limits(
                    max_connections=100,  # 最大连接数
                    max_keepalive_connections=20,  # 最大保活连接数
                    keepalive_expiry=30.0,  # 保活过期时间(秒)
                ),
                follow_redirects=True,  # 跟随重定向
            )
            logger.info("全局HTTP客户端池已初始化")
        return cls._default_client

    @classmethod
    def get_client(cls, name: str, **kwargs: Any) -> httpx.AsyncClient:
        """
        获取或创建命名的HTTP客户端

        用于需要特定配置的场景(如不同的超时设置、代理等)

        Args:
            name: 客户端标识符
            **kwargs: httpx.AsyncClient的配置参数
        """
        if name not in cls._clients:
            # 合并默认配置和自定义配置
            config = {
                "http2": False,
                "verify": True,
                "timeout": httpx.Timeout(10.0, read=300.0),
                "follow_redirects": True,
            }
            config.update(kwargs)

            cls._clients[name] = httpx.AsyncClient(**config)
            logger.debug(f"创建命名HTTP客户端: {name}")

        return cls._clients[name]

    @classmethod
    async def close_all(cls):
        """关闭所有HTTP客户端"""
        if cls._default_client is not None:
            await cls._default_client.aclose()
            cls._default_client = None
            logger.info("默认HTTP客户端已关闭")

        for name, client in cls._clients.items():
            await client.aclose()
            logger.debug(f"命名HTTP客户端已关闭: {name}")

        cls._clients.clear()
        logger.info("所有HTTP客户端已关闭")

    @classmethod
    @asynccontextmanager
    async def get_temp_client(cls, **kwargs: Any):
        """
        获取临时HTTP客户端(上下文管理器)

        用于一次性请求,使用后自动关闭

        用法:
            async with HTTPClientPool.get_temp_client() as client:
                response = await client.get('https://example.com')
        """
        config = {
            "http2": False,
            "verify": True,
            "timeout": httpx.Timeout(10.0),
        }
        config.update(kwargs)

        client = httpx.AsyncClient(**config)
        try:
            yield client
        finally:
            await client.aclose()

    @classmethod
    def create_client_with_proxy(
        cls,
        proxy_config: Optional[Dict[str, Any]] = None,
        timeout: Optional[httpx.Timeout] = None,
        **kwargs: Any,
    ) -> httpx.AsyncClient:
        """
        创建带代理配置的HTTP客户端

        Args:
            proxy_config: 代理配置字典，包含 url, username, password
            timeout: 超时配置
            **kwargs: 其他 httpx.AsyncClient 配置参数

        Returns:
            配置好的 httpx.AsyncClient 实例
        """
        config: Dict[str, Any] = {
            "http2": False,
            "verify": True,
            "follow_redirects": True,
        }

        if timeout:
            config["timeout"] = timeout
        else:
            config["timeout"] = httpx.Timeout(10.0, read=300.0)

        # 添加代理配置
        proxy_url = build_proxy_url(proxy_config) if proxy_config else None
        if proxy_url:
            config["proxy"] = proxy_url
            logger.debug(f"创建带代理的HTTP客户端: {proxy_config.get('url', 'unknown')}")

        config.update(kwargs)
        return httpx.AsyncClient(**config)


# 便捷访问函数
def get_http_client() -> httpx.AsyncClient:
    """获取默认HTTP客户端的便捷函数"""
    return HTTPClientPool.get_default_client()


async def close_http_clients():
    """关闭所有HTTP客户端的便捷函数"""
    await HTTPClientPool.close_all()
