"""
流式处理上下文 - 类型安全的数据类替代 dict

提供流式请求处理过程中的状态跟踪，包括：
- Provider/Endpoint/Key 信息
- Token 统计
- 响应状态
- 请求/响应数据
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StreamContext:
    """
    流式处理上下文

    用于在流式请求处理过程中跟踪状态，替代原有的 ctx dict。
    所有字段都有类型注解，提供更好的 IDE 支持和运行时类型安全。
    """

    # 请求基本信息
    model: str
    api_format: str

    # Provider 信息（在请求执行时填充）
    provider_name: Optional[str] = None
    provider_id: Optional[str] = None
    endpoint_id: Optional[str] = None
    key_id: Optional[str] = None
    attempt_id: Optional[str] = None
    provider_api_format: Optional[str] = None  # Provider 的响应格式

    # 模型映射
    mapped_model: Optional[str] = None

    # Token 统计
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    cache_creation_tokens: int = 0

    # 响应内容
    collected_text: str = ""

    # 响应状态
    status_code: int = 200
    error_message: Optional[str] = None
    has_completion: bool = False

    # 请求/响应数据
    response_headers: Dict[str, str] = field(default_factory=dict)
    provider_request_headers: Dict[str, str] = field(default_factory=dict)
    provider_request_body: Optional[Dict[str, Any]] = None

    # 流式处理统计
    data_count: int = 0
    chunk_count: int = 0
    parsed_chunks: List[Dict[str, Any]] = field(default_factory=list)

    def reset_for_retry(self) -> None:
        """
        重试时重置状态

        在故障转移重试时调用，清除之前的数据避免累积。
        保留 model 和 api_format，重置其他所有状态。
        """
        self.parsed_chunks = []
        self.chunk_count = 0
        self.data_count = 0
        self.has_completion = False
        self.collected_text = ""
        self.input_tokens = 0
        self.output_tokens = 0
        self.cached_tokens = 0
        self.cache_creation_tokens = 0
        self.error_message = None
        self.status_code = 200
        self.response_headers = {}
        self.provider_request_headers = {}
        self.provider_request_body = None

    def update_provider_info(
        self,
        provider_name: str,
        provider_id: str,
        endpoint_id: str,
        key_id: str,
        provider_api_format: Optional[str] = None,
    ) -> None:
        """更新 Provider 信息"""
        self.provider_name = provider_name
        self.provider_id = provider_id
        self.endpoint_id = endpoint_id
        self.key_id = key_id
        self.provider_api_format = provider_api_format

    def update_usage(
        self,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        cached_tokens: Optional[int] = None,
        cache_creation_tokens: Optional[int] = None,
    ) -> None:
        """
        更新 Token 使用统计

        采用防御性更新策略：只有当新值 > 0 或当前值为 0 时才更新，避免用 0 覆盖已有的正确值。

        设计原理：
        - 在流式响应中，某些事件可能不包含完整的 usage 信息（字段为 0 或不存在）
        - 后续事件可能会提供完整的统计数据
        - 通过这种策略，确保一旦获得非零值就保留它，不会被后续的 0 值覆盖

        示例场景：
        - message_start 事件：input_tokens=100, output_tokens=0
        - message_delta 事件：input_tokens=0, output_tokens=50
        - 最终结果：input_tokens=100, output_tokens=50

        注意事项：
        - 此策略假设初始值为 0 是正确的默认状态
        - 如果需要将已有值重置为 0，请直接修改实例属性（不使用此方法）

        Args:
            input_tokens: 输入 tokens 数量
            output_tokens: 输出 tokens 数量
            cached_tokens: 缓存命中 tokens 数量
            cache_creation_tokens: 缓存创建 tokens 数量
        """
        if input_tokens is not None and (input_tokens > 0 or self.input_tokens == 0):
            self.input_tokens = input_tokens
        if output_tokens is not None and (output_tokens > 0 or self.output_tokens == 0):
            self.output_tokens = output_tokens
        if cached_tokens is not None and (cached_tokens > 0 or self.cached_tokens == 0):
            self.cached_tokens = cached_tokens
        if cache_creation_tokens is not None and (
            cache_creation_tokens > 0 or self.cache_creation_tokens == 0
        ):
            self.cache_creation_tokens = cache_creation_tokens

    def mark_failed(self, status_code: int, error_message: str) -> None:
        """标记请求失败"""
        self.status_code = status_code
        self.error_message = error_message

    def is_success(self) -> bool:
        """检查请求是否成功"""
        return self.status_code < 400

    def build_response_body(self, response_time_ms: int) -> Dict[str, Any]:
        """
        构建响应体元数据

        用于记录到 Usage 表的 response_body 字段。
        """
        return {
            "chunks": self.parsed_chunks,
            "metadata": {
                "stream": True,
                "total_chunks": len(self.parsed_chunks),
                "data_count": self.data_count,
                "has_completion": self.has_completion,
                "response_time_ms": response_time_ms,
            },
        }

    def get_log_summary(self, request_id: str, response_time_ms: int) -> str:
        """
        获取日志摘要

        用于请求完成/失败时的日志输出。
        """
        status = "OK" if self.is_success() else "FAIL"
        return (
            f"[{status}] {request_id[:8]} | {self.model} | "
            f"{self.provider_name or 'unknown'} | {response_time_ms}ms | "
            f"in:{self.input_tokens} out:{self.output_tokens}"
        )
