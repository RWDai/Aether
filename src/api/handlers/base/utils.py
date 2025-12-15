"""
Handler 基础工具函数
"""

from typing import Any, Dict


def extract_cache_creation_tokens(usage: Dict[str, Any]) -> int:
    """
    提取缓存创建 tokens（兼容新旧格式）

    Claude API 在不同版本中使用了不同的字段名来表示缓存创建 tokens：
    - 新格式（2024年后）：使用 claude_cache_creation_5_m_tokens 和
      claude_cache_creation_1_h_tokens 分别表示 5 分钟和 1 小时缓存
    - 旧格式：使用 cache_creation_input_tokens 表示总的缓存创建 tokens

    此函数自动检测并适配两种格式，优先使用新格式。

    Args:
        usage: API 响应中的 usage 字典

    Returns:
        缓存创建 tokens 总数
    """
    # 优先使用新格式
    cache_5m = usage.get("claude_cache_creation_5_m_tokens", 0)
    cache_1h = usage.get("claude_cache_creation_1_h_tokens", 0)
    total = int(cache_5m) + int(cache_1h)

    # 如果新格式不存在（total == 0），回退到旧格式
    return total if total > 0 else int(usage.get("cache_creation_input_tokens", 0))
