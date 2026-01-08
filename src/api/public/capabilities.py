"""
能力配置公共 API

提供系统支持的能力列表，供前端展示和配置使用。
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.core.key_capabilities import (
    get_all_capabilities,
    get_user_configurable_capabilities,
)
from src.database import get_db

router = APIRouter(prefix="/api/capabilities", tags=["System Catalog"])


@router.get("")
async def list_capabilities():
    """
    获取所有能力定义

    返回系统中定义的所有能力（capabilities），包括用户可配置和系统内部使用的能力。
    能力用于描述模型支持的功能特性，如视觉输入、函数调用、流式输出等。

    **返回字段**
    - capabilities: 能力列表，每个能力包含：
      - name: 能力的唯一标识符（如 vision、function_calling）
      - display_name: 能力的显示名称（如"视觉输入"、"函数调用"）
      - short_name: 能力的简短名称（如"视觉"、"函数"）
      - description: 能力的详细描述
      - match_mode: 匹配模式（exact 精确匹配，fuzzy 模糊匹配，prefix 前缀匹配等）
      - config_mode: 配置模式（user_configurable 用户可配置，system_only 仅系统使用）
    """
    return {
        "capabilities": [
            {
                "name": cap.name,
                "display_name": cap.display_name,
                "short_name": cap.short_name,
                "description": cap.description,
                "match_mode": cap.match_mode.value,
                "config_mode": cap.config_mode.value,
            }
            for cap in get_all_capabilities()
        ]
    }


@router.get("/user-configurable")
async def list_user_configurable_capabilities():
    """
    获取用户可配置的能力列表

    返回允许用户在 API Key 中配置的能力列表，用于前端展示配置选项。
    用户可以通过配置这些能力来限制或指定 API Key 可以访问的模型功能。

    **返回字段**
    - capabilities: 用户可配置的能力列表，每个能力包含：
      - name: 能力的唯一标识符
      - display_name: 能力的显示名称
      - short_name: 能力的简短名称
      - description: 能力的详细描述
      - match_mode: 匹配模式（exact、fuzzy、prefix 等）
      - config_mode: 配置模式（此接口返回的都是 user_configurable）
    """
    return {
        "capabilities": [
            {
                "name": cap.name,
                "display_name": cap.display_name,
                "short_name": cap.short_name,
                "description": cap.description,
                "match_mode": cap.match_mode.value,
                "config_mode": cap.config_mode.value,
            }
            for cap in get_user_configurable_capabilities()
        ]
    }


@router.get("/model/{model_name}")
async def get_model_supported_capabilities(
    model_name: str,
    db: Session = Depends(get_db),
):
    """
    获取指定模型支持的能力列表

    根据全局模型名称（GlobalModel.name）查询该模型支持的能力，
    并返回每个能力的详细定义。只查询活跃的全局模型。

    **路径参数**
    - model_name: 全局模型名称（如 claude-sonnet-4-20250514，必须是 GlobalModel.name）

    **返回字段**
    - model: 查询的模型名称
    - global_model_id: 全局模型的 UUID
    - global_model_name: 全局模型的标准名称
    - supported_capabilities: 该模型支持的能力名称列表
    - capability_details: 支持的能力详细信息列表，每个能力包含：
      - name: 能力标识符
      - display_name: 能力显示名称
      - description: 能力描述
      - match_mode: 匹配模式
      - config_mode: 配置模式
    - error: 错误信息（仅在模型不存在时返回）
    """
    from src.models.database import GlobalModel

    global_model = (
        db.query(GlobalModel)
        .filter(GlobalModel.name == model_name, GlobalModel.is_active == True)
        .first()
    )

    if not global_model:
        return {
            "model": model_name,
            "supported_capabilities": [],
            "capability_details": [],
            "error": "模型不存在",
        }

    supported_caps = global_model.supported_capabilities or []

    # 获取支持的能力详情
    all_caps = {cap.name: cap for cap in get_all_capabilities()}
    capability_details = []
    for cap_name in supported_caps:
        if cap_name in all_caps:
            cap = all_caps[cap_name]
            capability_details.append({
                "name": cap.name,
                "display_name": cap.display_name,
                "description": cap.description,
                "match_mode": cap.match_mode.value,
                "config_mode": cap.config_mode.value,
            })

    return {
        "model": model_name,
        "global_model_id": str(global_model.id),
        "global_model_name": global_model.name,
        "supported_capabilities": supported_caps,
        "capability_details": capability_details,
    }
