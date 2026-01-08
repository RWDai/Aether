"""Routes for authenticated user self-service APIs."""

from fastapi import APIRouter

from .management_tokens import router as management_tokens_router
from .routes import router as me_router

router = APIRouter()
router.include_router(me_router)
router.include_router(management_tokens_router)

__all__ = ["router"]
