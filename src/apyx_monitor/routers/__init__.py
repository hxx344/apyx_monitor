from .alerts import router as alerts_router
from .dashboard import router as dashboard_router
from .health import router as health_router
from .jobs import router as jobs_router
from .metrics import router as metrics_router

__all__ = ["alerts_router", "dashboard_router", "health_router", "jobs_router", "metrics_router"]
