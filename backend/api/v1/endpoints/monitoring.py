"""
Monitoring and management API endpoints
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging
from workers.utils.task_manager import task_manager
from cache.cache_service import cache_service
from sqlalchemy import text
from log.backend_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])

@router.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Check the health status of system components
    """
    try:
        # Check Redis connection
        from workers.celery_app import celery_app
        redis_status = "ok"
        try:
            celery_app.control.inspect().ping()
        except Exception as e:
            redis_status = f"error: {str(e)}"
        
        # Check database connection
        db_status = "ok"
        try:
            from db.database import engine
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            db_status = f"error: {str(e)}"
        
        return {
            "status": "healthy" if redis_status == "ok" and db_status == "ok" else "unhealthy",
            "components": {
                "redis": redis_status,
                "database": db_status,
                "celery": redis_status  # Celery depends on Redis
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")

@router.get("/tasks/{task_id}")
async def get_task_info(task_id: str):
    """
    Get Celery task information
    
    Return detailed information and status of the specified task
    """
    try:
        result = task_manager.get_task_info(task_id)
        
        if "error" in result:
            raise HTTPException(
                status_code=result.get("http_status", 500),
                detail=result["error"]
            )
        
        # Remove http_status field
        result.pop("http_status", None)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/workers")
async def get_worker_info():
    """
    Get worker information
    
    Return information about currently active Celery workers
    """
    try:
        from workers.celery_app import celery_app
        
        # Get active workers
        inspect = celery_app.control.inspect()
        active_workers = inspect.active()
        registered_tasks = inspect.registered()
        worker_stats = inspect.stats()
        
        return {
            "active_workers": active_workers or {},
            "registered_tasks": registered_tasks or {},
            "worker_stats": worker_stats or {}
        }
        
    except Exception as e:
        logger.error(f"Error getting worker info: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting worker info: {str(e)}")

@router.get("/queues")
async def get_queue_info():
    """
    Get queue information
    
    Return status information of Celery queues
    """
    try:
        from workers.celery_app import celery_app
        
        # Get queue length information
        inspect = celery_app.control.inspect()
        active_queues = inspect.active_queues()
        
        return {
            "active_queues": active_queues or {}
        }
        
    except Exception as e:
        logger.error(f"Error getting queue info: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting queue info: {str(e)}")


@router.get("/system/info")
async def get_system_info():
    """
    Get system information
    
    Return system configuration and runtime information
    """
    try:
        from workers.config import WorkerConfig
        import platform
        import sys
        
        return {
            "system": {
                "platform": platform.platform(),
                "python_version": sys.version,
                "architecture": platform.architecture()
            },
            "config": {
                "redis_host": WorkerConfig.REDIS_HOST,
                "redis_port": WorkerConfig.REDIS_PORT,
                "db_host": WorkerConfig.DB_HOST,
                "db_port": WorkerConfig.DB_PORT,
                "http_timeout": WorkerConfig.HTTP_TIMEOUT
            },
            "sites": WorkerConfig.get_sites_config()
        }
        
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cache/stats")
async def get_cache_stats():
    """
    Get cache statistics
    
    Return current cache usage and statistics data
    """
    try:
        stats = cache_service.get_stats()
        return {
            "cache_stats": stats,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.delete("/cache")
async def clear_cache():
    """
    Clear all cache
    
    Clear all cached data in memory
    """
    try:
        cache_service.clear()
        return {
            "message": "Cache cleared successfully",
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")