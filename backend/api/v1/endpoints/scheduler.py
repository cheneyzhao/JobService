"""
Scheduler Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from pydantic import BaseModel
import logging
from workers.services.scheduler_service import scheduler_service
from log.backend_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/scheduler", tags=["scheduler"])

@router.get("/status")
async def get_scheduler_status():
    """
    Get scheduler status
    
    Returns scheduler running status and statistics
    """
    try:
        status = scheduler_service.get_status()
        return {
            "scheduler_status": status,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/start")
async def start_scheduler():
    """
    Start scheduler
    
    Start the scheduled task scheduler and begin executing predefined tasks
    """
    try:
        if scheduler_service.is_running:
            return {
                "message": "Scheduler is already running",
                "status": "success"
            }
        
        scheduler_service.start()
        return {
            "message": "Scheduler started successfully",
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error starting scheduler: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {str(e)}")

@router.post("/stop")
async def stop_scheduler():
    """
    Stop scheduler
    
    Stop the scheduled task scheduler, all scheduled tasks will stop executing
    """
    try:
        if not scheduler_service.is_running:
            return {
                "message": "Scheduler is not running",
                "status": "success"
            }
        
        scheduler_service.stop()
        return {
            "message": "Scheduler stopped successfully",
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {str(e)}")

@router.get("/sites-configs")
async def get_scheduled_configs():
    """
    Get predefined scheduled configurations
    
    Returns all predefined site and date configurations
    """
    try:
        configs = scheduler_service.scheduled_configs
        enabled_count = len([c for c in configs if c.get("enabled", True)])
        
        return {
            "configs": configs,
            "total": len(configs),
            "enabled": enabled_count,
            "disabled": len(configs) - enabled_count,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error getting scheduled configs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/next-runs")
async def get_next_run_times():
    """
    Get next run times
    
    Returns the next execution times for all scheduled jobs
    """
    try:
        jobs = scheduler_service.get_jobs()
        next_runs = []
        
        for job in jobs:
            if job.get("next_run_time"):
                next_runs.append({
                    "job_id": job["id"],
                    "job_name": job["name"],
                    "next_run_time": job["next_run_time"],
                    "trigger": job["trigger"]
                })
        
        # Sort by next execution time
        next_runs.sort(key=lambda x: x["next_run_time"])
        
        return {
            "next_runs": next_runs,
            "total": len(next_runs),
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error getting next run times: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")