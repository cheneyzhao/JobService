"""
API v1 module initialization
"""
from fastapi import APIRouter
from .endpoints import jobs, monitoring, scheduler

api_router = APIRouter()


api_router.include_router(jobs.router)
api_router.include_router(monitoring.router)
api_router.include_router(scheduler.router)