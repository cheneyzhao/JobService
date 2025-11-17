"""
Job management API endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Response
from typing import Optional, Dict, Any
from pydantic import BaseModel
import logging
from workers.utils.task_manager import task_manager
from workers.services.database_service import DatabaseService
from log.backend_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/sitedata", tags=["sitedata"])

class FetchJobRequest(BaseModel):
    """Create fetch job request model"""
    siteId: str
    date: str  # YYYY-MM-DD format

class FetchJobResponse(BaseModel):
    """Create fetch job response model"""
    jobId: str
    status: str

@router.post("/fetch", response_model=FetchJobResponse)
async def create_fetch_job(request: FetchJobRequest, response: Response):
    """
    Create data fetch job
    
    Start an asynchronous job to fetch data from all sitedata providers
    """
    try:
        result = task_manager.create_fetch_job(request.siteId, request.date)
        
        if "error" in result:
            raise HTTPException(
                status_code=result.get("http_status", 500),
                detail=result["error"]
            )
        response.status_code = result.get("http_status", 200)
        return FetchJobResponse(
            jobId=result["jobId"],
            status=result["status"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating fetch job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Get job status
    
    Return the current status, statistics, and error information (if any) of the job
    """
    try:
        result = task_manager.get_job_status(job_id)
        result.pop("http_status", None)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/jobs/{job_id}/results")
async def get_job_results(
    job_id: str,
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    supplier: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    confirmed: Optional[bool] = Query(None),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    site_id: Optional[str] = Query(None, alias="siteId"),
    sort_by: str = Query("sitedataScore desc", alias="sortBy")
):
    """
    Get job results
    
    Return sitedata records for the specified job, with filtering and sorting support
    """
    try:
        filters = {}
        if supplier:
            filters["supplier"] = supplier
        if status:
            filters["status"] = status
        if confirmed is not None:
            filters["confirmed"] = confirmed
        if from_date:
            filters["from_date"] = from_date
        if to_date:
            filters["to_date"] = to_date
        if site_id:
            filters["site_id"] = site_id
        if sort_by:
            filters["sort_by"] = sort_by
        
        result = task_manager.get_job_results(
            job_id, limit, offset, **filters
        )        
       
        result.pop("http_status", None)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job results: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("")
async def get_all_sitedata(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    supplier: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    confirmed: Optional[bool] = Query(None),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    site_id: Optional[str] = Query(None, alias="siteId"),
    sort_by: str = Query("sitedataScore desc", alias="sortBy")
):
    """
    Get all sitedata records
    
    Return all sitedata records with filtering and sorting support, sorted by sitedataScore in descending order by default
    """
    try:
        filters = {}
        if supplier:
            filters["supplier"] = supplier
        if status:
            filters["status"] = status
        if confirmed is not None:
            filters["confirmed"] = confirmed
        if from_date:
            filters["from_date"] = from_date
        if to_date:
            filters["to_date"] = to_date
        if site_id:
            filters["site_id"] = site_id
        if sort_by:
            filters["sort_by"] = sort_by
        
        result = DatabaseService.get_all_sitedata(
            limit, offset, **filters
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting all sitedata: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """
    Cancel job
    
    Cancel a job in progress
    """
    try:
        result = task_manager.cancel_job(job_id)
        
        if "error" in result:
            raise HTTPException(
                status_code=result.get("http_status", 500),
                detail=result["error"]
            )
        
        result.pop("http_status", None)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

