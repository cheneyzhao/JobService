"""
Task manager
Responsible for task creation, status tracking and lifecycle management
"""
import logging
from typing import Dict, Any, Optional, List
from celery import current_app
from celery.result import AsyncResult
from workers.services.database_service import DatabaseService
from db.models.job import Job, JobStatus
from workers.tasks.job_coordinator import start_fetch_job
from log.backend_logger import get_logger

logger = get_logger(__name__)

class TaskManager:
    """Task manager"""
    
    def __init__(self):
        self.celery_app = current_app
    
    def create_fetch_job(self, site_id: str, date: str) -> Dict[str, Any]:
        """
        Create data fetch job
        
        Args:
            site_id: Site ID
            date: Date (YYYY-MM-DD)
            
        Returns:
            Job creation result
        """
        try:
            # Check if there are jobs in progress
            existing_job = DatabaseService.find_existing_job(
                site_id, date, [JobStatus.CREATED, JobStatus.PROCESSING]
            )        
            if existing_job:
                logger.info(f"Found existing job {existing_job['id']} for site {site_id}, date {date}")
                return {
                    "jobId": existing_job["id"],
                    "status": existing_job["status"],
                    "message": "Job already exists",
                    "http_status": 202  # Accepted
                }
            
            # Create new job
            input_params = {"siteId": site_id, "date": date}
            job_id = DatabaseService.create_job(site_id, date, input_params)
            # Start asynchronous task
            task_result = start_fetch_job.delay(job_id, site_id, date)
                
            logger.info(f"Created new fetch job {job_id} with task {task_result.id}")
            
            return {
                "jobId": job_id,
                "status": JobStatus.CREATED.value,
                "message": "Job created successfully",
                "http_status": 201  # Created
            }
            
        except Exception as e:
            error_msg = f"Failed to create fetch job for site {site_id}, date {date}: {str(e)}"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "http_status": 500  # Internal Server Error
            }
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status
        
        Args:
            job_id: Job ID
            
        Returns:
            Job status information
        """
        try:
            job = DatabaseService.get_job_by_id(job_id)
            
            if not job:
                return {
                    "error": f"Job {job_id} not found",
                    "http_status": 404  # Not Found
                }
            
            # Get basic job information
            job_info = {
                "jobId": job["id"],
                "status": job["status"],
                "createdAt": job["created_at"].isoformat() + 'Z' if job["created_at"] else None,
                "updatedAt": job["updated_at"].isoformat() + 'Z' if job["updated_at"] else None,
                "input": job["input_params"] or {},
                "stats": job["stats"] or {},
                "error": job["error_message"]
            }
            
            # If job is processing, try to get Celery task status
            if job["status"] == JobStatus.PROCESSING and job["celery_task_id"]:
                celery_result = AsyncResult(job["celery_task_id"], app=self.celery_app)
                
                # Update task status information
                job_info["celery_status"] = celery_result.status
                job_info["celery_info"] = celery_result.info if celery_result.info else {}
            
            return {
                **job_info,
                "http_status": 200  # OK
            }
            
        except Exception as e:
            error_msg = f"Failed to get job status for {job_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "http_status": 500  # Internal Server Error
            }
    
    def get_job_results(
        self,
        job_id: str,
        limit: int = 50,
        offset: int = 0,
        **filters
    ) -> Dict[str, Any]:
        """
        Get job results
        
        Args:
            job_id: Job ID
            limit: Limit count
            offset: Offset
            **filters: Filter conditions
            
        Returns:
            Job results
        """
        try:
            # Check if job exists
            job = DatabaseService.get_job_by_id(job_id)
            if not job:
                return {
                    "error": f"Job {job_id} not found",
                    "http_status": 404  # Not Found
                }
            
            # Get sitedata records
            results = DatabaseService.get_job_sitedata(
                job_id, limit, offset, **filters
            )
            
            return {
                **results,
                "http_status": 200  # OK
            }
            
        except Exception as e:
            error_msg = f"Failed to get job results for {job_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "http_status": 500  # Internal Server Error
            }
    
    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel job
        
        Args:
            job_id: Job ID
            
        Returns:
            Cancellation result
        """
        try:
            job = DatabaseService.get_job_by_id(job_id)
            
            if not job:
                return {
                    "error": f"Job {job_id} not found",
                    "http_status": 404  # Not Found
                }
            
            # Can only cancel jobs in progress
            if job["status"] not in [JobStatus.CREATED, JobStatus.PROCESSING]:
                return {
                    "error": f"Cannot cancel job in status {job['status']}",
                    "http_status": 400  # Bad Request
                }
            
            # Cancel Celery task
            if job["celery_task_id"]:
                celery_result = AsyncResult(job["celery_task_id"], app=self.celery_app)
                celery_result.revoke(terminate=True)
                logger.info(f"Revoked Celery task {job['celery_task_id']} for job {job_id}")
            
            # Update job status
            DatabaseService.update_job_status(job_id, JobStatus.FAILED, "Job cancelled by user")
            
            return {
                "jobId": job_id,
                "status": "cancelled",
                "message": "Job cancelled successfully",
                "http_status": 200  # OK
            }
            
        except Exception as e:
            error_msg = f"Failed to cancel job {job_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "http_status": 500  # Internal Server Error
            }    

    def get_task_info(self, task_id: str) -> Dict[str, Any]:
        """
        Get Celery task information
        
        Args:
            task_id: Celery task ID
            
        Returns:
            Task information
        """
        try:
            result = AsyncResult(task_id, app=self.celery_app)
            
            return {
                "task_id": task_id,
                "status": result.status,
                "result": result.result,
                "traceback": result.traceback,
                "http_status": 200  # OK
            }
            
        except Exception as e:
            error_msg = f"Failed to get task info for {task_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "http_status": 500  # Internal Server Error
            }

# Global task manager instance
task_manager = TaskManager()