"""
Job coordinator task
Responsible for managing and coordinating the execution of multiple worker tasks
"""
import asyncio
from typing import Dict, Any, List, Optional
from celery import group
from celery import chord
from celery.result import AsyncResult
from celery.result import allow_join_result
from celery.exceptions import Retry
import logging
from workers.celery_app import celery_app
from workers.services.database_service import DatabaseService
from workers.services.data_transformer import DataTransformer
from db.models.job import JobStatus
from workers.tasks.data_fetcher import fetch_provider_data
from workers.config import WorkerConfig
from log.backend_logger import get_logger

logger = get_logger(__name__)

@celery_app.task(bind=True, name="backend.workers.tasks.job_coordinator.start_fetch_job")
def start_fetch_job(self, job_id: str, site_id: str, date: str) -> Dict[str, Any]:
    """
    Main coordinator task for starting data fetching jobs
    
    Args:
        job_id: Job ID
        site_id: Site ID
        date: Date
        
    Returns:
        Job execution result
    """
    logger.info(f"Starting fetch job {job_id} for site {site_id}, date {date}")
    
    try:
        # Update job status to processing
        DatabaseService.update_job_status(job_id, JobStatus.PROCESSING)
        DatabaseService.set_job_celery_task_id(job_id, self.request.id)
        
        # Create provider task group
        providers = WorkerConfig.get_provider_urls(site_id)
        if len(providers) == 0:
            raise ValueError(f"No providers found for site {site_id}")

        provider_tasks = []     
        for name, url in providers.items():
            provider_tasks.append(fetch_provider_data.s(name, url, job_id, site_id))

        # provider_tasks = group([
        #     fetch_provider_data.s("site_a", job_id, site_id),
        #     fetch_provider_data.s("site_b", job_id, site_id)
        # ])

        provider_group = group(provider_tasks)
        
        logger.info(f"Created task group for job {job_id} with {len(providers)} provider tasks")

        callback = process_group_results.s()
        c1 = chord(provider_group)(callback)     

        return {
            "job_id": job_id,
            "status": JobStatus.PROCESSING,
            "error": "",
            "stats": {}
        }
        
    except Exception as e:
        error_msg = f"Job coordinator failed for job {job_id}: {str(e)}"
        logger.error(error_msg)
        
        # Update job status to failed
        DatabaseService.update_job_status(job_id, JobStatus.FAILED, error_msg)
        
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "stats": {}
        }

@celery_app.task()
def process_group_results(results):    
     # Initialize statistics
    total_stats = {
        # "site_a": {"fetched": 0, "transformed": 0, "errors": 0},
        # "site_b": {"fetched": 0, "transformed": 0, "errors": 0},
        # "stored": 0
    }
    
    successful_tasks = 0
    failed_tasks = 0
    all_sitedata = []
    errors = []
    job_id = ""
    # Process each provider's results
    for result in results:
        job_id = result.get("job_id", "")
        stats = result.get("stats", {}) 
        provider = result.get("provider")       
        # Add statistics
        total_stats[provider] = stats

        if result.get("success", False):
            successful_tasks += 1            
            sitedata = result.get("sitedata", [])            
            # Collect all sitedata data
            all_sitedata.extend(sitedata)            
            logger.info(f"Provider {provider} fetched successfully with {len(sitedata)} sitedata for job {job_id}")
        else:
            failed_tasks += 1
            error = result.get("error", "Unknown error")
            errors.append(error)
            logger.error(f"Provider {provider} fetching task failed: {error} for job {job_id}")
    
    # Bulk store sitedata data
    total_stats["stored"] = 0
    if all_sitedata:
        stored_count = DatabaseService.bulk_create_sitedata(all_sitedata, job_id)
        total_stats["stored"] = stored_count
        logger.info(f"Provider {provider} stored {stored_count} sitedata for job {job_id}")
    
    # Update job statistics
    DatabaseService.update_job_stats(job_id, total_stats)
    
    # Determine final status
    error_message = None
    if successful_tasks > 0:
        # At least one provider succeeded, mark job as completed
        final_status = JobStatus.FINISHED
        if len(errors) > 0:
             error_message = f"Some providers failed: {'; '.join(errors)}"
             logger.warning(error_message)
        logger.info(f"Job {job_id} completed successfully with {successful_tasks} successful providers")
    else:
        # All providers failed, mark job as failed
        final_status = JobStatus.FAILED
        error_message = f"All providers failed: {'; '.join(errors)}"
        logger.error(f"Job {job_id} failed: {error_message}")
    
    # Update final status
    DatabaseService.update_job_status(job_id, final_status, error_message)