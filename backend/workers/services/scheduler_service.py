"""
Cron Job Service
Responsible for executing data fetch tasks on schedule
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from workers.utils.task_manager import task_manager
from workers.config import WorkerConfig
from log.backend_logger import get_logger
import json
import os


logger = get_logger(__name__)

class SchedulerService:
    """Scheduled task service class (Singleton)"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize scheduler (only once)"""
        if self._initialized:
            return
            
        # Configure scheduler
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(max_workers=5)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3,
            'misfire_grace_time': 300  # 5 minutes grace time
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        # Predefined site and date configurations
        self.scheduled_configs = WorkerConfig.get_sites_config()
        
        # Scheduler status
        self.is_running = False
        
        # Mark as initialized
        self._initialized = True    
   
    def _get_date_for_strategy(self, strategy: str, custom_date: Optional[str] = None) -> str:
        """
        Get date based on date strategy
        
        Args:
            strategy: Date strategy (today, yesterday, custom)
            custom_date: Custom date (YYYY-MM-DD)
            
        Returns:
            Date string (YYYY-MM-DD)
        """
        now = datetime.now()
        
        if strategy == "today":
            return now.strftime("%Y-%m-%d")
        elif strategy == "yesterday":
            yesterday = now - timedelta(days=1)
            return yesterday.strftime("%Y-%m-%d")
        elif strategy == "custom" and custom_date:
            return custom_date
        else:
            # Default to today
            return now.strftime("%Y-%m-%d")
    
    def _execute_scheduled_job(self, config: Dict[str, Any]):
        """
        Execute scheduled fetch task
        
        Args:
            config: Task configuration
        """
        try:
            site_id = config["site_id"]
            date_strategy = config.get("date_strategy", "today")
            custom_date = config.get("custom_date")
            
            # Get target date
            target_date = self._get_date_for_strategy(date_strategy, custom_date)
            
            logger.info(f"Executing scheduled job for site {site_id}, date {target_date}")
            
            # Call task manager to create fetch job
            result = task_manager.create_fetch_job(site_id, target_date)
            
            if "error" in result:
                logger.error(f"Scheduled job failed for site {site_id}: {result['error']}")
            else:
                logger.info(f"Scheduled job created successfully for site {site_id}: {result.get('jobId')}")
                
        except Exception as e:
            logger.error(f"Error executing scheduled job for config {config}: {e}")
    
    def start(self):
        """Start scheduler"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        try:
            # Add scheduled jobs
            self._add_scheduled_jobs()
            
            # Start scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("Scheduler started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    def stop(self):
        """Stop scheduler"""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return
        
        try:
            self.scheduler.shutdown(wait=True)
            self.is_running = False
            logger.info("Scheduler stopped successfully")
            
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            raise
    
    def _add_scheduled_jobs(self):
        """Add predefined scheduled jobs"""
        enabled_configs = [config for config in self.scheduled_configs if config.get("enabled", True)]
        
        logger.info(f"Adding {len(enabled_configs)} scheduled jobs")
        
        for i, config in enumerate(enabled_configs):
            job_id = f"fetch_job_{config['site_id']}_{i}"
            
            # Execute every hour
            self.scheduler.add_job(
                func=self._execute_scheduled_job,
                trigger=CronTrigger(minute=0),  # Execute at 0 minute of every hour
                args=[config],
                id=job_id,
                name=f"Fetch job for {config['site_id']}",
                replace_existing=True
            )
            
            logger.info(f"Added scheduled job: {job_id} - {config.get('description', 'No description')}")    
   
    def remove_job(self, job_id: str):
        """
        Remove scheduled job
        
        Args:
            job_id: Job ID
        """
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed scheduled job: {job_id}")
            return {"status": "success"}
            
        except Exception as e:
            logger.error(f"Failed to remove job {job_id}: {e}")
            return {"error": str(e), "status": "failed"}
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all scheduled job information
        
        Returns:
            List of job information
        """
        try:
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger)
                })
            
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs: {e}")
            return []
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get scheduler status
        
        Returns:
            Status information
        """
        return {
            "is_running": self.is_running,
            "total_jobs": len(self.scheduler.get_jobs()) if self.is_running else 0,
            "enabled_configs": len([c for c in self.scheduled_configs if c.get("enabled", True)]),
            "total_configs": len(self.scheduled_configs)
        }

# Global scheduler service instance (Singleton)
scheduler_service = SchedulerService()