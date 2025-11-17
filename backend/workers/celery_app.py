"""
Celery application configuration
"""
import os
from celery import Celery
from kombu import Queue
from workers.config import WorkerConfig

# Create Celery application instance
celery_app = Celery(
    "jobservice_workers",
    broker=WorkerConfig.REDIS_URL_FOR_WORKER,
    backend=WorkerConfig.REDIS_URL_FOR_WORKER,
    include=[
        "workers.tasks.data_fetcher",
        "workers.tasks.job_coordinator"
    ]
)

# Celery configuration
celery_app.conf.update(
    # Task serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    
    # Task routing configuration
    task_routes={
        "backend.workers.tasks.job_coordinator.start_fetch_job": {"queue": "job_queue"},
        "backend.workers.tasks.data_fetcher.fetch_provider_data": {"queue": "data_queue"},
    },
    
    # Queue configuration
    task_default_queue="default",
    task_queues=(
        Queue("default", routing_key="default"),
        Queue("job_queue", routing_key="job_queue"),
        Queue("data_queue", routing_key="data_queue"),
    ),
    
    # Task execution configuration
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    
    # Result configuration
    result_expires=3600,  # Results saved for 1 hour
    result_backend_transport_options={
        "master_name": "mymaster",
        "visibility_timeout": 3600,
    },
    
    # Retry configuration
    task_default_retry_delay=60,  # Default retry delay 60 seconds
    task_max_retries=3,  # Maximum 3 retries
    
    # Worker configuration
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
)

# Auto-discover tasks
celery_app.autodiscover_tasks([
    "workers.tasks"
])

if __name__ == "__main__":
    celery_app.start()