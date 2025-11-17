"""
Celery Worker startup script
"""
import os
import sys
import logging
from workers.celery_app import celery_app
from db.database import create_tables
from log.backend_logger import get_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = get_logger(__name__)

def setup_database():
    """Initialize database tables"""
    try:
        create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        sys.exit(1)

def start_worker():
    """Start Celery Worker"""
    logger.info("Starting JobService Celery Worker...")
    
    # Initialize database
    setup_database()
    
    # Start worker
    celery_app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',
        '--queues=default,job_queue,data_queue',
        '--hostname=jobservice-worker@%h'
    ])

if __name__ == "__main__":
    start_worker()