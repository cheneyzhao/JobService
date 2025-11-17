import os
import logging

import httpx
from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.v1 import api_router
from db.database import create_tables
from workers.services.scheduler_service import scheduler_service
from middleware.log_middleware import setup_log_middleware
from log.backend_logger import get_logger

logger = get_logger(__name__)


A_URL = os.getenv("A_URL", "http://a.com")
B_URL = os.getenv("B_URL", "http://b.com")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database tables
    try:
        create_tables()
        logger.info("Database tables initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database tables: {e}")
    
    # Start scheduler service
    try:
        scheduler_service.start()
        logger.info("Scheduler service started successfully")
    except Exception as e:
        logger.error(f"Failed to start scheduler service: {e}")
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        # Check URL A
        try:
            r = await client.post(A_URL)
            if r.status_code == 200:
                logger.info(f"URL A reachable at {A_URL}")
            else:
                logger.error(f"URL A returned status {r.status_code} at {A_URL}")
        except Exception as e:
            logger.error(f"Failed to reach URL A at {A_URL}: {e}")

        # Check URL B
        try:
            r = await client.post(B_URL)
            if r.status_code == 200:
                logger.info(f"URL B reachable at {B_URL}")
            else:
                logger.error(f"URL B returned status {r.status_code} at {B_URL}")
        except Exception as e:
            logger.error(f"Failed to reach URL B at {B_URL}: {e}")

    yield

    # Stop scheduler service
    try:
        scheduler_service.stop()
        logger.info("Scheduler service stopped successfully")
    except Exception as e:
        logger.error(f"Failed to stop scheduler service: {e}")

    logger.info("Application shutdown complete.")


app = FastAPI(
    title="JobService Backend Service", 
    description="sitedata Data Aggregation Service",
    version="1.0.0",
    lifespan=lifespan, 
    root_path="/backend"
)

# Setup log middleware
setup_log_middleware(app, "access_info.log")

app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def root():
    return {
        "message": "sitedata Data Aggregation Service",
        "version": "1.0.0",
        "features": [
            "Asynchronous Task Management",
            "Multi-Worker Concurrent Processing", 
            "Data Fetching and Transformation",
            "Fault Tolerance Mechanism",
            "Data Storage",
            "Status Monitoring"
        ]
    }
