"""
Data fetching task
Responsible for fetching and processing data from a single provider
"""
import asyncio
from typing import Dict, Any, List, Optional
import logging
import json
from workers.celery_app import celery_app
from workers.services.data_fetcher import DataFetcher
from workers.services.data_transformer import DataTransformer
from log.backend_logger import get_logger

logger = get_logger(__name__)

@celery_app.task(bind=True, name="backend.workers.tasks.data_fetcher.fetch_provider_data")
def fetch_provider_data(self, provider: str, url: str, job_id: str, site_id: str) -> Dict[str, Any]:
    """
    Worker task for fetching and processing data from specified provider
    
    Args:
        provider: Provider name ('site_a' or 'site_b')
        job_id: Job ID
        site_id: Site ID
        
    Returns:
        Processing result
    """
    logger.info(f"Starting data fetch for provider {provider} from url {url}, job {job_id}, site {site_id}")
    
    try:
        # Create data fetcher
        fetcher = DataFetcher()
        
        # Asynchronously fetch data
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            success, raw_data, error = loop.run_until_complete(
                fetcher.fetch_provider_data(provider, url)
            )
        finally:
            loop.close()
        
        if not success:
            logger.error(f"Failed to fetch data from {provider}: {error}")
            return {
                "success": False,
                "provider": provider,
                "job_id": job_id,
                "error": error,
                "stats": {"fetched": 0, "transformed": 0, "errors": 1},
                "sitedata": []
            }
        
        logger.info(f"Successfully fetched {len(raw_data)} items from {provider}")
        
        # Transform data
        transformed_data = transform_provider_data(provider, raw_data, site_id)
        
        # Generate statistics
        stats = {
            "fetched": len(raw_data) if raw_data else 0,
            "transformed": len(transformed_data),
            "errors": (len(raw_data) - len(transformed_data)) if raw_data else 0
        }
        
        logger.info(f"Provider {provider} processing completed: {stats}")
        
        return {
            "success": True,
            "provider": provider,
            "job_id": job_id,
            "sitedata": transformed_data,
            "stats": stats,
            "error": None
        }
        
    except Exception as e:
        error_msg = f"Unexpected error in provider {provider} task: {str(e)}"
        logger.error(error_msg)
        
        return {
            "success": False,
            "provider": provider,
            "job_id": job_id,
            "error": error_msg,
            "stats": {"fetched": 0, "transformed": 0, "errors": 1},
            "sitedata": []
        }

def transform_provider_data(provider: str, raw_data: List[Dict[str, Any]], site_id: str) -> List[Dict[str, Any]]:
    """
    Transform provider data to unified format
    
    Args:
        provider: Provider name
        raw_data: Raw data
        site_id: Site ID
        
    Returns:
        Transformed data in unified format
    """
    try:
        if provider == "site_a":
            return DataTransformer.transform_site_a_data(raw_data, site_id)
        elif provider == "site_b":
            return DataTransformer.transform_site_b_data(raw_data, site_id)
        else:
            logger.error(f"Unknown provider: {provider}")
            return []
            
    except Exception as e:
        logger.error(f"Error transforming data for provider {provider}: {e}")
        return []