"""
Data fetching service
Responsible for fetching data from different provider APIs
"""
import httpx
import asyncio
import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from workers.config import WorkerConfig
from log.backend_logger import get_logger

logger = get_logger(__name__)

class DataFetcher:
    """Data fetcher"""
    
    def __init__(self):
        self.timeout = httpx.Timeout(WorkerConfig.HTTP_TIMEOUT)
        self.retries = WorkerConfig.HTTP_RETRIES
    
    async def fetch_provider_data(self, provider: str, url: str) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Fetch data from specified provider
        
        Args:
            provider: provider name
            url: provider API URL
            
        Returns:
            Tuple[success, data, error_message]
        """         
        for attempt in range(self.retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    logger.info(f"Fetching data from {provider}'s url {url} (attempt {attempt + 1}/{self.retries})")                    

                    response = await client.post(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, list):                            
                            return True, data, None
                        else:
                            error_msg = f"Expected list response from {provider}, got {type(data)}"
                            logger.error(error_msg)
                            return False, None, error_msg
                    else:
                        error_msg = f"HTTP {response.status_code} from {provider}'s url {url} {response.text}"
                        logger.warning(error_msg)
                        
                        # Don't retry for 4xx errors
                        if 400 <= response.status_code < 500:
                            return False, None, error_msg
                        
                        # Continue retrying for 5xx errors
                        if attempt < self.retries:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            return False, None, error_msg
                            
            except httpx.TimeoutException:
                error_msg = f"Timeout fetching data from {provider}"
                logger.warning(error_msg)
                if attempt < self.retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return False, None, error_msg
                    
            except httpx.ConnectError:
                error_msg = f"Connection error to {provider}"
                logger.warning(error_msg)
                if attempt < self.retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return False, None, error_msg
                    
            except Exception as e:
                error_msg = f"Unexpected error fetching from {provider}: {str(e)}"
                logger.error(error_msg)
                if attempt < self.retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return False, None, error_msg
        
        return False, None, f"Failed to fetch data from {provider} after {self.retries} attempts"