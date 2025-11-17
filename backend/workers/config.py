"""
Workers configuration
"""
import os
import json
import logging
from typing import Dict, Any, List
from log.backend_logger import get_logger

logger = get_logger(__name__)

class WorkerConfig:
    """Worker configuration class"""
    
    # Redis configuration
    REDIS_HOST = os.getenv("REDIS_HOST", "0.0.0.0")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB_FOR_WORKER = int(os.getenv("REDIS_DB_FOR_WORKER", "0"))
    REDIS_URL_FOR_WORKER = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_FOR_WORKER}"

    REDIS_DB_FOR_CACHE = int(os.getenv("REDIS_DB_FOR_CACHE", "1"))
    REDIS_URL_FOR_CACHE = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_FOR_CACHE}"
    
    # MariaDB configuration
    DB_HOST = os.getenv("DB_HOST", "0.0.0.0")
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
    DB_NAME = os.getenv("DB_NAME", "jobservice")
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"    
   
    # HTTP client configuration
    HTTP_TIMEOUT = max(1, min(10, int(os.getenv("HTTP_TIMEOUT", "5"))))  # 1-10 seconds timeout
    HTTP_RETRIES = max(1, min(10, int(os.getenv("HTTP_RETRIES", "3"))))   # 1-10 retries    
   
    # Log configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def get_sites_config(cls) -> List[Dict[str, Any]]:
        """
        Load predefined sites configurations
        
        Returns:
            List of sites configurations
        """
        # Default configurations
        default_configs = [
              {
                "site_id": "news",
                "date_strategy": "today", # today, yesterday, custom
                "enabled": True,
                "providers": {
                                "site_a":"http://a.com",
                                "site_b":"http://b.com"
                             },
                "description": "fetch today's data every hour"
             }
         ]
        
        # Try to load from configuration file
        config_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'sites.json')
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    configs = json.load(f)
                    logger.info(f"Loaded {len(configs)} site configurations from file")
                    return configs
        except Exception as e:
            logger.warning(f"Failed to load site configurations from file: {e}")
        
        logger.info(f"Using default site configurations: {len(default_configs)} items")
        return default_configs

    @classmethod
    def get_provider_urls(cls, site_id:str) -> Dict[str, str]:
        enabled_configs = [config for config in cls.get_sites_config() if config.get("enabled", True)]
        
        for i, config in enumerate(enabled_configs):
            if site_id == config["site_id"]:
                return config["providers"]
        logger.error(f"Unknown site: {site_id}")
        return []