"""
Redis cache service
Provides Redis-based caching functionality with TTL support
"""
import time
import hashlib
import json
import logging
from typing import Any, Dict, Optional
import redis
from log.backend_logger import get_logger

from workers.config import WorkerConfig

class CacheService:
    """Redis cache service class (Singleton)"""
    
    _instance = None
    _initialized = False
    logger = get_logger(__name__)
    
    def __new__(cls, max_size: int = 1000, default_ttl: int = 300):
        if cls._instance is None:
            cls._instance = super(CacheService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        """
        Initialize Redis cache service (only once)
        
        Args:
            max_size: Maximum number of cache entries (Redis handles this automatically)
            default_ttl: Default TTL (seconds)
        """
        if self._initialized:
            return
            
        self.max_size = max_size
        self.default_ttl = default_ttl
        
        # Initialize Redis connection
        try:
            self.redis_client = redis.Redis.from_url(
                WorkerConfig.REDIS_URL_FOR_CACHE,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_client.ping()
            self.logger.info(f"Redis cache service initialized successfully: {WorkerConfig.REDIS_URL_FOR_CACHE}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis cache: {e}")
            raise
        
        # Mark as initialized
        self._initialized = True
      
    def _generate_key(self, prefix: str, **kwargs) -> str:
        """
        Generate cache key
        
        Args:
            prefix: Key prefix
            **kwargs: Parameter dictionary
            
        Returns:
            Cache key
        """
        # Create stable string representation of parameters
        params_str = json.dumps(kwargs, sort_keys=True, default=str)
        key_data = f"{prefix}:{params_str}"
        key_hash = hashlib.md5(key_data.encode()).hexdigest()
        # Use MD5 hash to shorten key length
        return key_hash
    
    def get(self, prefix: str, **kwargs) -> Optional[Any]:
        """
        Get cache value from Redis
        
        Args:
            prefix: Key prefix
            **kwargs: Parameter dictionary
            
        Returns:
            Cache value or None
        """
        key = self._generate_key(prefix, **kwargs)
        
        try:
            cached_value = self.redis_client.get(key)
            if cached_value is None:
                self.logger.info(f"Cache miss: {key}")
                return None
            
            # Redis handles TTL automatically, so we don't need to check expiration
            value = json.loads(cached_value)
            self.logger.info(f"Cache hit: {key}")
            return value
        except Exception as e:
            self.logger.error(f"Error getting cache value for key {key}: {e}")
            return None
    
    def set(self, prefix: str, value: Any, ttl: Optional[int] = None, **kwargs):
        """
        Set cache value in Redis
        
        Args:
            prefix: Key prefix
            value: Cache value
            ttl: Time to live (seconds), None uses default value
            **kwargs: Parameter dictionary
        """
        key = self._generate_key(prefix, **kwargs)
        ttl_seconds = ttl or self.default_ttl
        
        try:
            # Redis handles expiration automatically
            serialized_value = json.dumps(value)
            self.redis_client.setex(key, ttl_seconds, serialized_value)
            self.logger.info(f"Cache set: {key}, TTL: {ttl_seconds}s")
        except Exception as e:
            self.logger.error(f"Error setting cache value for key {key}: {e}")
    
    def delete(self, prefix: str, **kwargs) -> bool:
        """
        Delete cache value from Redis
        
        Args:
            prefix: Key prefix
            **kwargs: Parameter dictionary
            
        Returns:
            Whether successfully deleted
        """
        key = self._generate_key(prefix, **kwargs)
        
        try:
            deleted_count = self.redis_client.delete(key)
            if deleted_count > 0:
                self.logger.info(f"Cache deleted: {key}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error deleting cache value for key {key}: {e}")
            return False
    
    def clear(self):
        """Clear all cache from Redis"""
        try:
            # Clear all keys (CAUTION to applications that use the same Redis DB)
            self.redis_client.flushdb()
            self.logger.info("Redis cache cleared")
        except Exception as e:
            self.logger.error(f"Error clearing Redis cache: {e}")
                      
    def get_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        try:
            # Get Redis info
            info = self.redis_client.info()
            
            return {
                "redis_version": info.get('redis_version'),
                "connected_clients": info.get('connected_clients'),
                "used_memory_human": info.get('used_memory_human'),
                "used_memory_peak_human": info.get('used_memory_peak_human'),
                "keyspace_hits": info.get('keyspace_hits'),
                "keyspace_misses": info.get('keyspace_misses'),
                "max_size": self.max_size,
                "default_ttl": self.default_ttl
            }
        except Exception as e:
            self.logger.error(f"Error getting Redis cache stats: {e}")
            return {
                "error": str(e),
                "max_size": self.max_size,
                "default_ttl": self.default_ttl
            }

# Global cache service instance (Singleton)
cache_service = CacheService(max_size=1000, default_ttl=300)  # 5 minutes TTL