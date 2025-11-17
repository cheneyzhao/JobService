"""
Backend Logger Utility
Provides logging functionality for other classes to use via get_logger method
"""
import logging
import sys
from typing import Any, Optional


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get a configured logger instance for other classes to use
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if this logger doesn't have handlers yet
    if not logger.handlers:
        logger.setLevel(level)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        logger.propagate = False
    
    return logger


def set_log_level(logger: logging.Logger, level: int) -> None:
    """Set logging level for a logger instance"""
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


def get_level_name(logger: logging.Logger) -> str:
    """Get current logging level name"""
    return logging.getLevelName(logger.level)


