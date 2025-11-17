"""
Log middleware for FastAPI application
Tracks every API call and its processing time
"""
import time
import logging
from logging.handlers import RotatingFileHandler
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class LogMiddleware(BaseHTTPMiddleware):
    """Middleware to log API requests and response times"""
    
    def __init__(self, app, log_file: str = "access_info.log", max_file_size: int = 5*1024*1024, backup_count: int = 3):
        super().__init__(app)
        self.log_file = log_file
        self.max_file_size = max_file_size  # 5MB default
        self.backup_count = backup_count    # Keep 3 backup files
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup logger for access logs"""
        self.logger = logging.getLogger("access_logger")
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Create rotating file handler with size limit
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
    
    async def dispatch(self, request: Request, call_next):
        """Process request and log access information"""
        start_time = time.time()
        
        # Process request
        response = await call_next(request)
        
        # Calculate processing time
        process_time = time.time() - start_time
        
        # Log access information
        self._log_access(request, response, process_time)
        
        return response
    
    def _log_access(self, request: Request, response: Response, process_time: float):
        """Log access information to file"""
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        log_message = (
            f"IP: {client_ip} - "
            f"Method: {request.method} - "
            f"URL: {request.url} - "
            f"Status: {response.status_code} - "
            f"Process Time: {process_time:.4f}s - "
            f"User Agent: {user_agent}"
        )
        
        self.logger.info(log_message)


def setup_log_middleware(app, log_file: str = "access_info.log", max_file_size: int = 5*1024*1024, backup_count: int = 3):
    """Setup log middleware for FastAPI application"""
    app.add_middleware(LogMiddleware, log_file=log_file, max_file_size=max_file_size, backup_count=backup_count)