"""
Job data model
"""
from sqlalchemy import Column, String, DateTime, Text, Integer, JSON
from sqlalchemy.sql import func
from db.database import Base
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

class JobStatus(str, Enum):
    """Job status enumeration"""
    CREATED = "created"
    PROCESSING = "processing"
    FINISHED = "finished"
    FAILED = "failed"

class Job(Base):
    """Job data model"""
    __tablename__ = "jobs"
    
    # Primary key
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    
    # Basic job information
    site_id = Column(String(100), nullable=False, index=True)
    date = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD format
    status = Column(String(20), nullable=False, default=JobStatus.CREATED, index=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Input parameters
    input_params = Column(JSON, nullable=True)
    
    # Statistics
    stats = Column(JSON, nullable=True)
    
    # Error message
    error_message = Column(Text, nullable=True)
    
    # Celery task ID
    celery_task_id = Column(String(36), nullable=True, index=True)
    
    def __repr__(self):
        return f"<Job(id={self.id}, site_id={self.site_id}, status={self.status})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "jobId": self.id,
            "status": self.status,
            "createdAt": self.created_at.isoformat() + 'Z' if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() + 'Z' if self.updated_at else None,
            "input": self.input_params or {},
            "stats": self.stats or {},
            "error": self.error_message
        }
    
    def update_status(self, status: JobStatus, error_message: Optional[str] = None):
        """Update job status"""
        self.status = status
        self.updated_at = datetime.utcnow()
        if error_message:
            self.error_message = error_message
    
    def update_stats(self, stats: Dict[str, Any]):
        """Update statistics"""
        if self.stats is None:
            self.stats = {}
        self.stats = stats
        self.updated_at = datetime.utcnow()
    
    @classmethod
    def create_job(cls, site_id: str, date: str, input_params: Optional[Dict[str, Any]] = None) -> "Job":
        """Create new job"""
        job = cls(
            site_id=site_id,
            date=date,
            input_params=input_params or {"siteId": site_id, "date": date},
            stats={}
        )
        return job