"""
sitedata data model
"""
from sqlalchemy import Column, String, DateTime, Boolean, Float, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
from db.database import Base
from datetime import datetime
from typing import Dict, Any, Optional
import uuid

class sitedata(Base):
    """sitedata data model"""
    __tablename__ = "sitedata"
    
    # Primary key
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    
    # Foreign key to job
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    
    # Basic sitedata information
    sitedata_id = Column(String(100), nullable=False, unique=True, index=True)  # Original sitedata ID
    supplier = Column(String(200), nullable=False, index=True)
    receive_at = Column(DateTime, nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    confirmed = Column(Boolean, nullable=False, default=False, index=True)
    
    # Site information
    site_id = Column(String(100), nullable=False, index=True)
    
    # Data source information
    source = Column(String(50), nullable=False, index=True)  # URL A, URL B
    
    # Score
    sitedata_score = Column(Float, nullable=False, default=0.0, index=True)
    
    # Raw data (for debugging and auditing)
    raw_data = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Create composite indexes
    __table_args__ = (
        Index('idx_job_site_date', 'job_id', 'site_id'),
        Index('idx_site_receive_at', 'site_id', 'receive_at'),
        Index('idx_supplier_status', 'supplier', 'status'),
        Index('idx_score_desc', 'sitedata_score'),
    )
    
    def __repr__(self):
        return f"<sitedata(id={self.id}, sitedata_id={self.sitedata_id}, supplier={self.supplier})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "id": self.sitedata_id,
            "supplier": self.supplier,
            "comfirmedAt": self.receive_at.isoformat() + 'Z' if self.receive_at else None,
            "status": self.status,
            "confirmed": self.confirmed,
            "siteId": self.site_id,
            "source": self.source,
            "sitedataScore": self.sitedata_score
        }
    
    @classmethod
    def create_from_unified_data(
        cls, 
        job_id: str,
        unified_data: Dict[str, Any],
        raw_data: Optional[str] = None
    ) -> "sitedata":
        """Create sitedata record from unified format data"""
        sitedata = cls(
            job_id=job_id,
            sitedata_id=unified_data["id"],
            supplier=unified_data["supplier"],
            receive_at=datetime.fromisoformat(unified_data["comfirmedAt"].replace('Z', '+00:00')),
            status=unified_data["status"],
            confirmed=unified_data["confirmed"],
            site_id=unified_data["siteId"],
            source=unified_data["source"],
            sitedata_score=unified_data["sitedataScore"],
            raw_data=raw_data
        )
        return sitedata
