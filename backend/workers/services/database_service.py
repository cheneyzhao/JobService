"""
Database service
Responsible for data storage and query operations
"""
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc
import logging
import json
from db.database import get_db_session
from db.models.job import Job, JobStatus
from db.models.sitedata import sitedata
from cache.cache_service import cache_service
from log.backend_logger import get_logger

logger = get_logger(__name__)

class DatabaseService:
    """Database service class"""
    
    @staticmethod
    def _invalidate_sitedata_caches(job_id: Optional[str] = None):
        """
        Clear sitedata-related caches
        
        Args:
            job_id: Job ID, if provided only clear caches related to this job (TODO)
        """
        try:
            # Since new data may affect all query results, we clear all related caches
            cache_service.clear()
            logger.warning(f"Invalidated sitedata caches for job: {job_id or 'all'}")
        except Exception as e:
            logger.error(f"Error invalidating sitedata caches: {e}")
    
    @staticmethod
    def create_job(site_id: str, date: str, input_params: Optional[Dict[str, Any]] = None) -> str:
        """Create new job"""
        with get_db_session() as db:
            job = Job.create_job(site_id, date, input_params)
            db.add(job)
            db.flush()
            job_id = job.id
            logger.debug(f"Created job {job_id} for site {site_id}, date {date}")
            return job_id
    
    @staticmethod
    def get_job_by_id(job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID"""
        with get_db_session() as db:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                # Get all needed attributes within session
                return {
                    "id": job.id,
                    "site_id": job.site_id,
                    "date": job.date,
                    "status": job.status,
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                    "input_params": job.input_params,
                    "stats": job.stats,
                    "error_message": job.error_message,
                    "celery_task_id": job.celery_task_id
                }
            return None
    
    @staticmethod
    def update_job_status(job_id: str, status: JobStatus, error_message: Optional[str] = None) -> bool:
        """Update job status"""
        try:
            with get_db_session() as db:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.update_status(status, error_message)                                      
                    logger.debug(f"Updated job {job_id} status to {status}")
                    return True
                else:
                    logger.warning(f"Job {job_id} not found for status update")
                    return False
        except Exception as e:
            logger.error(f"Error updating job {job_id} status: {e}")
            return False
    
    @staticmethod
    def update_job_stats(job_id: str, stats: Dict[str, Any]) -> bool:
        """Update job statistics"""
        try:
            with get_db_session() as db:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.update_stats(stats)                    
                    logger.debug(f"Updated job {job_id} stats: {stats}")
                    return True
                else:
                    logger.warning(f"Job {job_id} not found for stats update")
                    return False
        except Exception as e:
            logger.error(f"Error updating job {job_id} stats: {e}")
            return False
    
    @staticmethod
    def set_job_celery_task_id(job_id: str, celery_task_id: str) -> bool:
        """Set job's Celery task ID"""
        try:
            with get_db_session() as db:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.celery_task_id = celery_task_id                    
                    logger.debug(f"Set job {job_id} celery task ID to {celery_task_id}")
                    return True
                else:
                    logger.warning(f"Job {job_id} not found for celery task ID update")
                    return False
        except Exception as e:
            logger.error(f"Error setting job {job_id} celery task ID: {e}")
            return False
    
    @staticmethod
    def find_existing_job(site_id: str, date: str, statuses: List[JobStatus]) -> Optional[Dict[str, Any]]:
        """Find existing job"""
        with get_db_session() as db:
            job = db.query(Job).filter(
                and_(
                    Job.site_id == site_id,
                    Job.date == date,
                    Job.status.in_(statuses)
                )
            ).first()
            if job:
                # Get all needed attributes within session
                return {
                    "id": job.id,
                    "site_id": job.site_id,
                    "date": job.date,
                    "status": job.status,
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                    "input_params": job.input_params,
                    "stats": job.stats,
                    "error_message": job.error_message,
                    "celery_task_id": job.celery_task_id
                }
            return None
    
    @staticmethod
    def bulk_create_sitedata(sitedata_data: List[Dict[str, Any]], job_id: str) -> int:
        """Bulk create sitedata records"""
        try:
            with get_db_session() as db:
                sitedata = []
                for data in sitedata_data:
                    try:
                        sitedata = sitedata.create_from_unified_data(
                            job_id=job_id,
                            unified_data=data,
                            raw_data=json.dumps(data)
                        )
                        sitedata.append(sitedata)
                    except Exception as e:
                        logger.error(f"Error creating sitedata from data {data}: {e}")
                        continue
                
                if sitedata:
                    db.bulk_save_objects(sitedata)
                    logger.debug(f"Bulk created {len(sitedata)} sitedata for job {job_id}")
                    
                    # Clear related caches
                    DatabaseService._invalidate_sitedata_caches(job_id)
                    
                    return len(sitedata)
                else:
                    logger.warning(f"No valid sitedata to create for job {job_id}")
                    return 0
                    
        except Exception as e:
            logger.error(f"Error bulk creating sitedata for job {job_id}: {type(e)}")
            return 0
    
    @staticmethod
    def get_job_sitedata(
        job_id: str,
        limit: int = 50,
        offset: int = 0,
        supplier: Optional[str] = None,
        status: Optional[str] = None,
        confirmed: Optional[bool] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        site_id: Optional[str] = None,
        sort_by: str = "sitedataScore desc"
    ) -> Dict[str, Any]:
        """Get job's sitedata records (with cache)"""
        # Try to get from cache
        cached_result = cache_service.get(
            "job_sitedata",
            job_id=job_id,
            limit=limit,
            offset=offset,
            supplier=supplier,
            status=status,
            confirmed=confirmed,
            from_date=from_date,
            to_date=to_date,
            site_id=site_id,
            sort_by=sort_by
        )
        
        if cached_result is not None:
            logger.debug(f"Cache hit for job sitedata: {job_id}")
            return cached_result
        
        # Cache miss, query from database
        with get_db_session() as db:
            query = db.query(sitedata).filter(sitedata.job_id == job_id)
            
            # Apply filter conditions
            if supplier:
                query = query.filter(sitedata.supplier.ilike(f"%{supplier}%"))
            if status:
                query = query.filter(sitedata.status == status)
            if confirmed is not None:
                query = query.filter(sitedata.confirmed == confirmed)
            if site_id:
                query = query.filter(sitedata.site_id == site_id)
            if from_date:
                query = query.filter(sitedata.receive_at >= from_date)
            if to_date:
                query = query.filter(sitedata.receive_at <= to_date)
            
            # Apply sorting
            if sort_by == "sitedataScore desc":
                query = query.order_by(desc(sitedata.sitedata_score))
            elif sort_by == "sitedataScore asc":
                query = query.order_by(asc(sitedata.sitedata_score))
            elif sort_by == "comfirmedAt desc":
                query = query.order_by(desc(sitedata.receive_at))
            elif sort_by == "comfirmedAt asc":
                query = query.order_by(asc(sitedata.receive_at))
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            sitedata = query.offset(offset).limit(limit).all()
            
            result = {
                "jobId": job_id,
                "items": [sitedata.to_dict() for sitedata in sitedata],
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
            # Cache result (TTL: 5 minutes)
            cache_service.set(
                "job_sitedata",
                result,
                ttl=300,
                job_id=job_id,
                limit=limit,
                offset=offset,
                supplier=supplier,
                status=status,
                confirmed=confirmed,
                from_date=from_date,
                to_date=to_date,
                site_id=site_id,
                sort_by=sort_by
            )
            
            logger.debug(f"Cached job sitedata result: {job_id}")
            return result
    
    @staticmethod
    def get_all_sitedata(
        limit: int = 50,
        offset: int = 0,
        supplier: Optional[str] = None,
        status: Optional[str] = None,
        confirmed: Optional[bool] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        site_id: Optional[str] = None,
        sort_by: str = "sitedataScore desc"
    ) -> Dict[str, Any]:
        """Get all sitedata records (with cache)"""
        # Try to get from cache
        cached_result = cache_service.get(
            "all_sitedata",
            limit=limit,
            offset=offset,
            supplier=supplier,
            status=status,
            confirmed=confirmed,
            from_date=from_date,
            to_date=to_date,
            site_id=site_id,
            sort_by=sort_by
        )
        
        if cached_result is not None:
            logger.debug("Cache hit for all sitedata")
            return cached_result
        
        # Cache miss, query from database
        with get_db_session() as db:
            query = db.query(sitedata)
            
            # Apply filter conditions
            if supplier:
                query = query.filter(sitedata.supplier.ilike(f"%{supplier}%"))
            if status:
                query = query.filter(sitedata.status == status)
            if confirmed is not None:
                query = query.filter(sitedata.confirmed == confirmed)
            if site_id:
                query = query.filter(sitedata.site_id == site_id)
            if from_date:
                query = query.filter(sitedata.receive_at >= from_date)
            if to_date:
                query = query.filter(sitedata.receive_at <= to_date)
            
            # Apply sorting
            if sort_by == "sitedataScore desc":
                query = query.order_by(desc(sitedata.sitedata_score))
            elif sort_by == "sitedataScore asc":
                query = query.order_by(asc(sitedata.sitedata_score))
            elif sort_by == "comfirmedAt desc":
                query = query.order_by(desc(sitedata.receive_at))
            elif sort_by == "comfirmedAt asc":
                query = query.order_by(asc(sitedata.receive_at))
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            sitedata = query.offset(offset).limit(limit).all()
            
            result = {
                "items": [sitedata.to_dict() for sitedata in sitedata],
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
            # Cache result (TTL: 5 minutes)
            cache_service.set(
                "all_sitedata",
                result,
                ttl=300,
                limit=limit,
                offset=offset,
                supplier=supplier,
                status=status,
                confirmed=confirmed,
                from_date=from_date,
                to_date=to_date,
                site_id=site_id,
                sort_by=sort_by
            )
            
            logger.debug("Cached all sitedata result")
            return result