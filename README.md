# JobService Backend - URL Data Aggregation Service

A robust backend service with job-based processing capabilities, which processes and aggregates sitedata data from multiple sitedata providers, transforming it into a unified format with scoring.

## 1. How to Run the Service

### Prerequisites
- Docker and Docker Compose
- Python 3.11+ (for local development)
- MariaDB 11.8.2+
- Redis 3.2.12+

Recommend to setup MariaDB and Redis with Docker Compose for local development


### Quick Start

**1. Clone and setup environment**:
   ```bash
   git clone git@github.com:cheneyzhao/jobservice_backend.git
   cd jobservice-backend
   cp .env.example .env
   ```

**2. Configure environment variables** in `.env`:
   ```bash
   # Database Configuration
   DB_HOST=x.x.x.x
   DB_PORT=xxxx
   DB_USER=xxx
   DB_PASSWORD=xxx
   DB_NAME=xxx
      
   # Backend Service Port Configuration
   HTTP_PORT=8000
   
   # Redis Configuration
   REDIS_HOST=x.x.x.x
   REDIS_PORT=xxxx
   REDIS_DB=x

   # Timeout(seconds, 1-10) for request to the URL's API, default 5
   HTTP_TIMEOUT=x
   # Retry times(1-10) for transient network errors or `5xx` status codes from URL's API, default 3
   HTTP_RETRIES=x
   ```
**3. Configure sites infomation** in `backend/config/sites.json`:

For example:
   ```json
[
  {
    // Site ID
    "site_id": "news",  
    //Date strategy (today, yesterday, custom)
    // today: Default, Fetch today's data
    // yesterday: Fetch (today -1 day)'s data
    // custom: Fetch custom_date's data
    "date_strategy": "today",
    // Enable to fetch this site regularly
    "enabled": true, 
    "providers": {
      "site_a":"http://a.com",
      "site_b":"http://b.com"
    },
    "description": ""
  } 
]
   ```

**4. Build all services**:
   ```bash
   docker-compose build
   ```

**5. Start all services**:
   ```bash
   docker-compose up 
   OR
   docker-compose up -d
   ```

**6. Verify services are running with corresponding http port**:

For example:
   - Backend API: http://localhost:8000/backend/docs

### Alternative: Local Development

```bash
# Install dependencies
cd backend
pip install -r requirements.txt

# Start backend server
./start_backend_local.sh

# Start Celery worker (in separate terminal)
./start_worker_local.sh
```

## 2. Architecture Overview

### Key Architectural Patterns And Layers

- **Asynchronous Task Processing**: Celery-based job queue for handling long-running operations
- **Job Coordinator Pattern**: Fetch external provider data concurrently and aggregate with main coordinator and worker tasks
- **Data Transformation Layer**: Unified data format with provider-specific transformers
- **Caching Layer**: Cache with TTL for improved performance
- **Scheduler Layer**: Trigger the data fetching process automatically by the hour for a predefined sites
- **Fault Tolerance**: Handle provider failures with partial success support

## 3. Potential Service Evolution
1. **Security & Compliance**
   - OAuth2/JWT authentication
   - Role-based access control (RBAC)
   - Data encryption in transit
   - Secure Dockerfile
2. **Integration Ecosystem**
   - Plugin architecture for new providers
3. **Operation and Monitoring**
   - Improve logger, job and cache monitoring
   - GUI with mangement of Cache, Workers and System Health check
