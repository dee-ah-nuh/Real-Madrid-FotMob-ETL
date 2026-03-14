# Agent Handoff — Next Session

## Context
This file is for Claude Code to pick up exactly where the previous session ended.
Read this first, then check `README.md` for full architecture context.

---

## Where We Left Off

### What Was Completed This Session
- Fixed all 9 dbt models (6 staging + 3 marts) — **43/43 tests passing, 0 errors**
- All 4 Real Madrid seasons (2021/22–2024/25) loading from S3 into DuckDB
- Airflow is running in Docker (`real-madrid-fotmob-etl-*` containers on port 8080)
- DAG `fotmob-etl-extract_teams` triggered but **FAILED** — see issue below

### Immediate Priority: Fix IP Rotator

**File:** `airflow/extract/fotmob_client.py`

**Error from Airflow logs:**
```
INFO  - Starting API gateway in 1 regions.
INFO  - Could not create region (some regions require manual enabling): us-east-2
INFO  - Using 0 endpoints with name 'https://www.fotmob.com - IP Rotate API' (0 new).
IndexError: Cannot choose from an empty sequence
  File "requests_ip_rotator/ip_rotator.py", line 56 in send
  File "random.py", line 347 in choice
```

**Root cause:** `requests-ip-rotator` creates AWS API Gateway endpoints to rotate IPs.
The IAM user (`diana_valladares`) doesn't have API Gateway creation permissions in
`us-east-2`, so 0 endpoints are created. When a request is made, it tries to pick a
random endpoint from an empty list → crashes.

**Fix to implement:**
1. In `start_session()`, after `gateway.start()`, check if any endpoints were created.
   If 0 endpoints, log a warning and fall back to a plain `requests.Session()` (no IP rotation).
2. In `close()`, wrap `gateway.shutdown()` in a try/except to handle the case where
   gateway is None or has 0 endpoints.
3. Try `us-east-1` as the default region — it is enabled by default in most AWS accounts.
   Change `self.regions = regions or ["us-east-2"]` to `["us-east-1", "us-east-2"]`.

**The fix in `fotmob_client.py` should look roughly like:**
```python
def start_session(self):
    logger.info("Starting IP rotator gateway...")
    try:
        self.gateway = ApiGateway(self.BASE_URL, regions=self.regions)
        self.gateway.start()
        if not getattr(self.gateway, 'endpoints', None):
            logger.warning("No API Gateway endpoints created. Falling back to direct requests.")
            self.gateway = None
    except Exception as e:
        logger.warning(f"IP rotator setup failed: {e}. Falling back to direct requests.")
        self.gateway = None

    self.session = requests.Session()
    if self.gateway:
        self.session.mount(self.BASE_URL, self.gateway)
        logger.info("Session ready with IP rotation.")
    else:
        logger.info("Session ready (direct requests, no IP rotation).")

def close(self):
    if self.gateway:
        try:
            logger.info("Shutting down gateway...")
            self.gateway.shutdown()
            logger.info("Gateway closed.")
        except Exception as e:
            logger.warning(f"Error shutting down gateway: {e}")
```

**After fixing:** Re-trigger the DAG in Airflow UI (http://localhost:8080)
to verify all 5 season tasks run successfully and upload to S3.

---

## Airflow Status

**Running containers** (from previous session, should still be up):
```
real-madrid-fotmob-etl-airflow-apiserver-1    → http://localhost:8080
real-madrid-fotmob-etl-airflow-scheduler-1
real-madrid-fotmob-etl-airflow-dag-processor-1
real-madrid-fotmob-etl-airflow-triggerer-1
real-madrid-fotmob-etl-postgres-1             → port 5432
```

**If containers are down**, start them:
```bash
cd /path/to/repo
set -a && source .env && set +a
docker-compose -f .dev_container/docker-compose.yaml up -d
```

**DAG:** `fotmob-etl-extract_teams`
**Tasks:** `extract_real_madrid_2021_2022` through `extract_real_madrid_2025_2026`

---

## After IP Rotator Is Fixed

Once extraction is running, the next priorities are:

### 1. Run dbt After Extraction
```bash
cd airflow/transform
set -a && source ../../.env && set +a
/Users/deeahnuh/Library/Python/3.9/bin/dbt build --profiles-dir .
```

### 2. Add 2025/2026 Season to Config
`airflow/config/teams/real_madrid.json` currently lists 4 seasons (2021/22–2024/25).
Add `"2025/2026"` to the `seasons` array.

### 3. News & Media Intelligence Layer (New Feature)
See `product-vision.md` in this directory for full spec.
High-level: add a new data source (news/social media) alongside FotMob match data,
feeding into a Notion dashboard for match prediction.

---

## Key File Paths

| Purpose | Path |
|---------|------|
| FotMob API client | `airflow/extract/fotmob_client.py` |
| Extraction orchestrator | `airflow/extract/extract_fotmob_data.py` |
| Airflow DAG | `airflow/dags/extract_team_data_dag.py` |
| Team config | `airflow/config/teams/real_madrid.json` |
| dbt project | `airflow/transform/` |
| DuckDB database | `airflow/fotmob_data.duckdb` |
| Docker Compose | `.dev_container/docker-compose.yaml` |
| Environment vars | `.env` (gitignored) |

## Run dbt
```bash
cd airflow/transform
set -a && source ../../.env && set +a
/Users/deeahnuh/Library/Python/3.9/bin/dbt build --profiles-dir .
```
Note: The `dbt` in PATH is `dbt-fusion` (wrong). Always use the full path above.
