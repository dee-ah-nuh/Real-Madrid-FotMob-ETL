# Real Madrid Intelligence Platform

An end-to-end data pipeline that extracts Real Madrid match data from FotMob,
transforms it with dbt, and stores it in DuckDB — the foundation for a match
prediction system and team intelligence dashboard.

---

## Vision

A personal Notion dashboard combining:
- **Match data** — scores, stats, formations, player ratings from FotMob
- **Media intelligence** — news and social media sentiment per player, manager, and club
- **Match predictions** — ML-driven Win/Draw/Loss probabilities based on form, xG, and media buzz

See `.github/agents/product-vision.md` for the full roadmap.

---

## Architecture

```
FotMob API
    ↓
Airflow (Docker) — fotmob-etl-extract_teams DAG
    ↓
S3: s3://real-madrid-fotmob-data/raw/json/real_madrid/{season}/{match_id}.json
    ↓
dbt (DuckDB) — stg_raw_matches reads all seasons via read_json()
    ↓
Staging Layer (views)            Mart Layer (tables)
─────────────────────            ───────────────────
stg_raw_matches              →   mart_match_details
stg_dim_teams                →   mart_player_performance
stg_dim_leagues              →   mart_team_statistics
stg_dim_players
stg_fact_matches
stg_fact_stats
    ↓
DuckDB: airflow/fotmob_data.duckdb
    ↓ (future)
Notion Dashboard
```

---

## Repository Structure

```
Real-Madrid-FotMob-ETL/
├── .env                              # Credentials (gitignored) — see Setup
├── .dev_container/
│   ├── docker-compose.yaml           # Airflow 3.0.0 stack
│   └── Dockerfile.airflow            # Custom image with requirements
├── .github/
│   └── agents/
│       ├── next-session.md           # Agent handoff — current blockers + TODOs
│       └── product-vision.md         # Full product roadmap
├── airflow/
│   ├── requirements.txt              # Python dependencies for Airflow image
│   ├── config/
│   │   ├── aws_config.py             # S3 bucket + path constants
│   │   └── teams/
│   │       └── real_madrid.json      # Team ID, league ID, seasons to extract
│   ├── dags/
│   │   └── extract_team_data_dag.py  # Airflow DAG definition
│   ├── extract/
│   │   ├── fotmob_client.py          # FotMob API client (IP rotation + retry)
│   │   ├── extract_fotmob_data.py    # Extraction orchestrator + S3 upload
│   │   └── data_model_client.py      # JSON schema definitions + parse functions
│   ├── fotmob_data.duckdb            # Local DuckDB warehouse
│   └── transform/                    # dbt project (see below)
└── README.md
```

---

## dbt Project (`airflow/transform/`)

```
transform/
├── dbt_project.yml           # Project: fotmob | Profile: fotmob | DuckDB
├── profiles.yml              # dev (local DuckDB) + prod targets
├── macros/
│   └── json_helpers.sql      # read_json_from_s3(), extract_json_field()
└── models/
    ├── sources.yml           # Note: reads directly from S3 via read_json()
    ├── staging/
    │   ├── schema.yml        # Docs + tests for all 6 staging models
    │   ├── stg_raw_matches.sql       # [TABLE] Reads all JSON from S3, adds raw_json_data
    │   ├── stg_dim_teams.sql         # [VIEW]  Team + stadium info per match
    │   ├── stg_dim_leagues.sql       # [VIEW]  Distinct leagues
    │   ├── stg_dim_players.sql       # [VIEW]  Starting lineups per match
    │   ├── stg_fact_matches.sql      # [VIEW]  Scores, formations, match status
    │   └── stg_fact_stats.sql        # [VIEW]  Home/away stats per match
    └── marts/
        ├── schema.yml        # Docs + tests for all 3 mart models
        ├── mart_match_details.sql      # [TABLE] Denormalized match view
        ├── mart_player_performance.sql # [TABLE] Player ratings + match result
        └── mart_team_statistics.sql    # [TABLE] Stats with home/away advantage
```

### DuckDB Schemas
| Schema | Contents |
|--------|----------|
| `main_staging` | 6 views (stg_* models) |
| `main_marts` | 3 tables (mart_* models) |

### dbt Test Coverage
34 tests across all models covering:
- `not_null` on all primary/foreign keys
- `unique` on match-level primary keys
- `accepted_values` on status, side, result, and category enums

---

## Data Model

### S3 Structure
```
s3://real-madrid-fotmob-data/
└── raw/json/
    ├── test/                         # Old test files (3 files, ignore)
    └── real_madrid/
        ├── 2021_2022/{match_id}.json
        ├── 2022_2023/{match_id}.json
        ├── 2023_2024/{match_id}.json
        └── 2024_2025/{match_id}.json
```

Each file is a raw FotMob match JSON with top-level keys:
`general`, `header`, `content`, `nav`, `seo`, `ongoing`, `hasPendingVAR`

### Key Extracted Fields
| Table | Key Columns |
|-------|------------|
| `stg_raw_matches` | match_id, home/away team, league, scores, raw_json_data |
| `stg_dim_teams` | match_id, team_id, team_name, stadium info, team_side |
| `stg_dim_leagues` | league_id, league_name, country_code |
| `stg_dim_players` | match_id, player_id, name, position, rating, side |
| `stg_fact_matches` | match_id, scores, formations, match_status, timestamps |
| `stg_fact_stats` | match_id, stat_key, home_value, away_value, is_highlighted |
| `mart_match_details` | Full denormalized match with team names, stadium, result |
| `mart_player_performance` | Player + match + rating + performance_category + match_result |
| `mart_team_statistics` | Per-stat advantage (home/away/tied) per match |

---

## Setup

### Prerequisites
- Docker Desktop
- AWS credentials with S3 read/write access to `real-madrid-fotmob-data`
- Python 3.9 with `dbt-duckdb==1.7.0` installed

### 1. Environment Variables
Create `.env` in the repo root:
```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-2
AIRFLOW_UID=501               # macOS: use output of `id -u`
AIRFLOW_PROJ_DIR=/path/to/this/repo
```

### 2. Start Airflow
```bash
docker-compose -f .dev_container/docker-compose.yaml up -d --build
```
Access UI at http://localhost:8080 (credentials: `airflow` / `airflow`)

### 3. Run dbt
```bash
cd airflow/transform
set -a && source ../../.env && set +a
/Users/deeahnuh/Library/Python/3.9/bin/dbt build --profiles-dir .
```
> Note: The `dbt` binary in PATH may be `dbt-fusion` which doesn't support DuckDB.
> Always use the full path to the `dbt-core` binary.

### 4. Query the Database
Open `airflow/fotmob_data.duckdb` with DBeaver or the DuckDB CLI:
```sql
SELECT * FROM main_marts.mart_match_details LIMIT 10;
SELECT * FROM main_marts.mart_player_performance ORDER BY rating DESC LIMIT 20;
SELECT * FROM main_marts.mart_team_statistics WHERE stat_key = 'possession';
```

---

## Airflow DAG

**DAG ID:** `fotmob-etl-extract_teams`
**Schedule:** Daily at 06:00 UTC
**Tasks (one per season):**
- `extract_real_madrid_2021_2022`
- `extract_real_madrid_2022_2023`
- `extract_real_madrid_2023_2024`
- `extract_real_madrid_2024_2025`
- `extract_real_madrid_2025_2026`

Each task:
1. Calls FotMob API to get all finished La Liga matches for Real Madrid in that season
2. For each match, fetches full match details JSON
3. Uploads to S3 at `raw/json/real_madrid/{season}/{match_id}.json`

### IP Rotation
The client uses `requests-ip-rotator` to route requests through AWS API Gateway,
avoiding FotMob rate limits. **Currently broken** — see `.github/agents/next-session.md`.

---

## Known Issues

| Issue | Status | Location |
|-------|--------|----------|
| IP rotator fails in us-east-2 (0 API Gateway endpoints created) | Open | `airflow/extract/fotmob_client.py` |
| dbt binary conflict (dbt-fusion vs dbt-core) | Workaround documented | Use full path |
| match_time_utc is a non-standard string format | Fixed | All marts use TRY_STRPTIME |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 3.0.0 (Docker) |
| Data Extraction | Python + requests-ip-rotator |
| Raw Storage | AWS S3 |
| Data Warehouse | DuckDB |
| Transformation | dbt-core 1.7.0 + dbt-duckdb 1.7.0 |
| Infrastructure | Docker Compose + PostgreSQL (Airflow metadata) |

---

## Contributing / Agent Instructions

This repository is designed to be worked on by AI agents.
Before starting any session, read `.github/agents/next-session.md` for the current
state of the project and immediate priorities.

After completing work, update `.github/agents/next-session.md` with:
- What was done
- What is still broken or pending
- Any new context the next session needs
