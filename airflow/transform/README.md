# FotMob Analytics DBT Project

A dbt project for transforming raw FotMob match data into analytics-ready dimensional and fact tables using DuckDB.

## Project Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_raw_matches.sql       # Raw JSON match data
│   │   ├── stg_dim_teams.sql         # Team dimension
│   │   ├── stg_dim_leagues.sql       # League dimension
│   │   ├── stg_dim_players.sql       # Player dimension
│   │   ├── stg_fact_matches.sql      # Match facts
│   │   └── stg_fact_stats.sql        # Match statistics
│   └── marts/
│       ├── mart_match_details.sql    # Denormalized match view
│       ├── mart_player_performance.sql # Player analytics
│       └── mart_team_statistics.sql  # Team analytics
├── macros/                            # Custom dbt macros
├── tests/                             # Data quality tests
├── seeds/                             # Static reference data
├── dbt_project.yml                    # Project configuration
├── profiles.yml                       # DuckDB connection config
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.8+
- DuckDB
- dbt-core and dbt-duckdb

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure DuckDB profile:
```bash
dbt debug
```

### Running Models

**Run all models:**
```bash
dbt run
```

**Run specific model:**
```bash
dbt run --select stg_dim_teams
```

**Run staging models only:**
```bash
dbt run --select staging
```

**Run marts only:**
```bash
dbt run --select marts
```

### Testing

**Run all tests:**
```bash
dbt test
```

**Run tests for specific model:**
```bash
dbt test --select stg_fact_matches
```

## Data Models

### Staging Layer

Raw extractions from FotMob JSON with minimal transformation:

- **stg_raw_matches**: Raw JSON input
- **stg_dim_teams**: Team information (home/away)
- **stg_dim_leagues**: League reference data
- **stg_dim_players**: Player roster and performance
- **stg_fact_matches**: Match scores and metadata
- **stg_fact_stats**: Team statistics by match

### Mart Layer

Business-friendly denormalized tables for analysis:

- **mart_match_details**: Complete match information with team/league context
- **mart_player_performance**: Player performance ratings and categories
- **mart_team_statistics**: Team stats with comparative advantage metrics

## Usage Examples

### View all matches with results
```sql
SELECT 
    match_datetime,
    league_name,
    home_team_name || ' vs ' || away_team_name as matchup,
    home_score || '-' || away_score as score,
    result
FROM mart_match_details
ORDER BY match_datetime DESC;
```

### Player performance by team
```sql
SELECT 
    team_name,
    player_name,
    COUNT(*) as matches_played,
    AVG(rating) as avg_rating,
    performance_category
FROM mart_player_performance
GROUP BY team_name, player_name
ORDER BY avg_rating DESC;
```

### Team statistics comparison
```sql
SELECT 
    match_id,
    home_team,
    away_team,
    stat_name,
    home_stat_value,
    away_stat_value,
    stat_advantage,
    match_result
FROM mart_team_statistics
WHERE stat_key IN ('Possession', 'Shots', 'Passes')
ORDER BY match_date DESC;
```

## Configuration

### Profile Setup (profiles.yml)

The project uses DuckDB as the data warehouse. Configuration:

```yaml
fotmob:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '../fotmob_data.duckdb'  # DuckDB file location
      schema: main
      threads: 4
```

### Variables (dbt_project.yml)

```yaml
vars:
  fotmob_data_path: "./fotmob_data.duckdb"
  match_json_path: "data/"
```

## Schema

All models follow a consistent schema with timestamps:
- `dbt_inserted_at`: When record was created
- `dbt_updated_at`: When record was last updated (fact tables)

## Notes

- Models are organized by layer (staging/marts) for clarity
- All JSON extraction uses DuckDB's native JSON functions
- Staging layer performs schema validation with TRY_CAST
- Null handling for optional fields like stadium info for away teams

## Troubleshooting

### Connection Issues
Check DuckDB path in profiles.yml matches your installation.

### JSON Parsing Errors
Verify JSON structure matches fotmob_schema expectations. Check raw match files for schema changes.

### Performance
- DuckDB automatically parallelizes queries across threads
- Increase `threads` in profiles.yml for larger datasets
- Consider creating indexes on frequently filtered columns

## Next Steps

1. Add tests for data quality
2. Implement incremental models for efficiency
3. Add macros for common transformations
4. Create dashboards from mart views
5. Automate dbt runs via Airflow DAG
