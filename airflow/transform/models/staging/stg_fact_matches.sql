-- Staging model for matches fact table
-- Extracts core match information from raw JSON
-- Based on parse_matches() function from data_model_client.py

{{ config(
    materialized='view',
    tags=['staging', 'fact']
) }}

WITH raw_matches AS (
    SELECT * FROM {{ ref('stg_raw_matches') }}
),

matches AS (
    SELECT 
        general_match_id as match_id,
        match_name,
        match_round,
        match_time_utc,
        league_id,
        home_team_id,
        away_team_id,
        TRY_CAST(json_extract_string(raw_json_data, '$.header.teams[0].score') AS INTEGER) as home_score,
        TRY_CAST(json_extract_string(raw_json_data, '$.header.teams[1].score') AS INTEGER) as away_score,
        json_extract_string(raw_json_data, '$.content.lineup.homeTeam.formation')::VARCHAR as home_formation,
        json_extract_string(raw_json_data, '$.content.lineup.awayTeam.formation')::VARCHAR as away_formation,
        country_code,
        CASE 
            WHEN json_extract_string(raw_json_data, '$.header.status.finished') = 'true' THEN 'finished'
            WHEN json_extract_string(raw_json_data, '$.header.status.started') = 'true' THEN 'started'
            WHEN json_extract_string(raw_json_data, '$.header.status.cancelled') = 'true' THEN 'cancelled'
            ELSE 'scheduled'
        END as match_status,
        CURRENT_TIMESTAMP as dbt_inserted_at,
        CURRENT_TIMESTAMP as dbt_updated_at
    FROM raw_matches
)

SELECT * FROM matches
WHERE match_id IS NOT NULL
