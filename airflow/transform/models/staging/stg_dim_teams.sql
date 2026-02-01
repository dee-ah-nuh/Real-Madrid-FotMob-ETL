-- Staging model for teams dimension
-- Extracts and flattens team information from match JSON
-- Based on parse_teams() function from data_model_client.py

{{ config(
    materialized='view',
    tags=['staging', 'dimension']
) }}

WITH raw_matches AS (
    SELECT * FROM {{ ref('stg_raw_matches') }}
),

-- Extract home team with stadium info
home_teams AS (
    SELECT 
        general_match_id as match_id,
        home_team_id as team_id,
        home_team_name as team_name,
        country_code,
        json_extract_string(raw_json_data, '$.content.matchFacts.infoBox.Stadium.name')::VARCHAR as stadium_name,
        json_extract_string(raw_json_data, '$.seo.eventJSONLD.location.address.addressLocality')::VARCHAR as stadium_city,
        TRY_CAST(json_extract_string(raw_json_data, '$.content.matchFacts.infoBox.Attendance') AS INTEGER) as stadium_capacity,
        TRY_CAST(json_extract_string(raw_json_data, '$.seo.eventJSONLD.latitude') AS FLOAT) as stadium_lat,
        TRY_CAST(json_extract_string(raw_json_data, '$.seo.eventJSONLD.longitude') AS FLOAT) as stadium_lon,
        'home' as team_side
    FROM raw_matches
),

-- Extract away team
away_teams AS (
    SELECT 
        general_match_id as match_id,
        away_team_id as team_id,
        away_team_name as team_name,
        country_code,
        NULL::VARCHAR as stadium_name,
        NULL::VARCHAR as stadium_city,
        NULL::INTEGER as stadium_capacity,
        NULL::FLOAT as stadium_lat,
        NULL::FLOAT as stadium_lon,
        'away' as team_side
    FROM raw_matches
),

-- Union both teams
all_teams AS (
    SELECT * FROM home_teams
    UNION ALL
    SELECT * FROM away_teams
)

SELECT 
    team_id,
    team_name,
    country_code,
    stadium_name,
    stadium_city,
    stadium_capacity,
    stadium_lat,
    stadium_lon,
    team_side,
    CURRENT_TIMESTAMP as dbt_inserted_at
FROM all_teams
    SELECT * FROM away_teams
)

SELECT * FROM all_teams
