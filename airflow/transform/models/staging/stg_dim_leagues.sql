-- Staging model for leagues dimension
-- Extracts and deduplicates league information from match JSON
-- Based on parse_leagues() function from data_model_client.py

{{ config(
    materialized='view',
    tags=['staging', 'dimension']
) }}

WITH raw_matches AS (
    SELECT * FROM {{ ref('stg_raw_matches') }}
),

leagues AS (
    SELECT DISTINCT
        league_id,
        league_name,
        country_code,
        CURRENT_TIMESTAMP as dbt_inserted_at
    FROM raw_matches
)

SELECT * FROM leagues
WHERE league_id IS NOT NULL
