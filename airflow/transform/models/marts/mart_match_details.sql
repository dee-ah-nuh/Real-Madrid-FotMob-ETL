-- Mart model: Complete match details with teams and league info
-- Business-friendly denormalized table for analysis

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_fact_matches') }}
),

home_teams AS (
    SELECT 
        match_id,
        team_id,
        team_name,
        stadium_name,
        stadium_city,
        stadium_lat,
        stadium_lon
    FROM {{ ref('stg_dim_teams') }}
    WHERE team_side = 'home'
),

away_teams AS (
    SELECT 
        match_id,
        team_id,
        team_name
    FROM {{ ref('stg_dim_teams') }}
    WHERE team_side = 'away'
),

leagues AS (
    SELECT * FROM {{ ref('stg_dim_leagues') }}
),

match_details AS (
    SELECT 
        m.match_id,
        m.match_name,
        m.match_round,
        m.match_time_utc::TIMESTAMP as match_datetime,
        m.league_id,
        l.league_name,
        l.parent_league_name,
        m.home_team_id,
        ht.team_name as home_team_name,
        m.away_team_id,
        at.team_name as away_team_name,
        m.home_score,
        m.away_score,
        CASE 
            WHEN m.home_score > m.away_score THEN 'home_win'
            WHEN m.home_score < m.away_score THEN 'away_win'
            WHEN m.home_score = m.away_score THEN 'draw'
            ELSE NULL
        END as result,
        m.home_formation,
        m.away_formation,
        ht.stadium_name,
        ht.stadium_city,
        m.match_status,
        m.dbt_inserted_at,
        m.dbt_updated_at
    FROM matches m
    LEFT JOIN home_teams ht ON m.match_id = ht.match_id
    LEFT JOIN away_teams at ON m.match_id = at.match_id
    LEFT JOIN leagues l ON m.league_id = l.league_id
)

SELECT * FROM match_details
