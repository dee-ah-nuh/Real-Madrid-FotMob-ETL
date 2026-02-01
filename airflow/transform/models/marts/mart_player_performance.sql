-- Mart model: Player performance analysis
-- Aggregates player-level data for performance metrics

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH players AS (
    SELECT * FROM {{ ref('stg_dim_players') }}
),

matches AS (
    SELECT * FROM {{ ref('stg_fact_matches') }}
),

player_performance AS (
    SELECT 
        p.player_id,
        p.player_name,
        p.first_name,
        p.last_name,
        p.position_id,
        p.usual_position_id,
        p.country_code,
        p.team_id,
        p.team_name,
        m.match_id,
        m.match_name,
        m.match_time_utc::TIMESTAMP as match_date,
        m.league_id,
        l.league_name,
        p.side,
        p.role,
        p.shirt_number,
        p.rating,
        CASE
            WHEN p.rating >= 7.5 THEN 'excellent'
            WHEN p.rating >= 6.5 THEN 'good'
            WHEN p.rating >= 5.5 THEN 'average'
            ELSE 'poor'
        END as performance_category,
        m.home_score,
        m.away_score,
        CASE 
            WHEN p.side = 'home' AND m.home_score > m.away_score THEN 'win'
            WHEN p.side = 'away' AND m.away_score > m.home_score THEN 'win'
            WHEN m.home_score = m.away_score THEN 'draw'
            ELSE 'loss'
        END as match_result,
        p.dbt_inserted_at
    FROM players p
    INNER JOIN matches m ON p.match_id = m.match_id
    LEFT JOIN {{ ref('stg_dim_leagues') }} l ON m.league_id = l.league_id
)

SELECT * FROM player_performance
ORDER BY player_id, match_date DESC
