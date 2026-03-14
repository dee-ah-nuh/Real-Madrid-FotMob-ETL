-- Mart model: Team statistics summary
-- Aggregates team-level statistics for performance analysis

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stats AS (
    SELECT * FROM {{ ref('stg_fact_stats') }}
),

matches AS (
    SELECT * FROM {{ ref('stg_fact_matches') }}
),

team_stats AS (
    SELECT 
        s.match_id,
        TRY_STRPTIME(m.match_time_utc, '%a, %b %d, %Y, %H:%M UTC') as match_date,
        m.league_id,
        l.league_name,
        s.home_team_id,
        ht.team_name as home_team,
        s.away_team_id,
        awt.team_name as away_team,
        s.stat_key,
        s.stat_name,
        s.home_value as home_stat_value,
        s.away_value as away_stat_value,
        s.is_highlighted,
        CASE
            WHEN s.home_value > s.away_value THEN 'home_advantage'
            WHEN s.home_value < s.away_value THEN 'away_advantage'
            ELSE 'tied'
        END as stat_advantage,
        m.home_score,
        m.away_score,
        CASE 
            WHEN m.home_score > m.away_score THEN 'home_win'
            WHEN m.away_score > m.home_score THEN 'away_win'
            ELSE 'draw'
        END as match_result,
        s.dbt_inserted_at
    FROM stats s
    INNER JOIN matches m ON s.match_id = m.match_id
    LEFT JOIN {{ ref('stg_dim_teams') }} ht ON s.home_team_id = ht.team_id AND s.match_id = ht.match_id AND ht.team_side = 'home'
    LEFT JOIN {{ ref('stg_dim_teams') }} awt ON s.away_team_id = awt.team_id AND s.match_id = awt.match_id AND awt.team_side = 'away'
    LEFT JOIN {{ ref('stg_dim_leagues') }} l ON m.league_id = l.league_id
)

SELECT * FROM team_stats
ORDER BY match_date DESC, stat_key
