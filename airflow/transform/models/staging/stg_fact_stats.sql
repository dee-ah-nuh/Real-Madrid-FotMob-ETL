-- Staging model for match statistics
-- Extracts team stats by period and category
-- Based on parse_stats() function from data_model_client.py

{{ config(
    materialized='view',
    tags=['staging', 'fact']
) }}

WITH raw_matches AS (
    SELECT * FROM {{ ref('stg_raw_matches') }}
),

stats AS (
    SELECT 
        general_match_id as match_id,
        home_team_id,
        away_team_id,
        stat->>'title' as stat_category,
        stat->>'key' as stat_key,
        stat_item->>'key' as stat_name,
        TRY_CAST(stat_item->'stat'->>'home' AS FLOAT) as home_value,
        TRY_CAST(stat_item->'stat'->>'away' AS FLOAT) as away_value,
        stat_item->'stat'->>'type' as stat_type,
        TRY_CAST(stat_item->>'highlighted' AS BOOLEAN) as is_highlighted,
        'all_periods' as period,
        CURRENT_TIMESTAMP as dbt_inserted_at
    FROM raw_matches,
    LATERAL (SELECT unnest(json_extract(raw_json_data, '$.content.stats.Periods.All.stats')::JSON[]) as stat) stats_arr,
    LATERAL (SELECT unnest(json_extract(stat, '$.stats')::JSON[]) as stat_item) stat_items
)

SELECT * FROM stats
WHERE match_id IS NOT NULL
