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
        TRY_CAST(stat_item->'stat'->>'value' AS FLOAT) as value,
        TRY_CAST(stat_item->'stat'->>'total' AS FLOAT) as total,
        stat_item->'stat'->>'type' as stat_type,
        'all_periods' as period,
        CURRENT_TIMESTAMP as dbt_inserted_at
    FROM raw_matches,
    LATERAL (
        SELECT json_extract(stat, '$') as stat
        FROM json_extract(raw_json_data, '$.content.stats.Periods.All.stats') as tbl(stat)
    ) stats_arr,
    LATERAL (
        SELECT json_extract(item, '$') as stat_item
        FROM json_extract(stats_arr.stat, '$.stats') as items(item)
    ) stat_items
)

SELECT * FROM stats
WHERE match_id IS NOT NULL
