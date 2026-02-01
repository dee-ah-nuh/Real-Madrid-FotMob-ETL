-- Staging model for players from match lineups
-- Extracts and unnests player information from lineup data
-- Based on parse_players() function from data_model_client.py

{{ config(
    materialized='view',
    tags=['staging', 'dimension']
) }}

WITH raw_matches AS (
    SELECT * FROM {{ ref('stg_raw_matches') }}
),

-- Extract home team starters
home_starters AS (
    SELECT 
        general_match_id as match_id,
        home_team_id as team_id,
        home_team_name as team_name,
        'home' as side,
        'starter' as role,
        player_obj->>'id' as player_id,
        player_obj->>'name' as player_name,
        player_obj->>'firstName' as first_name,
        player_obj->>'lastName' as last_name,
        TRY_CAST(player_obj->>'age' AS INTEGER) as age,
        player_obj->>'countryName' as country_name,
        player_obj->>'countryCode' as country_code,
        TRY_CAST(player_obj->>'positionId' AS INTEGER) as position_id,
        TRY_CAST(player_obj->>'usualPlayingPositionId' AS INTEGER) as usual_position_id,
        player_obj->>'shirtNumber' as shirt_number,
        TRY_CAST(player_obj->'performance'->>'rating' AS FLOAT) as rating,
        NULL::INTEGER as sub_in_time,
        NULL::INTEGER as sub_out_time,
        NULL::VARCHAR as unavailability_type
    FROM raw_matches,
    LATERAL (
        SELECT json_extract(player, '$') as player_obj
        FROM json_extract(raw_json_data, '$.content.lineup.homeTeam.starters') as tbl(player)
    ) starters
),

-- Extract away team starters
away_starters AS (
    SELECT 
        general_match_id as match_id,
        away_team_id as team_id,
        away_team_name as team_name,
        'away' as side,
        'starter' as role,
        player_obj->>'id' as player_id,
        player_obj->>'name' as player_name,
        player_obj->>'firstName' as first_name,
        player_obj->>'lastName' as last_name,
        TRY_CAST(player_obj->>'age' AS INTEGER) as age,
        player_obj->>'countryName' as country_name,
        player_obj->>'countryCode' as country_code,
        TRY_CAST(player_obj->>'positionId' AS INTEGER) as position_id,
        TRY_CAST(player_obj->>'usualPlayingPositionId' AS INTEGER) as usual_position_id,
        player_obj->>'shirtNumber' as shirt_number,
        TRY_CAST(player_obj->'performance'->>'rating' AS FLOAT) as rating,
        NULL::INTEGER as sub_in_time,
        NULL::INTEGER as sub_out_time,
        NULL::VARCHAR as unavailability_type
    FROM raw_matches,
    LATERAL (
        SELECT json_extract(player, '$') as player_obj
        FROM json_extract(raw_json_data, '$.content.lineup.awayTeam.starters') as tbl(player)
    ) starters
)

SELECT 
    match_id,
    team_id,
    team_name,
    side,
    role,
    player_id,
    player_name,
    first_name,
    last_name,
    age,
    country_name,
    country_code,
    position_id,
    usual_position_id,
    shirt_number,
    rating,
    sub_in_time,
    sub_out_time,
    unavailability_type,
    CURRENT_TIMESTAMP as dbt_inserted_at
FROM home_starters
UNION ALL
SELECT 
    match_id,
    team_id,
    team_name,
    side,
    role,
    player_id,
    player_name,
    first_name,
    last_name,
    age,
    country_name,
    country_code,
    position_id,
    usual_position_id,
    shirt_number,
    rating,
    sub_in_time,
    sub_out_time,
    unavailability_type,
    CURRENT_TIMESTAMP as dbt_inserted_at
FROM away_starters
        player->>'lastName' as last_name,
        TRY_CAST(player->>'age' AS INTEGER) as age,
        player->>'countryName' as country_name,
        player->>'countryCode' as country_code,
        TRY_CAST(player->>'positionId' AS INTEGER) as position_id,
        TRY_CAST(player->>'usualPlayingPositionId' AS INTEGER) as usual_position_id,
        player->>'shirtNumber' as shirt_number,
        TRY_CAST(player->'performance'->>'rating' AS FLOAT) as rating,
        NULL as sub_in_time,
        NULL as sub_out_time,
        NULL as unavailability_type,
        CURRENT_TIMESTAMP as dbt_inserted_at
    FROM raw_matches,
    LATERAL FLATTEN(input => match_json->'content'->'lineup'->'awayTeam'->'starters') player
),

all_players AS (
    SELECT * FROM home_starters
    UNION ALL
    SELECT * FROM away_starters
)

SELECT * FROM all_players
