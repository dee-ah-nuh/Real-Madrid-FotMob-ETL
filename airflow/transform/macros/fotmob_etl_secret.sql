-- Create S3 credential secret for fotmob data access
-- This uses the credential chain (AWS CLI config/env vars)
CREATE OR REPLACE SECRET fotmob_etl_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'AKIA2UC3E2EDZWZ6GOM5',
    SECRET 'rfKmcQvlsKDzswrFO2tpiPmBlEkD9YhYdObYcewf',
    REGION 'us-east-2'
);



SELECT * FROM read_json('s3://real-madrid-fotmob-data/raw/json/test/*.json');
-- general, header, nav, ongoing, hasPendingVAR, content, seo 


SELECT 
       regexp_extract(filename, '(\d+)\.json$', 1) as match_id,
       filename as source_file,
       general.matchId,
       * as raw_json_data,  -- or just SELECT * to see everything
       CURRENT_TIMESTAMP as loaded_at
   FROM read_json(
       's3://real-madrid-fotmob-data/raw/json/test/*.json',
       format='auto',
       filename=true
   );





SELECT 
    regexp_extract(filename, '(\d+)\.json$', 1)::BIGINT as match_id,
    filename as source_file,
    general['matchId']::BIGINT as general_match_id,
    general['matchName']::VARCHAR as match_name,
    general['matchRound']::INTEGER as match_round,
    general['teamColors']['darkMode']['home']::VARCHAR as team_color_dark_home,
    general['teamColors']['darkMode']['away']::VARCHAR as team_color_dark_away,
    general['teamColors']['lightMode']['home']::VARCHAR as team_color_light_home,
    general['teamColors']['lightMode']['away']::VARCHAR as team_color_light_away,
    general['teamColors']['fontDarkMode']['home']::VARCHAR as font_color_dark_home,
    general['teamColors']['fontDarkMode']['away']::VARCHAR as font_color_dark_away,
    general['teamColors']['fontLightMode']['home']::VARCHAR as font_color_light_home,
    general['teamColors']['fontLightMode']['away']::VARCHAR as font_color_light_away,
    general['leagueId']::INTEGER as league_id,
    general['leagueName']::VARCHAR as league_name,
    general['leagueRoundName']::VARCHAR as league_round_name,
    general['parentLeagueId']::INTEGER as parent_league_id,
    general['countryCode']::VARCHAR as country_code,
    general['homeTeam']['name']::VARCHAR as home_team_name,
    general['homeTeam']['id']::INTEGER as home_team_id,
    general['awayTeam']['name']::VARCHAR as away_team_name,
    general['awayTeam']['id']::INTEGER as away_team_id,
    general['coverageLevel']::VARCHAR as coverage_level,
    general['matchTimeUTC']::VARCHAR as match_time_utc,
    general['matchTimeUTCDate']::TIMESTAMP as match_time_utc_date,
    general['started']::BOOLEAN as started,
    general['finished']::BOOLEAN as finished,
    CURRENT_TIMESTAMP as loaded_at
FROM read_json('s3://real-madrid-fotmob-data/raw/json/test/*.json', format='auto', filename=true)
WHERE general IS NOT NULL;

SELECT 
    regexp_extract(filename, '(\d+)\.json$', 1) as match_id,
    filename as source_file,
    general.*,
    content.*,
    CURRENT_TIMESTAMP as loaded_at
FROM read_json('s3://real-madrid-fotmob-data/raw/json/test/*.json', format='auto', filename=true)
WHERE general IS NOT NULL;