import json
import logging
import boto3

from extract.fotmob_client import FotMobClient
from config.aws_config import ( #type:ignore
    AWS_REGION,
    S3_BUCKET,
    S3_PATHS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name=AWS_REGION)


def load_team_config(config_path):
    with open(config_path) as f:
        return json.load(f)


def upload_to_s3(data, team_name, match_id, season):
    season_str = season.replace("/", "_")
    key = f"{S3_PATHS['raw_json']}/{team_name}/{season_str}/{match_id}.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json",
        )
        logger.info(f"Uploaded to s3://{S3_BUCKET}/{key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {match_id}: {e}")
        return False


def extract_completed_matches(client, league_id, team_id, season):
    fixtures = client.get_team_fixtures(league_id, season, team_id)
    completed = []
    for match in fixtures:
        if match.get("status", {}).get("finished"):
            completed.append(match["id"])
    logger.info(f"Found {len(completed)} completed matches out of {len(fixtures)} total")
    return completed


def run_extraction(config_path, season):
    config = load_team_config(config_path)
    
    team_id = config["team_id"]
    team_name = config["team_name"]
    league_id = config["league_id"]
    
    client = FotMobClient()
    
    try:
        logger.info(f"Processing {team_name} - season {season}...")
        match_ids = extract_completed_matches(client, league_id, team_id, season)
        
        success_count = 0
        for match_id in match_ids:
            logger.info(f"Fetching match {match_id}...")
            details = client.get_match_details(match_id)
            
            if details:
                if upload_to_s3(details, team_name, match_id, season):
                    success_count += 1
            else:
                logger.warning(f"No details returned for match {match_id}")
        
        logger.info(f"Completed: {success_count}/{len(match_ids)} matches uploaded to S3")
        return success_count
    
    finally:
        client.close()