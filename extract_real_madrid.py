import json
import logging
import boto3
import argparse 

from extract.fotmob_client import FotMobClient
from config.aws_config import (
    AWS_REGION,
    S3_BUCKET,
    S3_PATHS,
    REAL_MADRID_TEAM_ID,
    LA_LIGA_ID,
    SEASONS
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name=AWS_REGION)

season = SEASONS[0]
# if ran locally then specify the season we want to extract

def upload_to_s3(data, match_id, season):
    key = f"{S3_PATHS['raw_matches']}/{match_id}_season_{season}.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json",
        )
        logger.info(f"Uploaded {match_id}.json to s3://{S3_BUCKET}/{key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {match_id}: {e}")
        return False


def extract_completed_matches(client, season):
    fixtures = client.get_team_fixtures(LA_LIGA_ID, season, REAL_MADRID_TEAM_ID)
    completed = []
    for match in fixtures:
        if match.get("status", {}).get("finished"):
            completed.append(match["id"])
    logger.info(f"Found {len(completed)} completed matches out of {len(fixtures)} total")
    return completed


def run_extraction(season):
    client = FotMobClient()
    
    try:
        logger.info(f"Processing season {season}...")
        match_ids = extract_completed_matches(client, season)
        
        success_count = 0
        for match_id in match_ids:
            logger.info(f"Fetching match {match_id}...")
            details = client.get_match_details(match_id)
            
            if details:
                if upload_to_s3(details, match_id, season):
                    success_count += 1
            else:
                logger.warning(f"No details returned for match {match_id}")
        
        logger.info(f"Completed: {success_count}/{len(match_ids)} matches uploaded to S3")
    
    finally:
        client.close()


# uncomment this for local run:
def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--season", required=True, help="Season to extract (e.g., 2024/2025)")
    # args = parser.parse_args()
    run_extraction(season)


if __name__ == "__main__":
    main()
