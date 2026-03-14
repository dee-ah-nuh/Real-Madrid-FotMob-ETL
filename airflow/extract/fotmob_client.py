import time
import logging
import requests
from requests_ip_rotator import ApiGateway

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FotMobClient:
    BASE_URL = "https://www.fotmob.com"
    API_URL = f"{BASE_URL}/api"

    def __init__(self, regions=None):
        self.regions = regions or ["us-east-1", "us-east-2", "us-west-2"]
        self.gateway = None
        self.session = None
        self.start_session()

    def start_session(self):
        logger.info("Starting IP rotator gateway...")
        try:
            self.gateway = ApiGateway(self.BASE_URL, regions=self.regions)
            self.gateway.start()
            endpoints = getattr(self.gateway, "endpoints", [])
            if not endpoints:
                logger.warning("No API Gateway endpoints created. Falling back to direct requests.")
                self.gateway = None
        except Exception as e:
            logger.warning(f"IP rotator setup failed: {e}. Falling back to direct requests.")
            self.gateway = None

        self.session = requests.Session()
        if self.gateway:
            self.session.mount(self.BASE_URL, self.gateway)
            logger.info("Session ready with IP rotation.")
        else:
            logger.info("Session ready (direct requests, no IP rotation).")

    def request(self, endpoint, params=None, max_retries=3):
        url = f"{self.API_URL}/{endpoint}"

        for attempt in range(max_retries):
            try:
                logger.info(f"Request: {endpoint} (attempt {attempt + 1})")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                time.sleep(1)  # Rate limit
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All retries failed for {endpoint}")
                    return None

    def get_league_fixtures(self, league_id, season):
        data = self.request("leagues", params={"id": league_id, "season": season})
        if data and "fixtures" in data:
            return data["fixtures"].get("allMatches", [])
        return []

    def get_team_fixtures(self, league_id, season, team_id):
        matches = self.get_league_fixtures(league_id, season)
        team_matches = []
        for match in matches:
            home_id = match.get("home", {}).get("id")
            away_id = match.get("away", {}).get("id")
            if str(home_id) == str(team_id) or str(away_id) == str(team_id):
                team_matches.append(match)
        logger.info(f"Found {len(team_matches)} matches for team {team_id}")
        return team_matches

    def get_match_details(self, match_id):
        return self.request("matchDetails", params={"matchId": match_id})

    def close(self):
        if self.gateway:
            try:
                logger.info("Shutting down gateway...")
                self.gateway.shutdown()
                logger.info("Gateway closed.")
            except Exception as e:
                logger.warning(f"Error shutting down gateway: {e}")
