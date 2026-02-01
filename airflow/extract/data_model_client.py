"""
Library for parsing match JSON data for FotMob
"""
import pandas as pd

def parse_teams(data):
    """Extract dim_teams from match JSON"""
    rows = []
    
    general = data.get("general", {})
    content = data.get("content", {})
    seo = data.get("seo", {})
    
    # Get stadium info
    info_box = content.get("matchFacts", {}).get("infoBox", {})
    stadium = info_box.get("Stadium", {})
    location = seo.get("eventJSONLD", {}).get("location", {})
    
    # Handle Attendance - could be int or dict
    attendance = info_box.get("Attendance")
    if isinstance(attendance, dict):
        attendance = attendance.get("number")
    
    # Home team
    home = general.get("homeTeam", {})
    rows.append({
        "team_id": home.get("id"),
        "team_name": home.get("name"),
        "country_code": general.get("countryCode"),
        "stadium_name": stadium.get("name") if isinstance(stadium, dict) else stadium,
        "stadium_city": location.get("address", {}).get("addressLocality"),
        "stadium_capacity": attendance,
        "stadium_lat": location.get("latitude"),
        "stadium_lon": location.get("longitude"),
    })
    
    # Away team
    away = general.get("awayTeam", {})
    rows.append({
        "team_id": away.get("id"),
        "team_name": away.get("name"),
        "country_code": general.get("countryCode"),
        "stadium_name": None,
        "stadium_city": None,
        "stadium_capacity": None,
        "stadium_lat": None,
        "stadium_lon": None,
    })
    
    return rows

def parse_leagues(data):
    """Extract dim_leagues from match JSON"""
    general = data.get("general", {})
    
    return [{
        "league_id": general.get("leagueId"),
        "league_name": general.get("leagueName"),
    }]

def parse_players(data):
    general = data.get("general", {})
    lineup = data.get("content", {}).get("lineup", {})

    match_id = general.get("matchId")
    rows = []

    def extract_players(team, side):
        team_id = team.get("id")
        team_name = team.get("name")

        # -------- starters --------
        for p in team.get("starters", []):
            rows.append({
                "match_id": match_id,
                "team_id": team_id,
                "team_name": team_name,
                "side": side,
                "role": "starter",

                "player_id": p.get("id"),
                "name": p.get("name"),
                "first_name": p.get("firstName"),
                "last_name": p.get("lastName"),
                "age": p.get("age"),
                "country_name": p.get("countryName"),
                "country_code": p.get("countryCode"),

                "position_id": p.get("positionId"),
                "usual_position_id": p.get("usualPlayingPositionId"),
                "shirt_number": p.get("shirtNumber"),

                "rating": p.get("performance", {}).get("rating"),
                "sub_in_time": None,
                "sub_out_time": next(
                    (e.get("time") for e in p.get("performance", {})
                     .get("substitutionEvents", [])
                     if e.get("type") == "subOut"),
                    None
                ),

                "unavailability_type": None,
                "expected_return": None,
            })

        # -------- subs --------
        for p in team.get("subs", []):
            sub_in_time = next(
                (e.get("time") for e in p.get("performance", {})
                 .get("substitutionEvents", [])
                 if e.get("type") == "subIn"),
                None
            )

            rows.append({
                "match_id": match_id,
                "team_id": team_id,
                "team_name": team_name,
                "side": side,
                "role": "sub",

                "player_id": p.get("id"),
                "name": p.get("name"),
                "first_name": p.get("firstName"),
                "last_name": p.get("lastName"),
                "age": p.get("age"),
                "country_name": p.get("countryName"),
                "country_code": p.get("countryCode"),

                "position_id": None,
                "usual_position_id": p.get("usualPlayingPositionId"),
                "shirt_number": p.get("shirtNumber"),

                "rating": p.get("performance", {}).get("rating"),
                "sub_in_time": sub_in_time,
                "sub_out_time": None,

                "unavailability_type": None,
                "expected_return": None,
            })

        # -------- unavailable --------
        for p in team.get("unavailable", []):
            rows.append({
                "match_id": match_id,
                "team_id": team_id,
                "team_name": team_name,
                "side": side,
                "role": "unavailable",

                "player_id": p.get("id"),
                "name": p.get("name"),
                "first_name": p.get("firstName"),
                "last_name": p.get("lastName"),
                "age": p.get("age"),
                "country_name": p.get("countryName"),
                "country_code": p.get("countryCode"),

                "position_id": None,
                "usual_position_id": None,
                "shirt_number": None,

                "rating": None,
                "sub_in_time": None,
                "sub_out_time": None,

                "unavailability_type": p.get("unavailability", {}).get("type"),
                "expected_return": p.get("unavailability", {}).get("expectedReturn"),
            })

    # home / away
    if "homeTeam" in lineup:
        extract_players(lineup["homeTeam"], "home")

    if "awayTeam" in lineup:
        extract_players(lineup["awayTeam"], "away")

    return pd.DataFrame(rows)

def parse_matches(data):
    """Extract fact_matches from match JSON"""
    general = data.get("general", {})
    header = data.get("header", {})
    lineup = data.get("content", {}).get("lineup", {})
    
    teams = header.get("teams", [{}, {}])

    get_starter_ids = lambda team: [p.get("id") for p in team.get("starters", [])]

    return [{
        "match_id": general.get("matchId"),
        "match_name": general.get("matchName"),
        "match_round": general.get("matchRound"),
        "match_time_utc": general.get("matchTimeUTCDate"),
        "league_id": general.get("leagueId"),
        "home_team_id": general.get("homeTeam", {}).get("id"),
        "away_team_id": general.get("awayTeam", {}).get("id"),
        "home_score": teams[0].get("score"),
        "away_score": teams[1].get("score"),
        "home_formation": lineup.get("homeTeam", {}).get("formation"),
        "away_formation": lineup.get("awayTeam", {}).get("formation"),
        "home_lineup": get_starter_ids(lineup.get("homeTeam", {})),
        "away_lineup": get_starter_ids(lineup.get("awayTeam", {}))
    }]

def parse_stats(data, period, match_id):
    """Extract fact_stats from match JSON"""
    rows = []
    
    stats_data = (data.get("content", {})
                      .get("stats", {})
                      .get("Periods", {})
                      .get(period, {})
                      .get("stats", []))
    
    for category in stats_data:
        category_name = category.get("title")
        
        for stat in category.get("stats", []):
            if stat.get("type") == "title":
                continue
            
            stat_values = stat.get("stats", [None, None])
            
            rows.append({
                "match_id": match_id,
                "stat_category": category_name,
                "stat_key": stat.get("key"),
                "stat_name": stat.get("title"),
                "home_value": str(stat_values[0]) if stat_values[0] is not None else None,
                "away_value": str(stat_values[1]) if stat_values[1] is not None else None,
                "h_a_flag": stat.get("highlighted"),
            })
    
    return rows



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    BooleanType,
    MapType,
    FloatType,
    DoubleType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import col, explode, lit, when

def fotmob_schema():
    shot_struct = StructType([
        StructField("id", LongType(), True),
        StructField("eventType", StringType(), True),
        StructField("teamId", IntegerType(), True),
        StructField("playerId", LongType(), True),
        StructField("playerName", StringType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("min", IntegerType(), True),
        StructField("minAdded", IntegerType(), True),
        StructField("isBlocked", BooleanType(), True),
        StructField("isOnTarget", BooleanType(), True),
        StructField("blockedX", DoubleType(), True),
        StructField("blockedY", DoubleType(), True),
        StructField("goalCrossedY", DoubleType(), True),
        StructField("goalCrossedZ", DoubleType(), True),
        StructField("expectedGoals", DoubleType(), True),
        StructField("expectedGoalsOnTarget", DoubleType(), True),
        StructField("shotType", StringType(), True),
        StructField("situation", StringType(), True),
        StructField("period", StringType(), True),
        StructField("isOwnGoal", BooleanType(), True),
        StructField("onGoalShot", StructType([
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("zoomRatio", DoubleType(), True)
        ]), True),
        StructField("isSavedOffLine", BooleanType(), True),
        StructField("isFromInsideBox", BooleanType(), True),
        StructField("keeperId", LongType(), True),
        StructField("teamColor", StringType(), True)
    ])
    
    # Schemas for playerStats
    player_stat_value_struct = StructType([
        StructField("value", DoubleType(), True),
        StructField("total", DoubleType(), True),
        StructField("type", StringType(), True)
    ])

    player_stat_item_struct = StructType([
        StructField("key", StringType(), True),
        StructField("stat", player_stat_value_struct, True)
    ])

    player_stat_group_struct = StructType([
        StructField("title", StringType(), True),
        StructField("key", StringType(), True),
        StructField("stats", MapType(StringType(), player_stat_item_struct), True)
    ])
    
    fun_fact_input_value_struct = StructType([
        StructField("type", StringType(), True),
        StructField("value", StringType(), True) 
    ])

    fun_fact_struct = StructType([
        StructField("key", StringType(), True),
        StructField("fallback", StringType(), True),
        StructField("inputValues", ArrayType(fun_fact_input_value_struct), True)
    ])

    player_stats_main_struct = StructType([
        StructField("name", StringType(), True),
        StructField("id", LongType(), True),
        StructField("optaId", StringType(), True),
        StructField("teamId", IntegerType(), True),
        StructField("teamName", StringType(), True),
        StructField("isGoalkeeper", BooleanType(), True),
        StructField("stats", ArrayType(player_stat_group_struct), True),
        StructField("shotmap", ArrayType(shot_struct), True),
        StructField("funFacts", ArrayType(fun_fact_struct), True)
    ])
        
    custom_schema = StructType([
        StructField("general", StructType([
            StructField("matchId", LongType(), True),
            StructField("matchName", StringType(), True),
            StructField("matchRound", StringType(), True),
            StructField("teamColors", StructType([
                StructField("darkMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("lightMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("fontDarkMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("fontLightMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True)
            ]), True),
            StructField("leagueId", IntegerType(), True),
            StructField("leagueName", StringType(), True),
            StructField("leagueRoundName", StringType(), True),
            StructField("parentLeagueId", IntegerType(), True),
            StructField("countryCode", StringType(), True),
            StructField("parentLeagueName", StringType(), True),
            StructField("parentLeagueSeason", StringType(), True),
            StructField("parentLeagueTopScorerLink", StringType(), True),
            StructField("parentLeagueTournamentId", IntegerType(), True),
            StructField("homeTeam", StructType([
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), True)
            ]), True),
            StructField("awayTeam", StructType([
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), True)
            ]), True),
            StructField("coverageLevel", StringType(), True),
            StructField("matchTimeUTC", StringType(), True), # Consider TimestampType() if conversion is needed
            StructField("matchTimeUTCDate", StringType(), True), # Consider TimestampType() if conversion is needed
            StructField("started", BooleanType(), True),
            StructField("finished", BooleanType(), True)
        ]), True),
        StructField("header", StructType([
            StructField("teams", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("score", IntegerType(), True),
                    StructField("imageUrl", StringType(), True),
                    StructField("pageUrl", StringType(), True),
                    StructField("fifaRank", IntegerType(), True) # Assuming Integer, adjust if needed (it's null in data)
                ])
            ), True),
            StructField("status", StructType([
                StructField("utcTime", StringType(), True), # Consider TimestampType()
                StructField("numberOfHomeRedCards", IntegerType(), True),
                StructField("numberOfAwayRedCards", IntegerType(), True),
                StructField("halfs", StructType([
                    StructField("firstHalfStarted", StringType(), True), # Consider TimestampType()
                    StructField("firstHalfEnded", StringType(), True), # Consider TimestampType()
                    StructField("secondHalfStarted", StringType(), True), # Consider TimestampType()
                    StructField("secondHalfEnded", StringType(), True), # Consider TimestampType()
                    StructField("firstExtraHalfStarted", StringType(), True),
                    StructField("secondExtraHalfStarted", StringType(), True),
                    StructField("gameEnded", StringType(), True) # Consider TimestampType()
                ]), True),
                StructField("finished", BooleanType(), True),
                StructField("started", BooleanType(), True),
                StructField("cancelled", BooleanType(), True),
                StructField("awarded", BooleanType(), True),
                StructField("scoreStr", StringType(), True),
                StructField("reason", StructType([
                    StructField("short", StringType(), True),
                    StructField("shortKey", StringType(), True),
                    StructField("long", StringType(), True),
                    StructField("longKey", StringType(), True)
                ]), True),
                StructField("whoLostOnPenalties", StringType(), True), # Type could vary, adjust if needed
                StructField("whoLostOnAggregated", StringType(), True)
            ]), True),
            StructField("events", StructType([
                # Using MapType for dynamic player names as keys
                StructField("homeTeamGoals", MapType(StringType(), ArrayType(
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data, keep as String or choose specific type
                        StructField("eventId", LongType(), True), # Using LongType for potentially large IDs
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True), # Boolean in data
                        StructField("isHome", BooleanType(), True),
                        StructField("ownGoal", StringType(), True), # Null in data
                        StructField("goalDescription", StringType(), True), # Null in data
                        StructField("goalDescriptionKey", StringType(), True), # Null in data
                        StructField("suffix", StringType(), True), # Null in data
                        StructField("suffixKey", StringType(), True), # Null in data
                        StructField("isPenaltyShootoutEvent", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("newScore", ArrayType(IntegerType()), True),
                        StructField("penShootoutScore", StringType(), True), # Null in data
                        StructField("shotmapEvent", StringType(), True), # Null in data
                        StructField("assistStr", StringType(), True), # Can be null
                        StructField("assistProfileUrl", StringType(), True), # Can be null
                        StructField("assistPlayerId", LongType(), True), # Can be null, using LongType
                        StructField("assistKey", StringType(), True), # Can be null
                        StructField("assistInput", StringType(), True) # Can be null
                    ])
                )), True),
                StructField("awayTeamGoals", MapType(StringType(), ArrayType( # Similar structure to homeTeamGoals
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data
                        StructField("eventId", LongType(), True),
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True),
                        StructField("isHome", BooleanType(), True),
                        StructField("ownGoal", StringType(), True),
                        StructField("goalDescription", StringType(), True),
                        StructField("goalDescriptionKey", StringType(), True),
                        StructField("suffix", StringType(), True),
                        StructField("suffixKey", StringType(), True),
                        StructField("isPenaltyShootoutEvent", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("newScore", ArrayType(IntegerType()), True),
                        StructField("penShootoutScore", StringType(), True),
                        StructField("shotmapEvent", StringType(), True),
                        StructField("assistStr", StringType(), True),
                        StructField("assistProfileUrl", StringType(), True),
                        StructField("assistPlayerId", LongType(), True),
                        StructField("assistKey", StringType(), True),
                        StructField("assistInput", StringType(), True)
                    ])
                )), True),
                StructField("homeTeamRedCards", MapType(StringType(), ArrayType(StringType())), True), # Assuming simple structure based on empty data
                StructField("awayTeamRedCards", MapType(StringType(), ArrayType(
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data
                        StructField("eventId", LongType(), True),
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True),
                        StructField("isHome", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("card", StringType(), True),
                        StructField("cardDescription", StringType(), True) # Null in data
                    ])
                )), True)
            ]), True)
        ]), True),
        # Additional top-level fields
        StructField("nav", ArrayType(StringType()), True),
        StructField("ongoing", BooleanType(), True),
        StructField("hasPendingVAR", BooleanType(), True),
        StructField("content", StructType([
            StructField("matchFacts", StructType([
                StructField("matchId", LongType(), True),
                StructField("highlights", StringType(), True), # Null in data
                StructField("playerOfTheMatch", StructType([
                    StructField("id", LongType(), True),
                    StructField("name", StringType(), True)
                ]), True), # Empty object in data
                StructField("matchesInRound", ArrayType(
                    StructType([
                        StructField("id", StringType(), True),
                        StructField("utcTime", StringType(), True), # Consider TimestampType()
                        StructField("roundId", StringType(), True),
                        StructField("roundName", StringType(), True),
                        StructField("status", StructType([
                            StructField("utcTime", StringType(), True), # Consider TimestampType()
                            StructField("finished", BooleanType(), True),
                            StructField("started", BooleanType(), True),
                            StructField("cancelled", BooleanType(), True),
                            StructField("awarded", BooleanType(), True),
                            StructField("scoreStr", StringType(), True),
                            StructField("reason", StructType([
                                StructField("short", StringType(), True),
                                StructField("shortKey", StringType(), True),
                                StructField("long", StringType(), True),
                                StructField("longKey", StringType(), True)
                            ]), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("home", StructType([
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("shortName", StringType(), True)
                        ]), True),
                        StructField("away", StructType([
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("shortName", StringType(), True)
                        ]), True),
                        StructField("league", StructType([
                            StructField("primaryLeagueId", IntegerType(), True),
                            StructField("leagueId", IntegerType(), True),
                            StructField("leagueName", StringType(), True),
                            StructField("parentLeagueId", IntegerType(), True),
                            StructField("gender", StringType(), True),
                            StructField("stageId", IntegerType(), True),
                            StructField("tournamentId", IntegerType(), True),
                            StructField("isCup", BooleanType(), True),
                            StructField("countryCode", StringType(), True)
                        ]), True)
                    ])
                ), True),
                StructField("events", StructType([ # Note: Duplicates structure from header.events
                    StructField("ongoing", BooleanType(), True),
                    StructField("events", ArrayType( # This events array contains various event types
                        StructType([ # Generic event structure - needs careful handling or flattening
                            StructField("reactKey", StringType(), True),
                            StructField("timeStr", IntegerType(), True), # Mixed types (string/int), using Int, might need cast
                            StructField("type", StringType(), True),
                            StructField("time", IntegerType(), True),
                            StructField("overloadTime", IntegerType(), True), # Mixed types (string/int), using Int
                            StructField("eventId", LongType(), True), # Can be null
                            StructField("player", StructType([
                                StructField("id", LongType(), True), # Can be null
                                StructField("name", StringType(), True), # Can be null
                                StructField("profileUrl", StringType(), True)
                            ]), True),
                            StructField("homeScore", IntegerType(), True),
                            StructField("awayScore", IntegerType(), True),
                            StructField("profileUrl", StringType(), True), # Can be null
                            StructField("overloadTimeStr", BooleanType(), True),
                            StructField("isHome", BooleanType(), True),
                            StructField("ownGoal", StringType(), True), # Can be null
                            StructField("goalDescription", StringType(), True), # Can be null
                            StructField("goalDescriptionKey", StringType(), True), # Can be null
                            StructField("suffix", StringType(), True), # Can be null
                            StructField("suffixKey", StringType(), True), # Can be null
                            StructField("isPenaltyShootoutEvent", BooleanType(), True),
                            StructField("nameStr", StringType(), True), # Can be null
                            StructField("firstName", StringType(), True), # Can be null
                            StructField("lastName", StringType(), True), # Can be null
                            StructField("fullName", StringType(), True), # Can be null
                            StructField("playerId", LongType(), True), # Can be null
                            StructField("newScore", ArrayType(IntegerType()), True), # Can be null
                            StructField("penShootoutScore", StringType(), True), # Can be null
                            StructField("shotmapEvent", StringType(), True), # Can be null
                            StructField("assistStr", StringType(), True), # Can be null
                            StructField("assistProfileUrl", StringType(), True), # Can be null
                            StructField("assistPlayerId", LongType(), True), # Can be null
                            StructField("assistKey", StringType(), True), # Can be null
                            StructField("assistInput", StringType(), True), # Can be null
                            StructField("card", StringType(), True), # Can be null
                            StructField("cardDescription", StringType(), True), # Can be null
                            StructField("minutesAddedStr", StringType(), True), # Can be null
                            StructField("minutesAddedKey", StringType(), True), # Can be null
                            StructField("minutesAddedInput", IntegerType(), True), # Can be null
                            StructField("halfStrShort", StringType(), True), # Can be null
                            StructField("halfStrKey", StringType(), True), # Can be null
                            StructField("injuredPlayerOut", BooleanType(), True), # Can be null
                            StructField("swap", ArrayType( # Can be null
                                StructType([
                                    StructField("name", StringType(), True),
                                    StructField("id", StringType(), True),
                                    StructField("profileUrl", StringType(), True)
                                ])
                            ), True)
                        ])
                    ), True),
                    StructField("eventTypes", ArrayType(StringType()), True),
                    StructField("penaltyShootoutEvents", StringType(), True) # Null in data
                ]), True),
                StructField("infoBox", StructType([
                    StructField("legInfo", StringType(), True), # Null in data
                    StructField("Match Date", StructType([ # Field name has space
                        StructField("utcTime", StringType(), True), # Consider TimestampType()
                        StructField("isDateCorrect", BooleanType(), True)
                    ]), True),
                    StructField("Tournament", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("parentLeagueId", IntegerType(), True),
                        StructField("link", StringType(), True),
                        StructField("leagueName", StringType(), True),
                        StructField("roundName", StringType(), True),
                        StructField("round", StringType(), True),
                        StructField("selectedSeason", StringType(), True),
                        StructField("isCurrentSeason", BooleanType(), True)
                    ]), True),
                    StructField("Stadium", StructType([
                        StructField("name", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("lat", FloatType(), True),
                        StructField("long", FloatType(), True)
                    ]), True),
                    StructField("Referee", StructType([
                        StructField("imgUrl", StringType(), True),
                        StructField("text", StringType(), True),
                        StructField("country", StringType(), True)
                    ]), True),
                    StructField("Attendance", IntegerType(), True)
                ]), True),
                StructField("teamForm", ArrayType( # Array of arrays
                    ArrayType(
                        StructType([
                            StructField("result", IntegerType(), True),
                            StructField("resultString", StringType(), True),
                            StructField("imageUrl", StringType(), True),
                            StructField("linkToMatch", StringType(), True),
                            StructField("date", StructType([
                                StructField("utcTime", StringType(), True) # Consider TimestampType()
                            ]), True),
                            StructField("teamPageUrl", StringType(), True),
                            StructField("tooltipText", StructType([
                                StructField("utcTime", StringType(), True), # Consider TimestampType()
                                StructField("homeTeam", StringType(), True),
                                StructField("homeTeamId", IntegerType(), True),
                                StructField("homeScore", StringType(), True),
                                StructField("awayTeam", StringType(), True),
                                StructField("awayTeamId", IntegerType(), True),
                                StructField("awayScore", StringType(), True)
                            ]), True),
                            StructField("score", StringType(), True),
                            StructField("home", StructType([
                                StructField("id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("isOurTeam", BooleanType(), True)
                            ]), True),
                            StructField("away", StructType([
                                StructField("id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("isOurTeam", BooleanType(), True)
                            ]), True)
                        ])
                    )
                ), True),
                StructField("poll", StructType([
                    StructField("renderToTop", BooleanType(), True)
                ]), True),
                StructField("topPlayers", StructType([
                    StructField("homeTopPlayers", ArrayType(StringType()), True), # Assuming string based on empty data
                    StructField("awayTopPlayers", ArrayType(StringType()), True) # Assuming string based on empty data
                ]), True),
                StructField("countryCode", StringType(), True),
                StructField("QAData", ArrayType(
                    StructType([
                        StructField("question", StringType(), True),
                        StructField("answer", StringType(), True)
                    ])
                ), True)
            ]), True),
            StructField("liveticker", StructType([
                StructField("langs", StringType(), True),
                StructField("teams", ArrayType(StringType()), True),
                StructField("matches", ArrayType(
                    StructType([
                        StructField("time", StructType([
                            StructField("utcTime", StringType(), True) # Consider TimestampType()
                        ]), True)
                    ])
                ), True)
            ]), True),
            StructField("superlive", StructType([
                StructField("superLiveUrl", StringType(), True), # Null in data
                StructField("showSuperLive", BooleanType(), True)
            ]), True),
            StructField("buzz", StringType(), True), # Null in data
            StructField("playerStats", MapType(StringType(), player_stats_main_struct), True),
            StructField("shotmap", StructType([
                StructField("shots", ArrayType(shot_struct), True),
                StructField("Periods", StructType([
                    StructField("All", ArrayType(shot_struct), True)
                ]), True)
            ]), True),
            StructField("lineup", StructType([
                StructField("matchId", LongType(), True),
                StructField("lineupType", StringType(), True),
                StructField("availableFilters", ArrayType(StringType()), True),
                StructField("homeTeam", StructType([ # Detailed lineup structure
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("formation", StringType(), True),
                    StructField("starters", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True), # String in data
                            StructField("isCaptain", BooleanType(), True),
                            StructField("horizontalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("verticalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("time", IntegerType(), True)
                                    ])
                                ), True),
                                StructField("substitutionEvents", ArrayType( # Empty in example for some players
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("coach", StructType([
                        StructField("id", LongType(), True),
                        StructField("name", StringType(), True),
                        StructField("usualPlayingPositionId", StringType(), True), # Null in data
                        StructField("primaryTeamName", StringType(), True),
                        StructField("performance", StructType([
                            StructField("events", ArrayType(StringType()), True) # Assuming string based on empty data
                        ]), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("primaryTeamId", IntegerType(), True),
                        StructField("isCoach", BooleanType(), True)
                    ]), True),
                    StructField("subs", ArrayType( # Similar structure to starters but simpler
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True), # String in data
                            StructField("isCaptain", BooleanType(), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(StringType()), True), # Assuming string
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("unavailable", ArrayType(StringType()), True) # Assuming string based on empty data
                ]), True),
                StructField("awayTeam", StructType([ # Mirrors homeTeam structure
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("formation", StringType(), True),
                    StructField("starters", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True),
                            StructField("isCaptain", BooleanType(), True),
                            StructField("horizontalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("verticalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("time", IntegerType(), True)
                                    ])
                                ), True),
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("coach", StructType([
                        StructField("id", LongType(), True),
                        StructField("name", StringType(), True),
                        StructField("usualPlayingPositionId", StringType(), True),
                        StructField("primaryTeamName", StringType(), True),
                        StructField("performance", StructType([
                            StructField("events", ArrayType(StringType()), True)
                        ]), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("primaryTeamId", IntegerType(), True),
                        StructField("isCoach", BooleanType(), True)
                    ]), True),
                    StructField("subs", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True),
                            StructField("isCaptain", BooleanType(), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(StringType()), True),
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("unavailable", ArrayType(StringType()), True)
                ]), True)
            ]), True),
            StructField("playoff", BooleanType(), True),
            StructField("table", StructType([
                StructField("leagueId", StringType(), True),
                StructField("url", StringType(), True),
                StructField("teams", ArrayType(IntegerType()), True),
                StructField("tournamentNameForUrl", StringType(), True),
                StructField("parentLeagueId", IntegerType(), True),
                StructField("parentLeagueName", StringType(), True),
                StructField("isCurrentSeason", BooleanType(), True),
                StructField("parentLeagueSeason", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True),
            StructField("h2h", StructType([
                StructField("summary", ArrayType(IntegerType()), True),
                StructField("matches", ArrayType(
                    StructType([
                        StructField("time", StructType([
                            StructField("utcTime", StringType(), True) # Consider TimestampType()
                        ]), True),
                        StructField("matchUrl", StringType(), True),
                        StructField("league", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("pageUrl", StringType(), True)
                        ]), True),
                        StructField("home", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True)
                        ]), True),
                        StructField("status", StructType([ # Similar to header.status
                            StructField("utcTime", StringType(), True), # Consider TimestampType()
                            StructField("started", BooleanType(), True),
                            StructField("cancelled", BooleanType(), True),
                            StructField("finished", BooleanType(), True),
                            StructField("awarded", BooleanType(), True), # Added based on other status objects
                            StructField("scoreStr", StringType(), True), # Added based on other status objects
                            StructField("reason", StructType([ # Added based on other status objects
                                StructField("short", StringType(), True),
                                StructField("shortKey", StringType(), True),
                                StructField("long", StringType(), True),
                                StructField("longKey", StringType(), True)
                            ]), True)
                        ]), True),
                        StructField("finished", BooleanType(), True), # Note: appears inside and outside status
                        StructField("away", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True)
                        ]), True)
                    ])
                ), True)
            ]), True),
            StructField("momentum", StringType(), True)
        ]), True),
        StructField("seo", StructType([
            StructField("path", StringType(), True),
            StructField("eventJSONLD", StructType([ # Contains nested JSON-LD schema definitions
                StructField("@context", StringType(), True),
                StructField("@type", StringType(), True),
                StructField("sport", StringType(), True),
                StructField("homeTeam", StructType([
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("sport", StringType(), True),
                    StructField("logo", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("location", StringType(), True), # Null in data
                    StructField("memberOf", StringType(), True) # Null in data
                ]), True),
                StructField("awayTeam", StructType([ # Mirrors homeTeam structure
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("sport", StringType(), True),
                    StructField("logo", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("memberOf", StringType(), True)
                ]), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("startDate", StringType(), True), # Consider TimestampType()
                StructField("endDate", StringType(), True), # Consider TimestampType()
                StructField("eventStatus", StringType(), True),
                StructField("eventAttendanceMode", StringType(), True),
                StructField("location", StructType([
                    StructField("@type", StringType(), True),
                    StructField("url", StringType(), True)
                ]), True),
                StructField("image", ArrayType(StringType()), True),
                StructField("organizer", StructType([
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("logo", StringType(), True)
                ]), True),
                StructField("offers", StructType([
                    StructField("@type", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("availability", StringType(), True),
                    StructField("price", StringType(), True), # String "0" in data
                    StructField("priceCurrency", StringType(), True),
                    StructField("validFrom", StringType(), True) # Consider TimestampType()
                ]), True),
                StructField("performer", ArrayType(
                    StructType([
                        StructField("@type", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("url", StringType(), True)
                    ])
                ), True)
            ]), True),
            StructField("breadcrumbJSONLD", ArrayType(
                StructType([
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("itemListElement", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("name", StringType(), True),
                            StructField("item", StringType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("faqJSONLD", StructType([
                StructField("@context", StringType(), True),
                StructField("@type", StringType(), True),
                StructField("mainEntity", ArrayType(
                    StructType([
                        StructField("@type", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("acceptedAnswer", StructType([
                            StructField("@type", StringType(), True),
                            StructField("text", StringType(), True)
                        ]), True)
                    ])
                ), True)
            ]), True)
        ]), True)
    ])

    return custom_schema