"""
Library for parsing match JSON data for FotMob
"""

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