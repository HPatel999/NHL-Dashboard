"""
NHL Team and Player Data Scraper

Fetches up-to-date team standings, player rosters, and player stats from the NHL public API
and exports them to CSV files for Power BI or analytics.
"""

import pandas as pd
import requests
import time
from typing import List, Dict, Optional, Tuple
import urllib.parse  # encoding the cayenne query
import time
import os

DATA_FOLDER = "data"  
SEASON_ID = "20252026"
SEASON_ID_OFFSEASON = "20242025"
BASE_URL = "https://api-web.nhle.com/v1"


def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data from a given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] Failed fetching {url}: {e}")
        return None



def fetch_teams() -> pd.DataFrame:
    """Fetch team standings, advanced team tracking stats, and weekly schedule."""
    url = f"{BASE_URL}/standings/now"
    data = fetch_json(url)
    if not data:
        return pd.DataFrame()

    teams = []

    for entry in data.get("standings", []):
        team_abbr = entry["teamAbbrev"]["default"]
        team_data = {
            "team_abbr": team_abbr,
            "team_name": entry["teamName"]["default"],
            "conference": entry["conferenceName"],
            "division": entry["divisionName"],
            "logo": entry["teamLogo"],

            # Basic record
            "wins": entry.get("wins"),
            "losses": entry.get("losses"),
            "ot_losses": entry.get("otLosses"),
            "games_played": entry.get("gamesPlayed"),
            "points": entry.get("points"),
            "point_pctg": entry.get("pointPctg"),
            "win_pctg": entry.get("winPctg"),
            "clinch": entry.get("clinchIndicator", ""),

            # Goals
            "goals_for": entry.get("goalFor"),
            "goals_against": entry.get("goalAgainst"),
            "goal_diff": entry.get("goalDifferential"),

            # Streaks
            "streak_type": entry.get("streakCode", ""),
            "streak_length": entry.get("streakCount"),

            # Regulation/Shootout
            "regulation_wins": entry.get("regulationWins"),
            "reg_ot_wins": entry.get("regulationPlusOtWins") - entry.get("regulationWins"),
            "shootout_wins": entry.get("shootoutWins"),
            "shootout_losses": entry.get("shootoutLosses"),

            # Home/Road splits
            "home_games_played" : entry.get("homeGamesPlayed"),
            "home_wins": entry.get("homeWins"),
            "home_losses": entry.get("homeLosses"),
            "home_ot_losses": entry.get("homeOtLosses"),
            "home_points": entry.get("homePoints"),
            "home_goals_for": entry.get("homeGoalsFor"),
            "home_goals_against": entry.get("homeGoalsAgainst"),
            "home_goal_diff": entry.get("homeGoalDifferential"),

            "road_games_played" : entry.get("roadGamesPlayed"),
            "road_wins": entry.get("roadWins"),
            "road_losses": entry.get("roadLosses"),
            "road_ot_losses": entry.get("roadOtLosses"),
            "road_points": entry.get("roadPoints"),
            "road_goals_for": entry.get("roadGoalsFor"),
            "road_goals_against": entry.get("roadGoalsAgainst"),
            "road_goal_diff": entry.get("roadGoalDifferential"),

            # Last 10 games
            "l10_wins": entry.get("l10Wins"),
            "l10_losses": entry.get("l10Losses"),
            "l10_ot_losses": entry.get("l10OtLosses"),
            "l10_points": entry.get("l10Points"),
            "l10_goals_for": entry.get("l10GoalsFor"),
            "l10_goals_against": entry.get("l10GoalsAgainst"),
            "l10_goal_diff": entry.get("l10GoalDifferential"),
        }

        #Encode Cayenne expression
        cayenne_expr = urllib.parse.quote(f"teamAbbrev='{team_abbr}' and seasonId={SEASON_ID_OFFSEASON}")

        #Power Play %
        pp_url = f"https://api.nhle.com/stats/rest/en/team/powerplay?cayenneExp={cayenne_expr}"
        pp_data = fetch_json(pp_url)
        if pp_data and pp_data["data"]:
            pp_stats = pp_data["data"][0]
            team_data.update({
                "power_play_pct": pp_stats.get("powerPlayPct"),
                "power_play_goals": pp_stats.get("powerPlayGoalsFor"),
                "pp_goals_per_game": pp_stats.get("ppGoalsPerGame"),
                "pp_opportunities": pp_stats.get("ppOpportunities"),
                "sh_goals_against": pp_stats.get("shGoalsAgainst"),
                "pp_net_goals": pp_stats.get("ppNetGoals")
            })

        #Goals by Period
        gbp_url = f"https://api.nhle.com/stats/rest/en/team/goalsbyperiod?cayenneExp={cayenne_expr}"
        gbp = fetch_json(gbp_url)
        if gbp and gbp.get("data"):
            gbp_row = gbp["data"][0]
            team_data.update({
                "period1GoalsFor": gbp_row.get("period1GoalsFor"),
                "period2GoalsFor": gbp_row.get("period2GoalsFor"),
                "period3GoalsFor": gbp_row.get("period3GoalsFor"),
                "periodOtGoalsFor": gbp_row.get("periodOtGoalsFor"),
                "period1GoalsAgainst": gbp_row.get("period1GoalsAgainst"),
                "period2GoalsAgainst": gbp_row.get("period2GoalsAgainst"),
                "period3GoalsAgainst": gbp_row.get("period3GoalsAgainst"),
                "periodOtGoalsAgainst": gbp_row.get("periodOtGoalsAgainst"),
            })

 
        #Faceoff %
        foz_url = f"https://api.nhle.com/stats/rest/en/team/faceoffpercentages?cayenneExp={cayenne_expr}"
        foz = fetch_json(foz_url)
        if foz and foz.get("data"):
            foz_row = foz["data"][0]
            team_data.update({
                "faceoff_win_pct": foz_row.get("faceoffWinPct"),
                "defensive_zone_faceoff_pct": foz_row.get("defensiveZoneFaceoffPct"),
                "neutral_zone_faceoff_pct": foz_row.get("neutralZoneFaceoffPct"),
                "offensive_zone_faceoff_pct": foz_row.get("offensiveZoneFaceoffPct"),
                "pp_faceoff_pct": foz_row.get("ppFaceoffPct"),
                "sh_faceoff_pct": foz_row.get("shFaceoffPct"),
                "total_faceoffs": foz_row.get("totalFaceoffs"),
            })

        teams.append(team_data)

    return pd.DataFrame(teams)

def fetch_team_roster(team_abbr: str) -> List[Dict]:
    """Fetch the player roster for a given team."""
    url = f"{BASE_URL}/roster/{team_abbr.lower()}/{SEASON_ID}"
    data = fetch_json(url)
    if not data:
        return []

    return data.get("forwards", []) + data.get("defensemen", []) + data.get("goalies", [])


def fetch_player_stats(player_id: int, full_name: str, team_name: str, position: str) -> List[Dict]:
    """Fetch season and career stats for a player."""
    url = f"{BASE_URL}/player/{player_id}/landing"
    data = fetch_json(url)
    if not data:
        return []

    stats = []

    # Season totals
    for season in data.get("seasonTotals", []):
        game_type = season.get("gameTypeId")
        season_type = {2: "regular", 3: "playoffs"}.get(game_type, "other")

        season.update({
            "player_id": player_id,
            "player": full_name,
            "team": season.get("teamName"),
            "type": season_type,
            "season_scope": "season",
            "positionCode": position,
        })
        stats.append(season)

    # Career totals
    for key, label in [("regularSeason", "regular"), ("playoffs", "playoffs")]:
        career = data.get("careerTotals", {}).get(key)
        if career:
            career.update({
                "player_id": player_id,
                "player": full_name,
                "team": team_name,
                "type": label,
                "season_scope": "career",
                "positionCode": position,
            })
            stats.append(career)

    return stats


def build_rosters_and_stats(teams_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Loop through teams, gather rosters and stats."""
    rosters = []
    all_stats = []

    for _, team in teams_df.iterrows():
        team_abbr = team["team_abbr"]
        team_name = team["team_name"]
        print(f"[INFO] Fetching roster for {team_name}...")

        players = fetch_team_roster(team_abbr)

        for player in players:
            player_id = player["id"]
            full_name = f"{player['firstName']['default']} {player['lastName']['default']}"
            position = player["positionCode"]
            sweater = player.get("sweaterNumber", "N/A")
            mug = player.get("headshot")

            rosters.append({
                "player_id": player_id,
                "player": full_name,
                "positionCode": position,
                "sweaterNumber": sweater,
                "team": team_name,
                "team_abbr": team_abbr,
                "headshot": mug,
            })

            all_stats += fetch_player_stats(player_id, full_name, team_name, position)

        time.sleep(0.2)  # API problems

    return pd.DataFrame(rosters), pd.DataFrame(all_stats)


def export_to_csv(df: pd.DataFrame, filename: str) -> None:
    """Save a DataFrame to CSV with row count logging."""
    df.to_csv(filename, index=False)
    print(f"[✓] Exported {filename} ({len(df)} rows)")

def main():
    print("[🏒] Starting NHL Data Fetch...")

    teams_df = fetch_teams()
    if teams_df.empty:
        print("[ERROR] No teams returned.")
        return

    rosters_df, stats_df = build_rosters_and_stats(teams_df)

    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    export_to_csv(teams_df, os.path.join(DATA_FOLDER, "teams.csv"))
    export_to_csv(rosters_df, os.path.join(DATA_FOLDER, "rosters.csv"))
    export_to_csv(stats_df, os.path.join(DATA_FOLDER, "player_stats.csv"))

    print("Success!")


if __name__ == "__main__":
    main()
