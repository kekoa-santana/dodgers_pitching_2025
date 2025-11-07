import requests
import pandas as pd
from sqlalchemy import create_engine

from utils.utils import build_db_url

engine = create_engine(build_db_url(), pool_pre_ping=True)

def _fetch_game_pks() -> []:
    game_pks = []

    schedule_url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId=119&startDate=2025-03-18&endDate=2025-11-02"
    response = requests.get(schedule_url)
    data = response.json() 

    schedule = data.get("dates") or {}
    for day in schedule:
        games = day.get("games") or []
        for game in games:
            if game.get("gameType", " ").lower() != "s":
                game_pk = game.get("gamePk")
                game_pks.append(game_pk)

    return game_pks

def fetch_boxscores(game_pks: [], desired_team_id: int) -> []:
    boxscore_base_url = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
    boxscore_rows = []
    row = 0

    for game_pk in game_pks:
        url = boxscore_base_url.format(game_pk)
        response = requests.get(url)
        data = response.json()

        teams = data.get("teams") or {}
        for side in ['away', 'home']:
            s = teams.get(side) or {}
            team_data = s.get("team") or {}
            player_data = s.get("players") or {}

            team_id = team_data.get("id")
            if team_id == desired_team_id:
                for player_idx, player in player_data.items():
                    player_stats = player.get("stats") or {}
                    person_data = player.get("person") or {}

                    pitching_stats = player_stats.get("pitching") or {}

                    if pitching_stats:
                        boxscore_rows.append({
                            "row_num": row,
                            "pitcher_id": person_data.get("id"),
                            "pitcher_name": person_data.get("fullName"),
                            "game_pk": game_pk,
                            "team_id": team_id,
                            "team_name": team_data.get("name"),
                            "is_starter": pitching_stats.get("gamesStarted"),
                            "fly_outs": pitching_stats.get("flyOuts"),
                            "ground_outs": pitching_stats.get("groundOuts"),
                            "air_outs": pitching_stats.get("airOuts"),
                            "runs": pitching_stats.get("runs"),
                            "doubles": pitching_stats.get("doubles"),
                            "triples": pitching_stats.get("triples"),
                            "home_runs": pitching_stats.get("homeRuns"),
                            "strike_outs": pitching_stats.get("strikeOuts"),
                            "walks": pitching_stats.get("baseOnBalls"),
                            "intentional_walks": pitching_stats.get("intentionalWalks"),
                            "hits": pitching_stats.get("hits"),
                            "hit_by_pitch": pitching_stats.get("hitByPitch"),
                            "at_bats": pitching_stats.get("atBats"),
                            "caught_stealing": pitching_stats.get("caughtStealing"),
                            "stolen_bases": pitching_stats.get("stolenBases"),
                            "stolen_base_percentage": pitching_stats.get("stolenBasePercentage"),
                            "number_of_pitches": pitching_stats.get("numberOfPitches"),
                            "innings_pitched": pitching_stats.get("inningsPitched"),
                            "wins": pitching_stats.get("wins"),
                            "losses": pitching_stats.get("losses"),
                            "saves": pitching_stats.get("saves"),
                            "save_opportunities": pitching_stats.get("saveOpportunities"),
                            "holds": pitching_stats.get("holds"),
                            "blown_saves": pitching_stats.get("blownSaves"),
                            "earned_runs": pitching_stats.get("earnedRuns"),
                            "batters_faced": pitching_stats.get("battersFaced"),
                            "outs": pitching_stats.get("outs"),
                            "complete_game": pitching_stats.get("completeGames"),
                            "shutout": pitching_stats.get("shutouts"),
                            "pitches_thrown": pitching_stats.get("pitchesThrown"),
                            "balls": pitching_stats.get("balls"),
                            "strikes": pitching_stats.get("strikes"),
                            "strike_percentage": pitching_stats.get("strikePercentage"),
                            "hit_batsmen": pitching_stats.get("hitBatsmen"),
                            "balks": pitching_stats.get("balks"),
                            "wild_pitches": pitching_stats.get("wildPitches"),
                            "pickoffs": pitching_stats.get("pickoffs"),
                            "rbi": pitching_stats.get("rbi"),
                            "games_finished": pitching_stats.get("gamesFinished"),
                            "runs_scored_per_9": pitching_stats.get("runsScoredPer9"),
                            "home_runs_per_9": pitching_stats.get("homeRunsPer9"),
                            "inherited_runners": pitching_stats.get("inheritedRunners"),
                            "inherited_runners_scored": pitching_stats.get("inheritedRunnersScored"),
                            "catchers_interference": pitching_stats.get("catchersInterference"),
                            "sac_bunts": pitching_stats.get("sacBunts"),
                            "sac_flies": pitching_stats.get("sacFlies"),
                            "passed_ball": pitching_stats.get("passedBall"),
                            "pop_outs": pitching_stats.get("popOuts"),
                            "lineouts": pitching_stats.get("lineOuts"),
                            "source": "MLB_stats_api"
                        })

                        row += 1
    return boxscore_rows

def load_to_psql(df: pd.DataFrame):
    with engine.begin() as conn:
        df.to_sql(
            'pitching_boxscores',
            conn,
            schema = 'raw',
            if_exists = 'replace',
            index = False
        )

def main():
    print("fetching game_pks")
    game_pks = _fetch_game_pks()
    rows = fetch_boxscores(game_pks, 119)

    df = pd.DataFrame(rows)
    load_to_psql(df)

if __name__ == "__main__":
    main()