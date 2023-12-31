import pandas as pd
from nba_api.stats.endpoints import TeamGameLogs

from datalake_nba.utils.db import insert_from_pandas
from datalake_nba.utils.decorators import retry
from datalake_nba.utils.etl import generate_hash_id


@retry(retries=10, delay=60, jitter=10)
def get_team_game_logs(season_nullable: str, season_type_nullable: str) -> pd.DataFrame:
    team_game_logs = TeamGameLogs(
        season_nullable=season_nullable, season_type_nullable=season_type_nullable
    ).get_data_frames()[0]
    team_game_logs["SEASON_TYPE"] = season_type_nullable
    return team_game_logs


def pre_process_team_game_logs(team_game_logs: pd.DataFrame) -> pd.DataFrame:
    team_game_logs["GAME_DATE"] = pd.to_datetime(team_game_logs["GAME_DATE"]).apply(
        lambda x: x.strftime("%Y-%m-%d")
    )
    team_game_logs["HASH_ID_TEAM"] = generate_hash_id(
        team_game_logs, ["GAME_ID", "TEAM_ID"]
    )
    return team_game_logs.loc[
        :,
        [
            "HASH_ID_TEAM",
            "SEASON_YEAR",
            "SEASON_TYPE",
            "TEAM_ID",
            "TEAM_ABBREVIATION",
            "TEAM_NAME",
            "GAME_ID",
            "GAME_DATE",
            "MATCHUP",
            "WL",
            "MIN",
            "FGM",
            "FGA",
            "FG_PCT",
            "FG3M",
            "FG3A",
            "FG3_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "OREB",
            "DREB",
            "REB",
            "AST",
            "TOV",
            "STL",
            "BLK",
            "BLKA",
            "PF",
            "PFD",
            "PTS",
            "PLUS_MINUS",
        ],
    ]


def insert_team_game_logs(season_year: str, season_type: str) -> None:
    # get data
    team_game_logs = get_team_game_logs(
        season_nullable=season_year, season_type_nullable=season_type
    )

    # pre-process data
    team_game_logs = pre_process_team_game_logs(team_game_logs)

    # insert into db
    insert_from_pandas(schema="nba_bronze", table="team_game_logs", df=team_game_logs)
