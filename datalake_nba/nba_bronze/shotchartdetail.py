from time import sleep

import pandas as pd
from nba_api.stats.endpoints import ShotChartDetail

from datalake_nba.utils.db import insert_from_pandas
from datalake_nba.utils.decorators import retry
from datalake_nba.utils.etl import generate_hash_id


@retry(retries=10, delay=60, jitter=10)
def get_shot_chart_detail(
    season_year: str, season_type: str, team_id: str, player_id: str
) -> pd.DataFrame:
    shot_chart = ShotChartDetail(
        season_nullable=season_year,
        season_type_all_star=season_type,
        team_id=team_id,
        player_id=player_id,
        context_measure_simple="FGA",
    ).get_data_frames()[0]

    return shot_chart


def create_hash_id_shot_chart_detail(shot_chart: pd.DataFrame) -> pd.DataFrame:
    keys = ["GAME_ID", "TEAM_ID", "PLAYER_ID", "GAME_EVENT_ID"]
    shot_chart["HASH_ID_ACTION"] = generate_hash_id(df=shot_chart, keys=keys)

    reordered_cols = ["HASH_ID_ACTION"] + [
        col for col in shot_chart.columns if col != "HASH_ID_ACTION"
    ]

    return shot_chart.loc[:, reordered_cols]


def process_types_shot_chart_detail(shot_chart: pd.DataFrame) -> pd.DataFrame:
    for key in ["GAME_ID", "TEAM_ID", "PLAYER_ID"]:
        if key in shot_chart.columns:
            shot_chart[key] = shot_chart[key].astype(str)
    return shot_chart


def insert_shot_chart_detail(
    season_year: str, season_type: str, team_id: str, player_id: str, sleep_secs=0.5
) -> None:
    try:
        shot_chart = get_shot_chart_detail(
            season_year=season_year,
            season_type=season_type,
            team_id=team_id,
            player_id=player_id,
        )
    except Exception as e:
        print(e)
    else:
        if len(shot_chart) > 0:
            shot_chart = create_hash_id_shot_chart_detail(shot_chart=shot_chart)
            shot_chart = process_types_shot_chart_detail(shot_chart=shot_chart)
            insert_from_pandas(
                schema="nba_bronze",
                table="shotchartdetail",
                df=shot_chart,
            )
        sleep(sleep_secs)
