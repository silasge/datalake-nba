from time import sleep

import pandas as pd
from nba_api.stats.endpoints import ShotChartDetail
from prefect import flow, task
from prefect.tasks import task_input_hash

from datalake_nba.etl_utils import generate_hash_id
from datalake_nba.db_utils import insert_from_pandas


@task(
    retries=10,
    retry_delay_seconds=60,
    retry_jitter_factor=1,
    cache_key_fn=task_input_hash,
)
def get_shot_chart_detail(
    season_year: str, team_id: str, player_id: str
) -> pd.DataFrame:
    shot_chart = ShotChartDetail(
        season_nullable=season_year,
        team_id=team_id,
        player_id=player_id,
        context_measure_simple="FGA",
    ).get_data_frames()[0]

    return shot_chart


@task(cache_key_fn=task_input_hash)
def create_hash_id_shot_chart_detail(shot_chart: pd.DataFrame) -> pd.DataFrame:
    keys = ["GAME_ID", "TEAM_ID", "PLAYER_ID", "GAME_EVENT_ID"]
    shot_chart["HASH_ID_ACTION"] = generate_hash_id(df=shot_chart, keys=keys)

    reordered_cols = ["HASH_ID_ACTION"] + [
        col for col in shot_chart.columns if col != "HASH_ID_ACTION"
    ]

    return shot_chart.loc[:, reordered_cols]


@task(cache_key_fn=task_input_hash)
def process_types_shot_chart_detail(shot_chart: pd.DataFrame) -> pd.DataFrame:
    for key in ["GAME_ID", "TEAM_ID", "PLAYER_ID"]:
        if key in shot_chart.columns:
            shot_chart[key] = shot_chart[key].astype(str)
    return shot_chart


@flow
def insert_shot_chart_detail(
    season_year: str, team_id: str, player_id: str, sleep_secs=0.5
) -> None:
    try:
        shot_chart = get_shot_chart_detail(
            season_year=season_year, team_id=team_id, player_id=player_id
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
