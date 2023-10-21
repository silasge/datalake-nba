from time import sleep

import pandas as pd
from nba_api.stats.endpoints import PlayByPlayV3
from prefect import flow, task
from prefect.tasks import task_input_hash

from datalake_nba.etl_utils import generate_hash_id
from datalake_nba.db_utils import insert_from_pandas


@task(
    retries=10,
    retry_delay_seconds=60,
    retry_jitter_factor=1,
    cache_key_fn=task_input_hash,
    tags=["nba-api"],
)
def get_playbyplayv3(game_id: str) -> pd.DataFrame:
    playbyplay = PlayByPlayV3(game_id=game_id).get_data_frames()[0]
    return playbyplay


@task(cache_key_fn=task_input_hash)
def create_hash_id_playbyplay(playbyplay: pd.DataFrame) -> pd.DataFrame:
    keys = ["gameId", "teamId", "personId", "actionId"]
    playbyplay["HASH_ID_ACTION"] = generate_hash_id(df=playbyplay, keys=keys)

    reordered_cols = ["HASH_ID_ACTION"] + [
        col for col in playbyplay.columns if col != "HASH_ID_ACTION"
    ]

    return playbyplay.loc[:, reordered_cols]


@flow
def insert_playbyplay(game_id: str, sleeps_secs=1):
    try:
        playbyplay = get_playbyplayv3(game_id=game_id)
    except Exception as e:
        print(e)
    else:
        playbyplay = create_hash_id_playbyplay(playbyplay=playbyplay)
        insert_from_pandas(schema="nba_bronze", table="playbyplayv3", df=playbyplay)
        sleep(sleeps_secs)
