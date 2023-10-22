from time import sleep

import pandas as pd
from nba_api.stats.endpoints import BoxScoreTraditionalV2, BoxScoreAdvancedV2

from datalake_nba.etl_utils import generate_hash_id
from datalake_nba.db_utils import insert_from_pandas
from datalake_nba.general_utils import retry


@retry(retries=10, delay=60, jitter=10)
def get_box_score_traditional(game_id: str) -> pd.DataFrame:
    box_score_traditional = BoxScoreTraditionalV2(game_id=game_id).get_data_frames()
    return box_score_traditional


@retry(retries=10, delay=60, jitter=10)
def get_box_score_advanced(game_id: str) -> pd.DataFrame:
    box_score_advanced = BoxScoreAdvancedV2(game_id=game_id).get_data_frames()
    return box_score_advanced


def create_hash_id_box_scores(
    box_score: pd.DataFrame, is_players: bool = False
) -> pd.DataFrame:
    keys = ["GAME_ID", "TEAM_ID"]
    col_name = "HASH_ID_TEAM"

    if is_players:
        keys = keys + ["PLAYER_ID"]
        col_name = "HASH_ID_PLAYER"

    box_score[col_name] = generate_hash_id(box_score, keys)

    reordered_cols = [col_name] + [col for col in box_score.columns if col != col_name]

    return box_score.loc[:, reordered_cols]


def process_types_box_scores(box_score: pd.DataFrame) -> pd.DataFrame:
    for key in ["GAME_ID", "TEAM_ID", "PLAYER_ID"]:
        if key in box_score.columns:
            box_score[key] = box_score[key].astype(str)
    return box_score


def insert_box_scores(game_id: str, sleep_secs=0.5) -> None:
    try:
        # box score traditional
        box_score_traditional = get_box_score_traditional(game_id=game_id)
        # box score advanced
        box_score_advanced = get_box_score_advanced(game_id=game_id)
    except Exception as e:
        print(e)
    else:
        # teams traditional
        team_box_score_traditional = create_hash_id_box_scores(
            box_score=box_score_traditional[1], is_players=False
        )
        team_box_score_traditional = process_types_box_scores(
            box_score=team_box_score_traditional
        )
        insert_from_pandas(
            schema="nba_bronze",
            table="teams_box_score_traditional",
            df=team_box_score_traditional,  # teams table
        )
        # players traditional
        players_box_score_traditional = create_hash_id_box_scores(
            box_score=box_score_traditional[0], is_players=True
        )
        players_box_score_traditional = process_types_box_scores(
            box_score=players_box_score_traditional
        )
        insert_from_pandas(
            schema="nba_bronze",
            table="players_box_score_traditional",
            df=players_box_score_traditional,  # players table
        )
        # teams advanced
        team_box_score_advanced = create_hash_id_box_scores(
            box_score=box_score_advanced[1], is_players=False
        )
        team_box_score_advanced = process_types_box_scores(
            box_score=team_box_score_advanced
        )
        insert_from_pandas(
            schema="nba_bronze",
            table="teams_box_score_advanced",
            df=team_box_score_advanced,  # teams table
        )
        # players advanced
        players_box_score_advanced = create_hash_id_box_scores(
            box_score=box_score_advanced[0], is_players=True
        )
        players_box_score_advanced = process_types_box_scores(
            box_score=players_box_score_advanced
        )
        insert_from_pandas(
            schema="nba_bronze",
            table="players_box_score_advanced",
            df=players_box_score_advanced,  # players table
        )
        sleep(sleep_secs)
