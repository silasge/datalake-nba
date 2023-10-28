import argparse
import sys

from tqdm import tqdm

from datalake_nba.config import (
    BRONZE_TABLES_PATH,
    GOLD_TABLES_PATH,  # noqa: F401
    SCHEMAS_PATH,
    SILVER_TABLES_PATH,  # noqa: F401
)
from datalake_nba.nba_bronze import (
    insert_box_scores,
    insert_playbyplay,
    insert_shot_chart_detail,
    insert_team_game_logs,
)
from datalake_nba.utils.db import (
    create_from_sql_file,
    get_distinct_game_ids,
    get_distinct_season_team_player_id,
)


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-sy", type=str)
    parser.add_argument("-st", type=str)
    parser.add_argument("-shotchartdetail", action=argparse.BooleanOptionalAction)
    return parser.parse_args(args)


def run_nba_bronze():
    args = parse_args(sys.argv[1:])
    # create schemas
    create_from_sql_file(sql_file=SCHEMAS_PATH)

    # create bronze tables
    create_from_sql_file(sql_file=BRONZE_TABLES_PATH)

    # insert team game logs
    insert_team_game_logs(season_year=args.sy, season_type=args.st)

    # get unique game_ids
    tables_to_check_for_unique_ids = [
        "teams_box_score_traditional",
        "players_box_score_traditional",
        "teams_box_score_advanced",
        "players_box_score_advanced",
        "playbyplayv3",
    ]

    distinct_game_ids = get_distinct_game_ids(
        tables=tables_to_check_for_unique_ids, season_year=args.sy, season_type=args.st
    )

    # insert box scores and playbyplay
    for game_id in (pbar := tqdm(distinct_game_ids)):
        pbar.set_description(f"Processing {game_id}")
        # box scores
        insert_box_scores(game_id=game_id)
        # playbyplay
        insert_playbyplay(game_id=game_id)

    if args.shotchartdetail:
        # shotchartdetail
        season_team_player_id = get_distinct_season_team_player_id(
            season_year=args.sy, season_type=args.st
        )

        for sy, st, tid, pid in (pbar := tqdm(season_team_player_id)):
            pbar.set_description(f"Processing {sy}-{st}-{tid}-{pid}")
            insert_shot_chart_detail(
                season_year=sy, season_type=st, team_id=tid, player_id=pid
            )
