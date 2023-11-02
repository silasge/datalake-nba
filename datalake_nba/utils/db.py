from pathlib import Path
from typing import List, Tuple

import duckdb
import pandas as pd

from datalake_nba.config import DUCKDB_PATH


def create_from_sql_file(sql_file: str | Path) -> None:
    conn = duckdb.connect(DUCKDB_PATH)
    # reading file
    with open(sql_file) as f:
        sql = f.read()

    # execute file and close connection
    conn.execute(sql)
    conn.close()


def insert_from_pandas(schema: str, table: str, df: pd.DataFrame) -> None:
    conn = duckdb.connect(DUCKDB_PATH)

    # copy df because duckdb can't recognize when the df comes
    # directly from an argument
    df_copy = df.copy()  # noqa: F841
    sql = f"""
    INSERT OR IGNORE INTO {schema}.{table}
    SELECT * FROM df_copy
    """

    # execute, commit and close connection
    conn.execute(sql)
    conn.commit()
    conn.close()


def get_distinct_game_ids(
    tables: List[str], season_year: str, season_type: str
) -> List[str]:
    conn = duckdb.connect(DUCKDB_PATH)

    def aux_distinct_game_ids(not_in_table: str, season_year: str, season_type: str):
        if not_in_table == "playbyplayv3":
            col = "gameId"
        else:
            col = "GAME_ID"

        subquery = f"SELECT DISTINCT {col} FROM nba_bronze.{not_in_table}"

        query = f"""
        SELECT DISTINCT
            GAME_ID
        FROM
            nba_bronze.team_game_logs
        WHERE
            SEASON_YEAR = '{season_year}'
            AND SEASON_TYPE = '{season_type}'
            AND GAME_ID NOT IN ({subquery});
        """

        results = conn.execute(query=query).fetchall()
        game_ids = [game_id[0] for game_id in results]
        return game_ids

    distinct_game_ids = []

    for tb in tables:
        distinct_game_ids += aux_distinct_game_ids(
            not_in_table=tb, season_year=season_year, season_type=season_type
        )
    conn.close()

    return list(set(distinct_game_ids))


def get_distinct_season_team_player_id(
    season_year: str, season_type: str
) -> List[Tuple]:
    conn = duckdb.connect(DUCKDB_PATH)

    query = f"""
    WITH
    distincts_gtpid_shot AS (
    	SELECT DISTINCT 
    		GAME_ID,
    		TEAM_ID,
    		PLAYER_ID
    	FROM 
    		nba_bronze.shotchartdetail 
    ),
    distincts_gtpid AS (
    	SELECT DISTINCT
    	    b.SEASON_YEAR,
    	    b.SEASON_TYPE,
    	    a.GAME_ID,
    	    a.TEAM_ID,
    	    a.PLAYER_ID
    	FROM
    	    nba_bronze.players_box_score_traditional AS a
    	INNER JOIN
    	    nba_bronze.team_game_logs AS b
    	    ON a.GAME_ID = b.GAME_ID
    	ANTI JOIN 
    		distincts_gtpid_shot AS c
    		ON a.GAME_ID = c.GAME_ID 
    		AND a.TEAM_ID = c.TEAM_ID 
    		AND a.PLAYER_ID = c.PLAYER_ID
    	WHERE
    	    b.SEASON_YEAR = '{season_year}'
    	    AND b.SEASON_TYPE = '{season_type}'
    	    AND a.FGA > 0
    )
    SELECT *
    FROM distincts_gtpid
    """

    results = conn.execute(query).fetchall()
    return results
