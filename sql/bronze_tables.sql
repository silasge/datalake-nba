CREATE TABLE IF NOT EXISTS nba_bronze.team_game_logs (
    HASH_ID_TEAM VARCHAR PRIMARY KEY,
    SEASON_YEAR VARCHAR,
    SEASON_TYPE VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_ABBREVIATION VARCHAR,
    TEAM_NAME VARCHAR,
    GAME_ID VARCHAR,
    GAME_DATE DATE,
    MATCHUP VARCHAR,
    WL VARCHAR,
    MIN VARCHAR,
    FGM INTEGER,
    FGA INTEGER,
    FG_PCT FLOAT,
    FG3M INTEGER,
    FG3A INTEGER,
    FG3_PCT FLOAT,
    FTM INTEGER,
    FTA INTEGER,
    FT_PCT FLOAT,
    OREB INTEGER,
    DREB INTEGER,
    REB INTEGER,
    AST INTEGER,
    TOV INTEGER,
    STL INTEGER,
    BLK INTEGER,
    BLKA INTEGER,
    PF INTEGER,
    PFD INTEGER,
    PTS INTEGER,
    PLUS_MINUS FLOAT
);


CREATE TABLE IF NOT EXISTS nba_bronze.teams_box_score_traditional (
    HASH_ID_TEAM VARCHAR PRIMARY KEY,
    GAME_ID VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_NAME VARCHAR,
    TEAM_ABBREVIATION VARCHAR,
    TEAM_CITY VARCHAR,
    MIN VARCHAR,
    FGM INTEGER,
    FGA INTEGER,
    FG_PCT FLOAT,
    FG3M INTEGER,
    FG3A INTEGER,
    FG3_PCT FLOAT,
    FTM INTEGER,
    FTA INTEGER,
    FT_PCT FLOAT,
    OREB INTEGER,
    DREB INTEGER,
    REB INTEGER,
    AST INTEGER,
    STL INTEGER,
    BLK INTEGER,
    "TO" INTEGER,
    PF INTEGER,
    PTS INTEGER,
    PLUS_MINUS INTEGER
);


CREATE TABLE IF NOT EXISTS nba_bronze.players_box_score_traditional (
    HASH_ID_PLAYER VARCHAR PRIMARY KEY,
    GAME_ID VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_ABBREVIATION VARCHAR,
    TEAM_CITY VARCHAR,
    PLAYER_ID VARCHAR,
    PLAYER_NAME VARCHAR,
    NICKNAME VARCHAR,
    START_POSITION VARCHAR,
    COMMENT VARCHAR,
    MIN VARCHAR,
    FGM INTEGER,
    FGA INTEGER,
    FG_PCT FLOAT,
    FG3M INTEGER,
    FG3A INTEGER,
    FG3_PCT FLOAT,
    FTM INTEGER,
    FTA INTEGER,
    FT_PCT FLOAT,
    OREB INTEGER,
    DREB INTEGER,
    REB INTEGER,
    AST INTEGER,
    STL INTEGER,
    BLK INTEGER,
    "TO" INTEGER,
    PF INTEGER,
    PTS INTEGER,
    PLUS_MINUS FLOAT
);


CREATE TABLE IF NOT EXISTS nba_bronze.teams_box_score_advanced (
    HASH_ID_TEAM VARCHAR PRIMARY KEY,
    GAME_ID VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_NAME VARCHAR,
    TEAM_ABBREVIATION VARCHAR,
    TEAM_CITY VARCHAR,
    MIN VARCHAR,
    E_OFF_RATING FLOAT,
    OFF_RATING FLOAT,
    E_DEF_RATING FLOAT,
    DEF_RATING FLOAT,
    E_NET_RATING FLOAT,
    NET_RATING FLOAT,
    AST_PCT FLOAT,
    AST_TOV FLOAT,
    AST_RATIO FLOAT,
    OREB_PCT FLOAT,
    DREB_PCT FLOAT,
    REB_PCT FLOAT,
    E_TM_TOV_PCT FLOAT,
    TM_TOV_PCT FLOAT,
    EFG_PCT FLOAT,
    TS_PCT FLOAT,
    USG_PCT FLOAT,
    E_USG_PCT FLOAT,
    E_PACE FLOAT,
    PACE FLOAT,
    PACE_PER40 FLOAT,
    POSS INTEGER,
    PIE FLOAT
);

CREATE TABLE IF NOT EXISTS nba_bronze.players_box_score_advanced (
    HASH_ID_PLAYER VARCHAR PRIMARY KEY,
    GAME_ID VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_ABBREVIATION VARCHAR,
    TEAM_CITY VARCHAR,
    PLAYER_ID VARCHAR,
    PLAYER_NAME VARCHAR,
    NICKNAME VARCHAR,
    START_POSITION VARCHAR,
    COMMENT VARCHAR,
    MIN VARCHAR,
    E_OFF_RATING FLOAT,
    OFF_RATING FLOAT,
    E_DEF_RATING FLOAT,
    DEF_RATING FLOAT,
    E_NET_RATING FLOAT,
    NET_RATING FLOAT,
    AST_PCT FLOAT,
    AST_TOV FLOAT,
    AST_RATIO FLOAT,
    OREB_PCT FLOAT,
    DREB_PCT FLOAT,
    REB_PCT FLOAT,
    TM_TOV_PCT FLOAT,
    EFG_PCT FLOAT,
    TS_PCT FLOAT,
    USG_PCT FLOAT,
    E_USG_PCT FLOAT,
    E_PACE FLOAT,
    PACE FLOAT,
    PACE_PER40 FLOAT,
    POSS INTEGER,
    PIE FLOAT
);

CREATE TABLE IF NOT EXISTS nba_bronze.playbyplayv3 (
    HASH_ID_ACTION VARCHAR PRIMARY KEY,
    gameId VARCHAR,
    actionNumber INTEGER,
    clock VARCHAR,
    "period" INTEGER,
    teamId INTEGER,
    teamTricode VARCHAR,
    personId VARCHAR,
    playerName VARCHAR,
    playerNameI VARCHAR,
    xLegacy INTEGER,
    yLegacy INTEGER,
    shotDistance INTEGER,
    shotResult VARCHAR,
    isFieldGoal INTEGER,
    scoreHome VARCHAR,
    scoreAway VARCHAR,
    pointsTotal INTEGER,
    "location" VARCHAR,
    "description" VARCHAR,
    actionType VARCHAR,
    subType VARCHAR,
    videoAvailable INTEGER,
    actionId INTEGER
);



CREATE TABLE IF NOT EXISTS nba_bronze.shotchartdetail (
    HASH_ID_ACTION VARCHAR PRIMARY KEY,
    GRID_TYPE VARCHAR,
    GAME_ID VARCHAR,
    GAME_EVENT_ID INTEGER,
    PLAYER_ID VARCHAR,
    PLAYER_NAME VARCHAR,
    TEAM_ID VARCHAR,
    TEAM_NAME VARCHAR,
    "PERIOD" INTEGER,
    MINUTES_REMAINING INTEGER,
    SECONDS_REMAINING INTEGER,
    EVENT_TYPE VARCHAR,
    ACTION_TYPE VARCHAR,
    SHOT_TYPE VARCHAR,
    SHOT_ZONE_BASIC VARCHAR,
    SHOT_ZONE_AREA VARCHAR,
    SHOT_ZONE_RANGE VARCHAR,
    SHOT_DISTANCE INTEGER,
    LOC_X INTEGER,
    LOC_Y INTEGER,
    SHOT_ATTEMPTED_FLAG INTEGER,
    SHOT_MADE_FLAG INTEGER,
    GAME_DATE VARCHAR,
    HTM VARCHAR,
    VTM VARCHAR
);
