function Update-NBADataLake {
    param (
        [string]$SeasonYear,
        [string]$SeasonType,
        [switch]$ShotChartDetail
    )

    $nba_bronze = "poetry run get_nba_bronze -sy $($SeasonYear) -st $($SeasonType)"

    if ($ShotChartDetail.IsPresent) {
        $nba_bronze = $nba_bronze + " -shotchartdetail"
    }

    Invoke-Expression $nba_bronze

    if (((Get-Date) - ((Get-Item db/nba.duckdb.dvc).LastWriteTime)).Day -gt 15) {
        poetry run dvc add .\db\nba.duckdb
        poetry run dvc push
    }
}
