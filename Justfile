

sync-pxp year:
    curl -LO https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{{year}}.parquet

sync-ecr:
    curl -LO https://github.com/dynastyprocess/data/raw/master/files/db_fpecr.parquet 
