

replacement := "read_parquet('play_by_play_2023.parquet')"
# Example: just query "select * from plays limit 1" -o json | jq '.'
query query *args:
    polars -c "{{ replace(query, 'plays', replacement)}}" {{args}}

sync-pxp year:
    curl -LO https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{{year}}.parquet

sync-ecr:
    curl -LO https://github.com/dynastyprocess/data/raw/master/files/db_fpecr.parquet 
