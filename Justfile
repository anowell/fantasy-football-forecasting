set dotenv-load

replacement := "read_parquet('play_by_play_2023.parquet')"
# Query with polars. Example: just query "select * from plays limit 1" -o json | jq '.'
query query *args:
    echo $FMT_TABLE_FORMATTING
    polars -c "{{ replace(query, 'plays', replacement)}}" {{args}}

# Download play-by-play data
download-pbp year:
    curl -LO https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{{year}}.parquet

# Download ECR data which attempts to distill Fantasy dynasty projections to a single number
download-ecr:
    curl -LO https://github.com/dynastyprocess/data/raw/master/files/db_fpecr.parquet 
