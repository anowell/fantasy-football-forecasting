set dotenv-load

plays := "read_parquet('play_by_play_2023.parquet')"
rosters := "read_parquet('roster_weekly_2023.parquet')"

run *args:
    cargo run -p fff-cli --bin fff -- {{args}}

# Query with polars. Example: just query "select * from plays limit 1" -o json | jq '.'
query query *args:
    polars -c "{{ replace(replace(query, 'rosters', rosters), 'plays', plays)}}" {{args}}

schema *args:
    @just query 'select * from plays {{args}} limit 1' -o json


_datadir:
   @mkdir -p data 

download-data year: _datadir (_download-pbp year) (_download-roster year)

# Download play-by-play data
_download-pbp year: _datadir
    curl -L -o data/pbp_{{year}}.parquet https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{{year}}.parquet

# Download weekly rosters
_download-roster year: _datadir
    curl -L -o data/rosters_{{year}}.parquet https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{{year}}.parquet

# Download ECR data which attempts to distill Fantasy dynasty projections to a single number
_download-ecr: _datadir
    curl -L -o data/ecr.parquet  https://github.com/dynastyprocess/data/raw/master/files/db_fpecr.parquet 
