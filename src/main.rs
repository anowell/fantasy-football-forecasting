use anyhow::{bail, Result};
use clap::Parser;
use polars::{prelude::*, sql::SQLContext};
use scoring::{fantasy_stats, Scoring};

// mod play;
mod scoring;

fn crazy_shawn_scoring() -> Scoring {
    let mut scoring = Scoring::ppr();
    scoring.passing_300yd_bonus = 1.0;
    scoring.passing_400yd_bonus = 1.0;
    scoring.passing_td_50yd_bonus = 1.0;
    scoring.rushing_100yd_bonus = 1.0;
    scoring.rushing_200yd_bonus = 1.0;
    scoring.rushing_td_50yd_bonus = 1.0;
    scoring.receiving_100yd_bonus = 1.0;
    scoring.receiving_200yd_bonus = 1.0;
    scoring.receiving_td_50yd_bonus = 1.0;
    scoring.fg_made_40yd_bonus = 1.0;
    scoring.fg_made_50yd_bonus = 1.0;
    scoring
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short = 'f', long = "file", value_name = "FILE")]
    file: std::path::PathBuf,

    #[arg(short = 'w', long = "week")]
    week: i32,

    #[arg(short = 't', long = "team")]
    team: String,

    #[arg(long, default_value = "ppr")]
    scoring: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut file = std::fs::File::open(args.file)?;
    let df = ParquetReader::new(&mut file).finish()?;

    let scoring = match &*args.scoring {
        "ppr" => Scoring::ppr(),
        "half-ppr" => Scoring::half_ppr(),
        "no-ppr" => Scoring::no_ppr(),
        "shawn" => crazy_shawn_scoring(),
        _ => bail!("Unsupported scoring. Use: ppr, half-ppr, no-ppr, or shawn"),
    };

    let game_df = filter_game(df, args.week, &args.team)?;
    let fantasy_stats = fantasy_stats(game_df, scoring)?;

    let simple = fantasy_stats
        .lazy()
        .select([cols(["team", "player_id", "player_name", "fantasy_points"])])
        .collect()?;
    println!("{}", simple);
    // for stats in fantasy_stats.i {
    //     println!("{:?}", stats);
    // }

    Ok(())
}

pub fn filter_game(df: DataFrame, week: i32, team: &str) -> Result<DataFrame> {
    let mut ctx = SQLContext::new();
    ctx.register("plays", df.lazy());

    // SQL query to aggregate stats by player
    let query = format!(
        r#"
        SELECT *
        FROM plays
        WHERE week = {} AND posteam = '{}'
    "#,
        week, team
    );

    // Execute the query
    let fdf = ctx.execute(&query)?.collect()?;
    println!("{} week {}: {} plays", team, week, fdf.height());
    Ok(fdf)
}
