use anyhow::{bail, Result};
use clap::Parser;
use fff::{filter::Filter, scoring::score_game, scoring::Scoring};
use log::LevelFilter;
use polars::prelude::*;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode};

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

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Set the default level based on verbosity
    let default_level = match args.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let config = ConfigBuilder::new().add_filter_allow_str("fff::").build();

    // Initialize the logger with the custom configuration
    TermLogger::init(
        default_level,
        config,
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    log::trace!("Args {:#?}", args);

    let df = fff::load_parquet(args.file)?;
    log::info!("Loaded {} plays", df.height());

    let scoring = match &*args.scoring {
        "ppr" => Scoring::ppr(),
        "half-ppr" => Scoring::half_ppr(),
        "no-ppr" => Scoring::no_ppr(),
        "shawn" => crazy_shawn_scoring(),
        _ => bail!("Unsupported scoring. Use: ppr, half-ppr, no-ppr, or shawn"),
    };

    let filter = Filter::new().week(args.week).team(&args.team).build();
    let game_df = df.lazy().filter(filter).collect()?;
    let fantasy_stats = score_game(game_df, scoring)?;

    let simple = fantasy_stats
        .lazy()
        .select([cols(["team", "player_id", "player_name", "fantasy_points"])])
        .collect()?;
    println!("{}", simple);

    Ok(())
}
