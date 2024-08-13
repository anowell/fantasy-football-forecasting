use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use fff::{
    pbp::{PbpDf, PbpFilter},
    roster::{RosterDf, RosterFilter},
    scoring::Scoring,
    Position,
};
use itertools::Itertools;
use log::LevelFilter;
use parse_display::Display;
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
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Loads data for a given year
    #[arg(short = 'y', long = "year", default_value_t = 2023)]
    year: u16,

    /// Filter by team
    #[arg(short = 't', long = "team")]
    team: Option<String>,

    /// Filter by position
    #[arg(short = 'p', long = "pos", value_enum)]
    position: Option<Position>,

    /// Filtering week number or range (e.g. 3 or 3-5)
    #[arg(short = 'w', long = "week", alias = "weeks")]
    weeks: Option<WeekArg>,

    #[arg(short = 'x', long = "exclude")]
    exclude: Option<PathBuf>,

    /// Calculate fantasy score
    #[arg(long)]
    score: Option<String>,

    /// Choose how to aggregate scores
    #[arg(long = "score-by", default_value_t = ScoreBy::Player)]
    score_by: ScoreBy,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[display(style = "lowercase")]
pub enum ScoreBy {
    PlayerGame,
    Player,
    Game,
    // FantasyTeam,
}

#[derive(Copy, Clone, Debug)]
enum WeekArg {
    Week(u16),
    WeekRange(u16, u16),
}

impl std::str::FromStr for WeekArg {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<u16> = s.split('-').map(str::parse).try_collect()?;
        match &*parts {
            [single] => Ok(WeekArg::Week(*single)),
            [start, end] if start <= end => Ok(WeekArg::WeekRange(*start, *end)),
            _ => Err(s.parse::<u16>().unwrap_err()),
        }
    }
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

    let config = ConfigBuilder::new().add_filter_allow_str("fff").build();

    // Initialize the logger with the custom configuration
    TermLogger::init(
        default_level,
        config,
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    log::trace!("Args {:#?}", args);

    let mut pbp_filter = PbpFilter::new();
    let mut roster_filter = RosterFilter::new();

    if let Some(team) = args.team {
        pbp_filter = pbp_filter.team(&team);
        roster_filter = roster_filter.team(&team);
    }

    if let Some(pos) = args.position {
        roster_filter = roster_filter.position(pos);
        // pbp doesn't have a way to filter on position, so we have to do it after joining with roster df
    }

    match args.weeks {
        Some(WeekArg::Week(week)) => {
            pbp_filter = pbp_filter.week(week);
            roster_filter = roster_filter.week(week);
        }
        Some(WeekArg::WeekRange(week, through)) => {
            pbp_filter = pbp_filter.week_range(week, through);
            roster_filter = roster_filter.week_range(week, through);
        }
        None => {}
    }

    if let Some(_exclude) = args.exclude {
        todo!("Implement --exclude");
    }

    // Finalize the filters
    let pbp_filter = pbp_filter.build();
    let roster_filter = roster_filter.build();

    let pbp_df = PbpDf::load(args.year)?;
    log::info!("Loaded {} plays", pbp_df.height());

    let game_df = pbp_df.filter(pbp_filter)?;
    log::info!("Filtered to {} plays", game_df.height());
    let mut fantasy_stats = game_df.fantasy_stats()?;

    if let Some(position) = args.position {
        let roster_df = RosterDf::load(args.year)?;
        log::info!("Loaded {} roster entries", roster_df.height());
        let position_df = roster_df.filter(roster_filter)?.unique_players()?;

        log::info!("Filtered players for position: {}", position);
        let print_cols = ["week", "team", "position", "gsis_id", "full_name", "status"];
        debug_df(&position_df, &print_cols)?;

        // Merge roster_df with fantasy_stats
        fantasy_stats = fantasy_stats.merge_roster(position_df)?;
        log::info!("Stats for position: {}", position);
        let print_cols = cols(["game_id", "player_id", "player_name", "position"]);
        if args.score.is_none() {
            print_df(&fantasy_stats, print_cols)?;
        }
    }

    if let Some(score) = args.score {
        let scoring = match &*score {
            "ppr" => Scoring::ppr(),
            "half-ppr" => Scoring::half_ppr(),
            "no-ppr" => Scoring::no_ppr(),
            "shawn" => crazy_shawn_scoring(),
            _ => bail!("Unsupported scoring. Use: ppr, half-ppr, no-ppr, or shawn"),
        };
        let scores = match args.score_by {
            ScoreBy::Player => fantasy_stats.score_by_player(scoring)?,
            ScoreBy::PlayerGame => fantasy_stats.score(scoring)?,
            ScoreBy::Game => fantasy_stats.score_by_game(scoring)?,
        };
        let print_cols = match args.score_by {
            ScoreBy::Player => all(),
            ScoreBy::Game => all(),
            ScoreBy::PlayerGame => cols(["game_id", "week", "team", "player_id", "fantasy_points"]),
        };
        log::info!("Fantasy points for query");
        print_df(&scores, print_cols)?;
    }

    Ok(())
}

fn debug_df(df: &DataFrame, print_cols: &[&str]) -> Result<()> {
    let print_df = df.clone().lazy().select([cols(print_cols)]).collect()?;
    log::debug!("{}", print_df);
    Ok(())
}

fn print_df(df: &DataFrame, print_cols: Expr) -> Result<()> {
    let print_df = df.clone().lazy().select([print_cols]).collect()?;
    println!("{}", print_df);
    Ok(())
}
