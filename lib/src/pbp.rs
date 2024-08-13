use crate::{
    error::Error,
    roster::RosterDf,
    scoring::{self, FantasyStatsDf},
    Result,
};
use derive_deref::Deref;
use polars::{prelude::*, sql::SQLContext};

#[derive(Clone, Deref)]
pub struct PbpDf(DataFrame);

impl PbpDf {
    pub fn new(df: DataFrame) -> Self {
        PbpDf(df)
    }

    pub fn load(year: u16) -> Result<Self> {
        let df = crate::load_parquet(format!("data/pbp_{}.parquet", year))?;
        Ok(PbpDf(df))
    }

    pub fn filter(self, filter: Expr) -> Result<Self> {
        let df = self.0.lazy().filter(filter).collect()?;
        Ok(PbpDf(df))
    }

    pub fn merge_roster(&self, roster_df: RosterDf) -> Result<DataFrame> {
        let join_args = JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns);
        let merged_df = self.join(&roster_df, ["player_id"], ["gsis_id"], join_args)?;

        log::debug!("{} total players with fantasy points", merged_df.height());
        Ok(merged_df)
    }

    /// Takes a play-by-play dataframe and returns a new dataframe containing fantasy stats by player
    pub fn fantasy_stats(self) -> Result<FantasyStatsDf> {
        log::trace!("pbp::fantasy_game_stats");
        let df = self.0;

        let mut ctx = SQLContext::new();
        ctx.register("plays", df.lazy());

        let passing_df = ctx.execute(scoring::PASSING_QUERY)?.collect()?;
        log::debug!("{} passers with fantasy points", passing_df.height());
        let receiving_df = ctx.execute(scoring::RECEIVING_QUERY)?.collect()?;
        log::debug!("{} receivers with fantasy points", receiving_df.height());
        let rushing_df = ctx.execute(scoring::RUSHING_QUERY)?.collect()?;
        log::debug!("{} rushers with fantasy points", rushing_df.height());
        let fumbling_df = ctx.execute(scoring::FUMBLING_QUERY)?.collect()?;
        log::debug!("{} fumblers with fantasy points", fumbling_df.height());
        let kicking_df = ctx.execute(scoring::KICKING_QUERY)?.collect()?;
        log::debug!("{} kickers with fantasy points", kicking_df.height());
        let returning_df = ctx.execute(scoring::RETURNING_QUERY)?.collect()?;
        log::debug!("{} returners with fantasy points", returning_df.height());

        // Merge the DataFrames on team, player_id, and player_name
        let join_cols = ["game_id", "team", "player_id", "player_name"];
        let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
        let merged_df = passing_df
            .join(&receiving_df, join_cols, join_cols, join_args.clone())?
            .join(&rushing_df, join_cols, join_cols, join_args.clone())?
            .join(&passing_df, join_cols, join_cols, join_args.clone())?
            .join(&kicking_df, join_cols, join_cols, join_args.clone())?
            .join(&returning_df, join_cols, join_cols, join_args.clone())?
            .join(&fumbling_df, join_cols, join_cols, join_args.clone())?;
        log::debug!("{} total players with fantasy points", merged_df.height());
        Ok(FantasyStatsDf::new(merged_df))
    }
}

pub fn assert_single_game_id(df: &DataFrame) -> Result<()> {
    // Get the unique values in the `game_id` column
    let unique_game_ids = df
        .column("game_id")
        .unwrap()
        // .utf8()
        // .unwrap()
        .unique()
        .unwrap();

    // Check the number of unique values
    let unique_count = unique_game_ids.len();

    if unique_count == 1 {
        // If exactly one unique value is found, return Ok
        Ok(())
    } else {
        // Otherwise, return an Err with the found game_ids
        let game_ids: Vec<String> = unique_game_ids.iter().map(|s| s.to_string()).collect();
        Err(Error::NotASingleGame(game_ids))
    }
}

#[derive(Clone)]
pub struct PbpFilter {
    filter_expr: Option<Expr>,
}

impl PbpFilter {
    pub fn new() -> Self {
        Self { filter_expr: None }
    }

    pub fn team(mut self, team_name: &str) -> Self {
        let expr = col("posteam").eq(lit(team_name));
        self.extend_filter(expr)
    }

    // Adds a filter for the game
    pub fn game(mut self, game_id: &str) -> Self {
        let expr = col("game_id").eq(lit(game_id));
        self.extend_filter(expr)
    }

    // Adds a filter for the player, which matches against multiple columns
    pub fn player_id(mut self, player_id: &str) -> Self {
        let player_columns = vec![
            "passer_player_id",
            "receiver_player_id",
            "rusher_player_id",
            "fumbled_1_player_id",
            "kicker_player_id",
            "lateral_kickoff_returner_player_id",
            "lateral_punt_returner_player_id",
            "kickoff_returner_player_id",
            "punt_returner_player_id",
        ];

        let expr = player_columns
            .into_iter()
            .map(|col_name| col(col_name).eq(lit(player_id)))
            .reduce(|acc, expr| acc.or(expr))
            .unwrap();

        self.extend_filter(expr)
    }

    // Adds a filter for the player, which matches against multiple columns
    pub fn player_name(mut self, player_name: &str) -> Self {
        let player_columns = vec![
            "passer_player_name",
            "receiver_player_name",
            "rusher_player_name",
            "fumbled_1_player_name",
            "kicker_player_name",
            "lateral_kickoff_returner_player_name",
            "lateral_punt_returner_player_name",
            "kickoff_returner_player_name",
            "punt_returner_player_name",
        ];

        let expr = player_columns
            .into_iter()
            .map(|col_name| col(col_name).eq(lit(player_name)))
            .reduce(|acc, expr| acc.or(expr))
            .unwrap();

        self.extend_filter(expr)
    }

    // Adds a filter for the week
    pub fn week(mut self, week: u16) -> Self {
        let expr = col("week").eq(lit(week as u32));
        self.extend_filter(expr)
    }

    pub fn week_range(mut self, start: u16, end: u16) -> Self {
        let expr = col("week").is_between(start as u32, end as u32, ClosedInterval::Both);
        self.extend_filter(expr)
    }

    // Combines the current filter with a new one using AND logic
    fn extend_filter(&mut self, new_expr: Expr) -> Self {
        self.filter_expr = match self.filter_expr.take() {
            Some(existing_expr) => Some(existing_expr.and(new_expr)),
            None => Some(new_expr),
        };
        self.clone()
    }

    // Builds the final filter expression
    pub fn build(self) -> Expr {
        self.filter_expr.unwrap_or_else(|| lit(true))
    }
}
