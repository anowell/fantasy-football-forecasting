use crate::roster::RosterDf;
use crate::Result;
use derive_deref::Deref;
use polars::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Scoring {
    pub passing_yd_per_point: f64,
    pub passing_300yd_bonus: f64,
    pub passing_400yd_bonus: f64,
    pub passing_td_points: f64,
    pub passing_td_50yd_bonus: f64,

    pub rushing_yd_per_point: f64,
    pub rushing_100yd_bonus: f64,
    pub rushing_200yd_bonus: f64,
    pub rushing_td_points: f64,
    pub rushing_td_50yd_bonus: f64,

    pub reception_points: f64,
    pub receiving_yd_per_point: f64,
    pub receiving_100yd_bonus: f64,
    pub receiving_200yd_bonus: f64,
    pub receiving_td_points: f64,
    pub receiving_td_50yd_bonus: f64,

    pub fumble_recovery_td_points: f64,
    pub return_td_points: f64,

    pub interception_points: f64,
    pub fumble_lost_points: f64,
    pub two_point_conversion_points: f64,
    pub fg_made_points: f64,
    pub fg_made_40yd_bonus: f64,
    pub fg_made_50yd_bonus: f64,
    pub pat_made_points: f64,
}

impl Scoring {
    pub fn ppr() -> Self {
        Self {
            passing_yd_per_point: 25.0,
            passing_td_points: 4.0,
            interception_points: -2.0,
            rushing_yd_per_point: 10.0,
            rushing_td_points: 6.0,
            reception_points: 1.0,
            receiving_yd_per_point: 10.0,
            receiving_td_points: 6.0,
            fumble_lost_points: -2.0,
            two_point_conversion_points: 2.0,
            fg_made_points: 3.0,
            pat_made_points: 1.0,
            passing_300yd_bonus: 0.0,
            passing_400yd_bonus: 0.0,
            passing_td_50yd_bonus: 0.0,
            rushing_100yd_bonus: 0.0,
            rushing_200yd_bonus: 0.0,
            rushing_td_50yd_bonus: 0.0,
            receiving_100yd_bonus: 0.0,
            receiving_200yd_bonus: 0.0,
            receiving_td_50yd_bonus: 0.0,
            fumble_recovery_td_points: 6.0,
            return_td_points: 6.0,
            fg_made_40yd_bonus: 0.0,
            fg_made_50yd_bonus: 0.0,
        }
    }

    pub fn half_ppr() -> Self {
        let mut scoring = Self::ppr();
        scoring.reception_points = 0.5;
        scoring
    }

    pub fn no_ppr() -> Self {
        let mut scoring = Self::ppr();
        scoring.reception_points = 0.0;
        scoring
    }
}

pub(crate) static PASSING_QUERY: &str = r#"
    SELECT 
        game_id,
        posteam as team,
        passer_player_id as player_id,
        passer_player_name as player_name,
        SUM(passing_yards) as passing_yards,
        SUM(pass_touchdown) as pass_touchdowns,
        SUM(interception) as interceptions,
        SUM(CASE WHEN passing_yards > 50 THEN pass_touchdown ELSE 0 END) as passing_50yd_td
    FROM plays
    WHERE passer_player_name IS NOT NULL
    GROUP BY game_id, posteam, passer_player_id, passer_player_name
"#;

pub(crate) static RECEIVING_QUERY: &str = r#"
    SELECT
        game_id,
        posteam as team,
        receiver_player_id as player_id,
        receiver_player_name as player_name,
        SUM(complete_pass) as receptions,
        SUM(receiving_yards) as receiving_yards,
        SUM(pass_touchdown) as receiving_touchdowns,
        SUM(CASE WHEN two_point_conv_result = 'success' THEN 1 ELSE 0 END) as two_pt_conv_made,
        SUM(CASE WHEN receiving_yards > 50 THEN pass_touchdown ELSE 0 END) as receiving_50yd_td
    FROM plays
    WHERE receiver_player_name IS NOT NULL
    GROUP BY game_id, posteam, receiver_player_id, receiver_player_name
"#;

pub(crate) static RUSHING_QUERY: &str = r#"
    SELECT
        game_id,
        posteam as team,
        rusher_player_id as player_id,
        rusher_player_name as player_name,
        SUM(rushing_yards) as rushing_yards,
        SUM(rush_touchdown) as rush_touchdowns,
        SUM(CASE WHEN two_point_conv_result = 'success' THEN 1 ELSE 0 END) as two_pt_conv_made,
        SUM(CASE WHEN rushing_yards > 50 THEN rush_touchdown ELSE 0 END) as rushing_50yd_td
    FROM plays
    WHERE rusher_player_name IS NOT NULL
    GROUP BY game_id, posteam, rusher_player_id, rusher_player_name
"#;

pub(crate) static FUMBLING_QUERY: &str = r#"
    SELECT
        game_id,
        posteam as team,
        fumbled_1_player_id as player_id,
        fumbled_1_player_name as player_name,
        SUM(fumble_lost) as fumbles_lost
    FROM plays
    WHERE fumbled_1_player_name IS NOT NULL
    GROUP BY game_id, posteam, fumbled_1_player_id, fumbled_1_player_name
"#;

pub(crate) static KICKING_QUERY: &str = r#"
    SELECT
        game_id,
        posteam as team,
        kicker_player_id as player_id,
        kicker_player_name as player_name,
        SUM(CASE WHEN extra_point_result = 'good' THEN 1 ELSE 0 END) as pat_made,
        SUM(CASE WHEN field_goal_result = 'made' THEN 1 ELSE 0 END) as fg_made,
        SUM(CASE WHEN kick_distance >= 40 AND field_goal_result = 'made' THEN 1 ELSE 0 END) as fg_40plus_made,
        SUM(CASE WHEN kick_distance >= 50 AND field_goal_result = 'made' THEN 1 ELSE 0 END) as fg_50plus_made
    FROM plays
    WHERE play_type != 'kickoff' AND kicker_player_name IS NOT NULL
    GROUP BY game_id, posteam, kicker_player_id, kicker_player_name
"#;

pub(crate) static RETURNING_QUERY: &str = r#"
    SELECT
        game_id,
        team,
        player_id,
        player_name,
        SUM(return_touchdown) as td_returns
    FROM (
        SELECT 
            game_id,
            posteam as team,
            COALESCE(
                lateral_kickoff_returner_player_id, 
                lateral_punt_returner_player_id, 
                kickoff_returner_player_id, 
                punt_returner_player_id
            ) as player_id,
            COALESCE(
                lateral_kickoff_returner_player_name, 
                lateral_punt_returner_player_name, 
                kickoff_returner_player_name, 
                punt_returner_player_name
            ) as player_name,
            return_touchdown
        FROM plays
        WHERE return_touchdown = 1.0
    ) as coalesced_players
    GROUP BY game_id, team, player_id, player_name
"#;

#[derive(Clone, Deref)]
pub struct FantasyStatsDf(DataFrame);

impl FantasyStatsDf {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self(df)
    }

    pub fn filter(self, filter: Expr) -> Result<Self> {
        let df = self.0.lazy().filter(filter).collect()?;
        Ok(Self(df))
    }

    pub fn merge_roster(&self, roster_df: RosterDf) -> Result<FantasyStatsDf> {
        let join_args = JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns);
        let merged_df = self.join(&roster_df, ["player_id"], ["gsis_id"], join_args)?;

        log::debug!("{} total players with fantasy points", merged_df.height());
        Ok(Self(merged_df))
    }

    /// Takes player fantasy stats dataframe and calculates fantasy score
    fn score_lazy(self, scoring: Scoring) -> Result<LazyFrame> {
        let fantasy_df = self
            .0
            .lazy()
            .with_column(scoring_cols(scoring).alias("fantasy_points"));
        Ok(fantasy_df)
    }

    pub fn score(self, scoring: Scoring) -> Result<DataFrame> {
        let df = self
            .score_lazy(scoring)?
            .sort(
                ["fantasy_points"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()?;
        Ok(df)
    }

    pub fn score_by_player(self, scoring: Scoring) -> Result<DataFrame> {
        let lf = self.score_lazy(scoring)?;
        let df = lf
            .group_by(&[col("player_id")])
            .agg([
                col("fantasy_points").sum(),
                cols(["player_name", "team"]).first(),
            ])
            .sort(
                ["fantasy_points"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()?;
        Ok(df)
    }

    // ScoreBy::Game => &["game_id", "week", "team", "fantasy_points"],
    pub fn score_by_game(self, scoring: Scoring) -> Result<DataFrame> {
        let lf = self.score_lazy(scoring)?;
        let df = lf
            .group_by(&[col("game_id")])
            .agg([col("fantasy_points").sum(), cols(["week", "team"]).first()])
            .sort(
                ["fantasy_points"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()?;
        Ok(df)
    }
}

fn scoring_cols(scoring: Scoring) -> Expr {
    col("passing_yards").fill_null(lit(0.0)) / lit(scoring.passing_yd_per_point)
        + col("pass_touchdowns").fill_null(lit(0.0)) * lit(scoring.passing_td_points)
        + col("rushing_yards").fill_null(lit(0.0)) / lit(scoring.rushing_yd_per_point)
        + col("rush_touchdowns").fill_null(lit(0.0)) * lit(scoring.rushing_td_points)
        + col("receptions").fill_null(lit(0.0)) * lit(scoring.reception_points)
        + col("receiving_yards").fill_null(lit(0.0)) / lit(scoring.receiving_yd_per_point)
        + col("receiving_touchdowns").fill_null(lit(0.0)) * lit(scoring.receiving_td_points)
        + col("interceptions").fill_null(lit(0.0)) * lit(scoring.interception_points)
        + col("fumbles_lost").fill_null(lit(0.0)) * lit(scoring.fumble_lost_points)
        + col("fg_made").fill_null(lit(0.0)) * lit(scoring.fg_made_points)
        + col("pat_made").fill_null(lit(0.0)) * lit(scoring.pat_made_points)
        + col("td_returns").fill_null(lit(0.0)) * lit(scoring.return_td_points)
        + col("two_pt_conv_made").fill_null(lit(0.0)) * lit(scoring.two_point_conversion_points)
        // TODO: Figure out how to count fumble recovery TDs


        // All the code below here is because my brother
        // couldn't just pick standard scoring when starting
        // a game with a bunch of family that hasn't played FF before.
        // A true jerk move. There, I said it.

        // Passing bonuses
        + col("passing_50yd_td").fill_null(lit(0.0)) * lit(scoring.passing_td_50yd_bonus)
        + when(col("passing_yards").gt(lit(300.0)))
            .then(lit(scoring.passing_300yd_bonus))
            .otherwise(lit(0.0))
        + when(col("passing_yards").gt(lit(400.0)))
            .then(lit(scoring.passing_400yd_bonus))
            .otherwise(lit(0.0))
        // Rushing bonuses
        + col("rushing_50yd_td").fill_null(lit(0.0)) * lit(scoring.rushing_td_50yd_bonus)
        + when(col("rushing_yards").gt(lit(100.0)))
            .then(lit(scoring.rushing_100yd_bonus))
            .otherwise(lit(0.0))
        + when(col("rushing_yards").gt(lit(200.0)))
            .then(lit(scoring.rushing_200yd_bonus))
            .otherwise(lit(0.0))
        // Receiving bonuses
        + col("receiving_50yd_td").fill_null(lit(0.0)) * lit(scoring.receiving_td_50yd_bonus)
        + when(col("receiving_yards").gt(lit(100.0)))
            .then(lit(scoring.receiving_100yd_bonus))
            .otherwise(lit(0.0))
        + when(col("receiving_yards").gt(lit(200.0)))
            .then(lit(scoring.receiving_200yd_bonus))
            .otherwise(lit(0.0))
        // Kicking bonuses
        + col("fg_40plus_made").fill_null(lit(0.0)) * lit(scoring.fg_made_40yd_bonus)
        + col("fg_50plus_made").fill_null(lit(0.0)) * lit(scoring.fg_made_50yd_bonus)
}
