use anyhow::Result;
use polars::prelude::*;
use polars::sql::SQLContext;

// #[derive(Debug, Clone)]
// pub struct PlayerFantasyStats {
//     pub team: String,
//     pub player_name: String,
//     pub player_id: String,
//     pub fantasy_points: f64,
// }

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
    pub kick_punt_return_td_points: f64,

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
            kick_punt_return_td_points: 6.0,
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

pub fn fantasy_stats(df: DataFrame, scoring: Scoring) -> Result<DataFrame> {
    // Initialize the SQL context
    let mut ctx = SQLContext::new();
    ctx.register("plays", df.lazy());
    let passing_query = r#"
        SELECT 
            posteam as team,
            passer_player_id as player_id,
            passer_player_name as player_name,
            SUM(passing_yards) as passing_yards,
            SUM(pass_touchdown) as pass_touchdowns,
            SUM(interception) as interceptions,
            SUM(CASE WHEN passing_yards > 50 THEN pass_touchdown ELSE 0 END) as passing_50yd_td
        FROM plays
        WHERE passer_player_name IS NOT NULL
        GROUP BY posteam, passer_player_id, passer_player_name
    "#;

    let receiving_query = r#"
        SELECT 
            posteam as team,
            receiver_player_id as player_id,
            receiver_player_name as player_name,
            SUM(complete_pass) as receptions,
            SUM(receiving_yards) as receiving_yards,
            SUM(pass_touchdown) as receiving_touchdowns,
            SUM(CASE WHEN receiving_yards > 50 THEN pass_touchdown ELSE 0 END) as receiving_50yd_td
        FROM plays
        WHERE receiver_player_name IS NOT NULL
        GROUP BY posteam, receiver_player_id, receiver_player_name
    "#;

    let rushing_query = r#"
        SELECT 
            posteam as team,
            rusher_player_id as player_id,
            rusher_player_name as player_name,
            SUM(rushing_yards) as rushing_yards,
            SUM(rush_touchdown) as rush_touchdowns,
            SUM(CASE WHEN rushing_yards > 50 THEN rush_touchdown ELSE 0 END) as rushing_50yd_td
        FROM plays
        WHERE rusher_player_name IS NOT NULL
        GROUP BY posteam, rusher_player_id, rusher_player_name
        "#;

    let fumbling_query = r#"
        SELECT 
            posteam as team,
            fumbled_1_player_id as player_id,
            fumbled_1_player_name as player_name,
            SUM(fumble_lost) as fumbles_lost
        FROM plays
        WHERE fumbled_1_player_name IS NOT NULL
        GROUP BY posteam, fumbled_1_player_id, fumbled_1_player_name
    "#;

    let kicking_query = r#"
        SELECT 
            posteam as team,
            kicker_player_id as player_id,
            kicker_player_name as player_name,
            SUM(field_goal_made) as fg_made,
            SUM(extra_point_made) as pat_made,
            SUM(CASE WHEN kick_distance >= 40 AND field_goal_made = 1 THEN 1 ELSE 0 END) as fg_40plus_made,
            SUM(CASE WHEN kick_distance >= 50 AND field_goal_made = 1 THEN 1 ELSE 0 END) as fg_50plus_made
        FROM plays
        WHERE kicker_player_name IS NOT NULL
        GROUP BY posteam, kicker_player_id, kicker_player_name
    "#;

    let passing_df = ctx.execute(passing_query)?.collect()?;
    println!("{} passers with fantasy points", passing_df.height());
    let receiving_df = ctx.execute(receiving_query)?.collect()?;
    println!("{} receivers with fantasy points", receiving_df.height());
    let rushing_df = ctx.execute(rushing_query)?.collect()?;
    println!("{} rushers with fantasy points", rushing_df.height());
    let fumbling_df = ctx.execute(fumbling_query)?.collect()?;
    println!("{} fumblers with fantasy points", fumbling_df.height());
    let kicking_df = ctx.execute(kicking_query)?.collect()?;
    println!("{} kickers with fantasy points", kicking_df.height());

    // Merge the DataFrames on team, player_id, and player_name
    let join_cols = ["team", "player_id", "player_name"];
    let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    let merged_df = passing_df
        .join(&receiving_df, join_cols, join_cols, join_args.clone())?
        .join(&rushing_df, join_cols, join_cols, join_args.clone())?
        .join(&passing_df, join_cols, join_cols, join_args.clone())?
        .join(&kicking_df, join_cols, join_cols, join_args.clone())?;
    println!("{} total players with fantasy points", merged_df.height());

    // Perform fantasy point calculations
    let fantasy_df = merged_df
        .lazy()
        .with_column(scoring_cols(scoring).alias("fantasy_points"))
        .sort(
            ["fantasy_points"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()?;

    Ok(fantasy_df)
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

        // All the ccode below here is because my brother
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
}
