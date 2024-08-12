use crate::Result;
use polars::{prelude::*, sql::SQLContext};

#[derive(Clone)]
pub struct Filter {
    filter_expr: Option<Expr>,
}

impl Filter {
    // Creates a new FilterBuilder
    pub fn new() -> Self {
        Self { filter_expr: None }
    }

    // Adds a filter for the team
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
    pub fn player(mut self, player_name: &str) -> Self {
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
            .unwrap(); // Ensures there's at least one expression

        self.extend_filter(expr)
    }

    // Adds a filter for the week
    pub fn week(mut self, week: i32) -> Self {
        let expr = col("week").eq(lit(week));
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
        self.filter_expr
            .expect("Must call at least one Filter builder method")
    }
}

pub fn filter_sql(df: LazyFrame, query: &str) -> Result<LazyFrame> {
    let mut ctx = SQLContext::new();
    ctx.register("plays", df);
    let df = ctx.execute(&query)?;
    Ok(df)
}
