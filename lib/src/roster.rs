use crate::{Position, Result};
use derive_deref::Deref;
use polars::prelude::*;

#[derive(Clone, Deref)]
pub struct RosterDf(DataFrame);

impl RosterDf {
    pub fn new(df: DataFrame) -> Self {
        RosterDf(df)
    }

    pub fn load(year: u16) -> Result<Self> {
        let df = crate::load_parquet(format!("data/rosters_{}.parquet", year))?;
        Ok(RosterDf(df))
    }

    pub fn filter(self, filter: Expr) -> Result<Self> {
        let df = self.0.lazy().filter(filter).collect()?;
        Ok(RosterDf(df))
    }

    pub fn unique_players(self) -> Result<Self> {
        let expr = col("gsis_id").is_first_distinct();
        let df = self.0.lazy().filter(expr).collect()?;
        Ok(RosterDf(df))
    }
}

#[derive(Clone)]
pub struct RosterFilter {
    filter_expr: Option<Expr>,
}

impl RosterFilter {
    pub fn new() -> Self {
        Self { filter_expr: None }
    }

    pub fn team(mut self, team_name: &str) -> Self {
        let expr = col("team").eq(lit(team_name));
        self.extend_filter(expr)
    }

    pub fn player_id(mut self, player_id: &str) -> Self {
        let expr = col("gsis_id").eq(lit(player_id));
        self.extend_filter(expr)
    }

    pub fn player_name(mut self, player_name: &str) -> Self {
        let player_columns = vec!["full_name", "last_name", "first_name"];

        let expr = player_columns
            .into_iter()
            .map(|col_name| col(col_name).eq(lit(player_name)))
            .reduce(|acc, expr| acc.or(expr))
            .unwrap();

        self.extend_filter(expr)
    }

    pub fn position(mut self, position: Position) -> Self {
        let expr = match position {
            Position::Flex => {
                // Flex should match WR, TE, or RB
                col("position")
                    .eq(lit(Position::Wr.to_string().to_uppercase()))
                    .or(col("position").eq(lit(Position::Te.to_string().to_uppercase())))
                    .or(col("position").eq(lit(Position::Rb.to_string().to_uppercase())))
            }
            _ => {
                // Default case: match the specific position
                col("position").eq(lit(position.to_string().to_uppercase()))
            }
        };

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

    pub fn unique_players(mut self) -> Self {
        let expr = col("gsis_id").is_first_distinct();
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
