use crate::Result;
use polars::{prelude::*, sql::SQLContext};

pub fn filter_sql(df: LazyFrame, query: &str) -> Result<LazyFrame> {
    let mut ctx = SQLContext::new();
    ctx.register("plays", df);
    let df = ctx.execute(&query)?;
    Ok(df)
}
