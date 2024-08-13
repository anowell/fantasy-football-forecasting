use parse_display::{Display, FromStr};
use polars::prelude::*;
use std::path::Path;

mod error;
pub mod filter;
pub mod pbp;
pub mod roster;
pub mod scoring;
pub use scoring::Scoring;

type Result<T> = std::result::Result<T, error::Error>;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Display, FromStr)]
#[display(style = "lowercase")]
pub enum Position {
    Qb,
    Rb,
    Wr,
    Te,
    Flex,
    K,
}

pub fn load_parquet<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let mut file = std::fs::File::open(path)?;
    let df = ParquetReader::new(&mut file).finish()?;
    Ok(df)
}
