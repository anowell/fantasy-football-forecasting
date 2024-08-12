use std::path::Path;

use polars::prelude::*;

mod error;
pub mod filter;
pub mod scoring;

pub use scoring::Scoring;

type Result<T> = std::result::Result<T, error::Error>;

pub fn load_parquet<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let mut file = std::fs::File::open(path)?;
    let df = ParquetReader::new(&mut file).finish()?;
    Ok(df)
}
