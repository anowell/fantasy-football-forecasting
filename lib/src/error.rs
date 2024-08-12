use polars::error::PolarsError;
use std::io::Error as IoError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("IO error: {0}")]
    Io(#[from] IoError),

    #[error("Expected single game, found multiple: {}", .0.join(", "))]
    NotASingleGame(Vec<String>),
}
