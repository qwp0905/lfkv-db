use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("not found")]
  NotFound,

  #[error("invalid")]
  Invalid,

  #[error("unknown")]
  Unknown(Box<dyn std::error::Error + Send + Sync>),

  #[error("io error")]
  IO(std::io::Error),

  #[error("end of file")]
  EOF,

  #[error("transaction already closed")]
  TransactionClosed,

  #[error("engine unavailable")]
  EngineUnavailable,
}
impl Error {
  pub fn unknown<E>(e: E) -> Error
  where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
  {
    Error::Unknown(e.into())
  }
}

pub type Result<T = ()> = std::result::Result<T, Error>;
