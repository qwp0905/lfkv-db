use thiserror::Error;

#[derive(Debug)]
pub struct Unknown {
  error: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, Error)]
pub enum Error {
  #[error("not found")]
  NotFound,

  #[error("invalid")]
  Invalid,

  #[error("unknown")]
  Unknown(Unknown),

  #[error("io error")]
  IO(std::io::Error),

  #[error("end of file")]
  EOF,
}
impl Error {
  pub fn to_string(&self) -> String {
    match self {
      Self::NotFound => format!("not found"),
      Self::Invalid => format!("invalid"),
      Self::Unknown(err) => format!("unknown error {}", err.error),
      Self::EOF => format!("end of file"),
      Self::IO(err) => err.to_string(),
    }
  }

  pub fn unknown<E>(e: E) -> Error
  where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
  {
    Error::Unknown(Unknown { error: e.into() })
  }
}

pub type Result<T> = std::result::Result<T, Error>;
