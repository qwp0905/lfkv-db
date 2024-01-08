use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErrorKind {
  #[error("not found")]
  NotFound,

  #[error("invalid")]
  Invalid,

  #[error("unknown")]
  Unknown,

  #[error("io error")]
  IO(std::io::Error),
}
impl ErrorKind {
  pub fn to_string(&self) -> String {
    match self {
      Self::NotFound => format!("not found"),
      Self::Invalid => format!("invalid"),
      Self::Unknown => format!("unknown error"),
      Self::IO(err) => err.to_string(),
    }
  }
}

pub type Result<T> = std::result::Result<T, ErrorKind>;
