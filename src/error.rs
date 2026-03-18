use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("not found")]
  NotFound,

  #[error("invalid format")]
  InvalidFormat(&'static str),

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

  #[error("worker closed")]
  WorkerClosed,

  #[error("flush failed")]
  FlushFailed,

  #[error("buffered write failed")]
  BufferedWriteFailed,

  #[error("write conflict detected")]
  WriteConflict,

  #[error("thread conflict detected")]
  ThreadConflict,

  #[error("channel disconnected")]
  ChannelDisconnected,

  #[error("panic")]
  Panic(Box<dyn std::any::Any + Send>),
}
impl Error {
  pub fn unknown<E>(err: E) -> Self
  where
    E: std::error::Error + Send + Sync + 'static,
  {
    Self::Unknown(Box::new(err))
  }
}

pub type Result<T = ()> = std::result::Result<T, Error>;
