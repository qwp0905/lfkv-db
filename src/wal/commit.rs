pub struct CommitInfo {
  pub tx_id: usize,
  pub log_index: usize,
}

impl CommitInfo {
  pub fn new(tx_id: usize, log_index: usize) -> Self {
    Self { tx_id, log_index }
  }
}
