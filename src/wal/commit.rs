pub struct CommitInfo {
  pub tx_id: usize,
  pub commit_index: usize,
}

impl CommitInfo {
  pub fn new(tx_id: usize, commit_index: usize) -> Self {
    Self {
      tx_id,
      commit_index,
    }
  }
}
