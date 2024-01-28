use crate::disk::PageSeeker;

pub struct UndoLog {
  disk: PageSeeker,
}
