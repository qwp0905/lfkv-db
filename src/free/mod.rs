use std::sync::Mutex;

pub struct FreeList(Mutex<FreeListInner>);
impl FreeList {}

struct FreeListInner {}
