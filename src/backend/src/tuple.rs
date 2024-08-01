use std::sync::{Arc, RwLock};

use crate::{buffer::Buffer, layout::Layout, page::Page};

// point directly to data in a page, along with the layout that can be used to read it
pub struct Tuple<'a> {
    buffer: Arc<RwLock<Buffer>>,
    offset: usize,
    layout: &'a Layout,
}

impl<'a> Tuple<'a> {
    pub fn new(buffer: Arc<RwLock<Buffer>>, offset: usize, layout: &'a Layout) -> Self {
        //buffer.write().unwrap().pin();

        Self {
            buffer,
            offset,
            layout,
        }
    }
}

//impl<'a> Drop for Tuple<'a> {
//    fn drop(&mut self) {
//        self.buffer.write().unwrap().unpin();
//    }
//}
