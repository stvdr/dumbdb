// use crate::file_manager::{BlockId, FileManager, Page, PAGE_SIZE};
//
// pub struct Buffer {
//     page: Page,
//     block_id: BlockId,
//     pin_count: u64,
// }
//
// impl Default for Buffer {
//     fn default() -> Self {
//         Self {
//             page: Page::new(),
//             block_id: BlockId::default(),
//             pin_count: 0,
//         }
//     }
// }
//
// pub struct BufferManager<'a> {
//     buffers: Vec<Buffer>,
//     file_manager: &'a FileManager,
// }
//
// impl<'a> BufferManager<'a> {
//     pub fn new(size: usize, file_manager: &'a FileManager) -> Self {
//         let num_buffers = size / PAGE_SIZE;
//         Self {
//             file_manager,
//             buffers: (0..num_buffers).map(|| {
//                 Buffer::default()
//             }).collect()
//         }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use std::fs;
//     use super::*;
//     use tempfile::{tempdir, TempDir};
//
//     fn setup() -> BufferManager {
//         let temp_dir = tempdir().unwrap();
//         let root_dir = temp_dir.path().join("data");
//         fs::create_dir_all(&root_dir).expect("Failed to create root directory");
//         let file_manager = FileManager::new(&root_dir);
//
//         BufferManager::new(10, &file_manager)
//
//     }
//
//     #[test]
//     fn test_create_new() {
//         let buf_mgr = setup();
//     }
// }
//
