use byteorder::{ByteOrder, LittleEndian};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::block_id::BlockId;
use crate::page::{Page, PAGE_SIZE};

const HEADER_SIZE: u64 = 1024;

pub struct FileManager {
    files: RwLock<HashMap<String, Arc<Mutex<File>>>>,
    root_directory: PathBuf,
    page_size: usize,
}

impl std::fmt::Debug for FileManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileManager")
            .field("files", &self.files)
            .field("root_directory", &self.root_directory)
            .finish()
    }
}

impl FileManager {
    pub fn new(root_directory: &Path) -> Self {
        if !root_directory.exists() {
            panic!(
                "Directory does not exist: {}",
                root_directory.to_string_lossy()
            );
        }

        Self {
            files: RwLock::new(HashMap::new()),
            root_directory: root_directory.to_path_buf(),
            page_size: PAGE_SIZE,
        }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    fn get_file_position(bid: &BlockId) -> u64 {
        (bid.num() * PAGE_SIZE as u64 + HEADER_SIZE) as u64
    }

    fn get_block_file(&self, file_id: &str) -> PathBuf {
        self.root_directory.join(file_id)
    }

    pub fn get_block(&self, bid: &BlockId, page: &mut Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(bid);
        let file = self.get_or_create_file(&bid.file_id());

        //{
        //    let files = self.files.read().unwrap();
        //    file = files
        //        .get(&bid.file_id)
        //        .expect(&format!("file '{}' not found", bid.file_id))
        //        .clone();
        //}

        // TODO: more research on what to do with poison errors
        let mut file = file.lock().unwrap();

        //assert!(seek_position + page.data.len() as u64 <= file.metadata()?.len());
        if seek_position + PAGE_SIZE as u64 <= file.metadata()?.len() {
            file.seek(SeekFrom::Start(seek_position))?;
            file.read_exact(page.raw_mut())?;
        }

        Ok(())
    }

    /// Write data in the provided page to a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId that identifies where the page should be written.
    /// * `page` - The page that will be written.
    pub fn write_block(&self, blk: &BlockId, page: &Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(blk);
        let file;
        {
            let files = self.files.read().unwrap();
            file = files
                .get(blk.file_id())
                .expect(&format!("file '{}' not found", blk.file_id()))
                .clone();
        }

        let mut file = file.lock().unwrap();

        assert!(seek_position + PAGE_SIZE as u64 <= file.metadata()?.len());

        file.seek(SeekFrom::Start(seek_position))?;
        file.write_all(page.raw())?;
        file.flush()?;
        file.sync_data()?;

        Ok(())
    }

    // TODO: proper error handling
    /// Append the provided page to the file identified by the file_id
    pub fn append_block(&self, file_id: &str, page: &Page) -> Result<BlockId, Error> {
        let file = self.get_or_create_file(file_id);
        let mut file = file.lock().unwrap();
        let block_start = file.seek(SeekFrom::End(0))?;
        let block_number = (block_start - HEADER_SIZE) / PAGE_SIZE as u64;
        file.write_all(page.raw())?;
        file.sync_all()?;

        Ok(BlockId::new(file_id, block_number))
    }

    /// Get the number of blocks in a file.
    pub fn length(&self, file_id: &str) -> Result<u64, Error> {
        let file = {
            let files = self.files.read().unwrap();
            match files.get(file_id) {
                None => {
                    // The file is not currently managed by the filemanager
                    return Ok(0);
                }
                Some(f) => f.clone(),
            }
        };

        let file = file.lock().unwrap();

        let file_size = file.metadata().unwrap().len();
        Ok((file_size - 1) / PAGE_SIZE as u64)
    }

    fn get_or_create_file(&self, file_id: &str) -> Arc<Mutex<File>> {
        let mut files = self.files.write().unwrap();
        files
            .entry(file_id.to_string())
            .or_insert_with_key(|f| {
                let file_path = self.get_block_file(f);

                // TODO: error handling
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(file_path.clone())
                    .expect(&format!(
                        "Unable to open file path: {}",
                        file_path.to_string_lossy()
                    ));

                // Add a header to the file for storing metadata
                let buf = [0; HEADER_SIZE as usize];
                file.write_all(&buf).unwrap();

                Arc::new(Mutex::new(file))
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::{tempdir, TempDir};

    fn setup() -> (TempDir, FileManager) {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().join("data");
        fs::create_dir_all(&root_dir).expect("Failed to create root directory");
        (temp_dir, FileManager::new(&root_dir))
    }

    #[test]
    fn test_write_primitive() {
        let mut page = Page::new();

        let mut offset = 0;
        for i in 1..10u32 {
            let previous_offset = offset;
            let n = page.write(i, offset);

            offset += n;

            assert_eq!(offset, size_of::<u32>() * i as usize);
            assert_eq!(page.read::<u32>(previous_offset), i);
        }

        // Hardcoded check to make sure that the above logic is sane
        assert_eq!(offset, 9 * size_of::<u32>());
    }

    #[test]
    fn test_write_bytes() {
        let mut page = Page::new();
        let bytes = [42u8; 64];

        let n = page.write_bytes(&bytes[..], 0);

        let reread = page.read_bytes(0, n);

        assert_eq!(n, 64);
        assert_eq!(bytes, reread);
    }

    #[test]
    fn test_append_read_write_multiple_files_serial() {
        let (_temp_dir, file_mgr) = setup();

        for f in 1..4 {
            let file_name = format!("file_{}", f);
            assert_eq!(file_mgr.length(&file_name).unwrap(), 0);
            for b in 0..3u8 {
                let mut page = Page::new();
                *page.raw_mut() = [b; PAGE_SIZE];

                // Append a new block
                let block_id = file_mgr.append_block(&file_name, &page).unwrap();
                file_mgr.get_block(&block_id, &mut page).unwrap();
                assert_eq!(block_id.num(), b as u64);
                assert_eq!(block_id.file_id(), file_name);
                assert_eq!(page.raw(), &[b; PAGE_SIZE]);

                // Write over the appended block
                *page.raw_mut() = [b + 100; PAGE_SIZE];
                file_mgr.write_block(&block_id, &page).unwrap();

                // Read the re-written block into a new page
                let mut new_page = Page::new();
                file_mgr.get_block(&block_id, &mut new_page).unwrap();
                assert_eq!(page.raw(), new_page.raw());
            }

            assert_eq!(3, file_mgr.length(&file_name).unwrap());
        }
    }

    #[test]
    fn test_write_string_to_page() {
        let mut page = Page::new();
        let off0 = page.write("first test string", 0);
        assert_eq!(page.read::<String>(0), "first test string");

        let off1 = off0 + page.write("", off0);
        let off2 = off1 + page.write("this is a test string", off1);
        let off3 = off2 + page.write("", off2);
        let off4 = off3 + page.write("", off3);
        let off5 = off4 + page.write("this is another test string", off4);
        page.write("", off5);

        assert_eq!(page.read::<String>(0), "first test string");
        assert_eq!(page.read::<String>(off0), "");
        assert_eq!(page.read::<String>(off1), "this is a test string");
        assert_eq!(page.read::<String>(off2), "");
        assert_eq!(page.read::<String>(off3), "");
        assert_eq!(page.read::<String>(off4), "this is another test string");
        assert_eq!(page.read::<String>(off5), "");
    }

    //#[test]
    //fn test_create_out_of_order_blocks() {
    //    let (_temp_dir, file_mgr) = setup();

    //    let mut page = Page::new();

    //    let _ = file_mgr.get_block(&BlockId::new("file", 4), &mut page);
    //    page.write(42, 32);
    //    let _ = file_mgr.write_block(&BlockId::new("file", 4), &page);
    //}
}
