use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::macos::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use byteorder::{ByteOrder, LittleEndian};

pub const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = PAGE_SIZE;

pub trait WriteToPage {
    fn write_to_page(&self, page: &mut Page, offset: usize);
}

pub trait ReadFromPage {
    fn read_from_page(&self, page: &Page, offset: usize) -> Self;
}

impl WriteToPage for u32 {
    fn write_to_page(&self, page: &mut Page, offset: usize) {
        let size = size_of::<self>();
        LittleEndian::write_u32(&mut page.data[offset..offset + size], *self);
    }
}

impl ReadFromPage for u32 {
    fn read_from_page(&self, page: &Page, offset: usize) -> u32 {
        let size = size_of::<self>();
        LittleEndian::read_u32(&page.data[offset..offset + size])
    }
}

impl WriteToPage for u64 {
    fn write_to_page(&self, page: &mut Page, offset: usize) {
        let size = size_of::<self>();
        LittleEndian::write_u64(&mut page.data[offset..offset + size], *self);
    }
}

impl ReadFromPage for u64 {
    fn read_from_page(&self, page: &Page, offset: usize) -> u64 {
        let size = size_of::<self>();
        LittleEndian::read_u64(&page.data[offset..offset + size])
    }
}

// Page is a block that has been pulled into a memory buffer.
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: [0; PAGE_SIZE],
        }
    }

    pub fn write_at_offset<T: WriteToPage>(&mut self, data: T, offset: usize) {
        data.write_to_page(&mut self, offset);
    }
}

// BlockId points to a block's location on disk.
#[derive(Debug, PartialEq, Eq)]
pub struct BlockId {
    file_id: String,
    n: usize,
}

impl BlockId {
    pub fn new(file_id: &str, n: usize) -> Self {
        BlockId {
            file_id: file_id.to_string(),
            n,
        }
    }
}

impl Default for BlockId {
    fn default() -> Self {
        Self {
            file_id: String::new(),
            n: 0,
        }
    }
}

pub struct FileManager {
    files: RwLock<HashMap<String, Arc<Mutex<File>>>>,
    root_directory: PathBuf,
}

impl FileManager {
    pub fn new(root_directory: &Path) -> Self {
        if !root_directory.exists() {
            panic!("Directory does not exist: {}", root_directory.to_string_lossy());
        }

        Self {
            files: RwLock::new(HashMap::new()),
            root_directory: root_directory.to_path_buf(),
        }
    }

    fn get_file_position(bid: &BlockId) -> u64 {
        (bid.n * PAGE_SIZE + HEADER_SIZE) as u64
    }

    fn get_block_file(&self, file_id: &str) -> PathBuf {
        self.root_directory.join(file_id)
    }

    pub fn get_block(&self, bid: &BlockId, page: &mut Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(bid);
        let file;

        {
            let files = self.files.read().unwrap();
            file = files.get(&bid.file_id).expect("file not found").clone();
        }

        // TODO: more research on what to do with poison errors
        let mut file = file.lock().unwrap();

        assert!(seek_position + page.data.len() as u64 <= file.metadata()?.len());

        file.seek(SeekFrom::Start(seek_position))?;
        file.read_exact(&mut page.data)?;

        Ok(())
    }

    pub fn write_block(&self, bid: &BlockId, page: &Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(bid);
        let file;
        {
            let files = self.files.read().unwrap();
            file = files.get(&bid.file_id).expect("file not found").clone();
        }

        let mut file = file.lock().unwrap();

        assert!(seek_position + page.data.len() as u64 <= file.metadata()?.len());

        file.seek(SeekFrom::Start(seek_position))?;
        file.write_all(&page.data)?;
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    // TODO: proper error handling
    pub fn append_block(&self, file_id: &str, page: &Page) -> Result<BlockId, Error> {
        let file;

        {
            let mut files = self.files.write().unwrap();
            file = files.entry(file_id.to_string()).or_insert_with_key(|f| {
                let file_path = self.get_block_file(f);

                // TODO: error handling inside closure
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(file_path).unwrap();

                // Write the header to the file
                let buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
                file.write_all(&buf).unwrap();

                Arc::new(Mutex::new(file))
            }).clone();
        }

        let mut file = file.lock().unwrap();
        let block_start = file.seek(SeekFrom::End(0))?;
        let block_number = (block_start as usize - HEADER_SIZE) / PAGE_SIZE;
        file.write_all(&page.data)?;
        file.sync_all()?;

        Ok(BlockId {
            file_id: file_id.to_string(),
            n: block_number,
        })
    }

    pub fn num_blocks(&self, file_id: &str) -> Result<usize, Error> {
        let file;
        {
            let files = self.files.read().unwrap();
            file = files.get(file_id).expect("file not found").clone();
        }

        let mut file = file.lock().unwrap();
        //file.sync_all()?;

        let file_size = file.metadata().unwrap().len() as usize;
        Ok((file_size - 1) / PAGE_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::write;
    use super::*;
    use tempfile::{tempdir, TempDir};

    fn setup() -> (TempDir, FileManager) {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().join("data");
        fs::create_dir_all(&root_dir).expect("Failed to create root directory");
        (temp_dir, FileManager::new(&root_dir))
    }

    #[test]
    fn test_append_read_write_multiple_files_serial() {
        let (_temp_dir, file_mgr) = setup();

        for f in 1..4 {
            let file_name = format!("file_{}", f);
            for b in 0..3u8 {
                let mut page = Page::new();
                page.data = [b; PAGE_SIZE];

                // Append a new block
                let block_id = file_mgr.append_block(&file_name, &page).unwrap();
                file_mgr.get_block(&block_id, &mut page).unwrap();
                assert_eq!(block_id.n, b as usize);
                assert_eq!(block_id.file_id, file_name);
                assert_eq!(page.data, [b; PAGE_SIZE]);

                // Write over the appended block
                page.data = [b + 100; PAGE_SIZE];
                file_mgr.write_block(&block_id, &page).unwrap();

                // Read the re-written block into a new page
                let mut new_page = Page::new();
                file_mgr.get_block(&block_id, &mut new_page).unwrap();
                assert_eq!(page.data, new_page.data);
            }

            assert_eq!(3, file_mgr.num_blocks(&file_name).unwrap());
        }
    }
}

