use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

pub const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = PAGE_SIZE;

pub trait WriteTypeToPage {
    fn write(&self, page: &mut Page, offset: usize) -> usize;
}

pub trait ReadTypeFromPage<'a> {
    fn read(page: &'a Page, offset: usize) -> Self;
}

macro_rules! impl_endian_io_traits {
    ($t:ty, $write_fn:ident, $read_fn:ident) => {
        impl WriteTypeToPage for $t {
            fn write(&self, page: &mut Page, offset: usize) -> usize {
                let size = size_of::<Self>();
                let end = offset + size;
                LittleEndian::$write_fn(&mut page.data[offset..end], *self);
                size
            }
        }

        impl ReadTypeFromPage<'_> for $t {
            fn read(page: &Page, offset: usize) -> Self {
                let size = size_of::<Self>();
                LittleEndian::$read_fn(&page.data[offset..offset + size])
            }
        }
    };
}

impl_endian_io_traits!(u16, write_u16, read_u16);
impl_endian_io_traits!(i16, write_i16, read_i16);
impl_endian_io_traits!(u32, write_u32, read_u32);
impl_endian_io_traits!(i32, write_i32, read_i32);
impl_endian_io_traits!(u64, write_u64, read_u64);
impl_endian_io_traits!(i64, write_i64, read_i64);

// Page is a block that has been pulled into a memory buffer.
#[derive(Debug)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: [0; PAGE_SIZE],
        }
    }

    /// Write data to a page at the provided offset and return the number of bytes written.    
    ///
    /// # Arguments
    ///
    /// * `data` - Data to be written to the page.
    /// * `offset` - The offset in the page where data will be written.
    pub fn write<T: WriteTypeToPage>(&mut self, data: T, offset: usize) -> usize {
        data.write(self, offset)
    }

    /// Write bytes to a page at the provided offset and return the number of bytes written.    
    ///
    /// # Arguments
    ///
    /// * `data` - Data to be written to the page.
    /// * `offset` - The offset in the page where data will be written.
    pub fn write_bytes(&mut self, data: &[u8], offset: usize) -> usize {
        self.data[offset..offset + data.len()].copy_from_slice(data);
        data.len()
    }

    pub fn read<'a, T: ReadTypeFromPage<'a>>(&'a self, offset: usize) -> T {
        T::read(self, offset)
    }

    pub fn read_bytes<'a>(&'a self, offset: usize, length: usize) -> &'a [u8] {
        &self.data[offset..offset + length]
    }
}

// BlockId points to a block's location on disk.
#[derive(Debug, PartialEq, Eq)]
pub struct BlockId {
    file_id: String,
    num: usize,
}

impl BlockId {
    pub fn new(file_id: &str, n: usize) -> Self {
        BlockId {
            file_id: file_id.to_string(),
            num: n,
        }
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn num(&self) -> usize {
        self.num
    }

    pub fn previous(&self) -> Option<BlockId> {
        match self.num {
            0 => None,
            _ => Some(BlockId {
                file_id: self.file_id.clone(),
                num: self.num - 1,
            }),
        }
    }

    pub fn next(&self) -> BlockId {
        BlockId {
            file_id: self.file_id.clone(),
            num: self.num + 1,
        }
    }
}

impl Default for BlockId {
    fn default() -> Self {
        Self {
            file_id: String::new(),
            num: 0,
        }
    }
}

pub struct FileManager {
    files: RwLock<HashMap<String, Arc<Mutex<File>>>>,
    root_directory: PathBuf,
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
        }
    }

    fn get_file_position(bid: &BlockId) -> u64 {
        (bid.num * PAGE_SIZE + HEADER_SIZE) as u64
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
        file.sync_data()?;

        Ok(())
    }

    // TODO: proper error handling
    pub fn append_block(&self, file_id: &str, page: &Page) -> Result<BlockId, Error> {
        let file;

        {
            let mut files = self.files.write().unwrap();
            file = files
                .entry(file_id.to_string())
                .or_insert_with_key(|f| {
                    let file_path = self.get_block_file(f);

                    // TODO: error handling inside closure
                    let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(file_path)
                        .unwrap();

                    // Write the header to the file
                    let buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
                    file.write_all(&buf).unwrap();

                    Arc::new(Mutex::new(file))
                })
                .clone();
        }

        let mut file = file.lock().unwrap();
        let block_start = file.seek(SeekFrom::End(0))?;
        let block_number = (block_start as usize - HEADER_SIZE) / PAGE_SIZE;
        file.write_all(&page.data)?;
        file.sync_all()?;

        Ok(BlockId {
            file_id: file_id.to_string(),
            num: block_number,
        })
    }

    pub fn num_blocks(&self, file_id: &str) -> Result<usize, Error> {
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

        let mut file = file.lock().unwrap();

        let file_size = file.metadata().unwrap().len() as usize;
        Ok((file_size - 1) / PAGE_SIZE)
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
            assert_eq!(file_mgr.num_blocks(&file_name).unwrap(), 0);
            for b in 0..3u8 {
                let mut page = Page::new();
                page.data = [b; PAGE_SIZE];

                // Append a new block
                let block_id = file_mgr.append_block(&file_name, &page).unwrap();
                file_mgr.get_block(&block_id, &mut page).unwrap();
                assert_eq!(block_id.num, b as usize);
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
