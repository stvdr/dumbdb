use byteorder::{ByteOrder, LittleEndian};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

const HEADER_SIZE: u64 = 1024;

pub trait WriteTypeToPage<const P: usize> {
    fn write(&self, page: &mut Page<P>, offset: usize) -> usize;
}

pub trait ReadTypeFromPage<'a, const P: usize> {
    fn read(page: &'a Page<P>, offset: usize) -> Self;
}

macro_rules! impl_endian_io_traits {
    ($t:ty, $write_fn:ident, $read_fn:ident) => {
        impl<const P: usize> WriteTypeToPage<P> for $t {
            fn write(&self, page: &mut Page<P>, offset: usize) -> usize {
                let size = size_of::<Self>();
                let end = offset + size;
                LittleEndian::$write_fn(&mut page.data[offset..end], *self);
                size
            }
        }

        impl<const P: usize> ReadTypeFromPage<'_, P> for $t {
            fn read(page: &Page<P>, offset: usize) -> Self {
                let size = size_of::<Self>();
                LittleEndian::$read_fn(&page.data[offset..offset + size])
            }
        }
    };
}

impl<const P: usize> WriteTypeToPage<P> for &str {
    fn write(&self, page: &mut Page<P>, offset: usize) -> usize {
        assert!(self.is_ascii(), "strings must be ASCII");

        let bytes = self.as_bytes();
        let len = bytes.len() as u32;
        assert!((offset + size_of::<u32>() + len as usize) <= P);

        page.data[offset..offset + size_of::<u32>()].copy_from_slice(&len.to_be_bytes());

        if len > 0 {
            page.data[offset + size_of::<u32>()..offset + size_of::<u32>() + len as usize]
                .copy_from_slice(bytes);
        }
        size_of::<u32>() + len as usize
    }
}

impl<const P: usize> ReadTypeFromPage<'_, P> for String {
    fn read(page: &Page<P>, offset: usize) -> String {
        // Read the bytes that indicate the length of the string
        let len_bytes = &page.data[offset..offset + size_of::<u32>()];

        // Convert the length into a primitive
        let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;

        if len == 0 {
            return String::new();
        }

        // Read the bytes that define the string
        let str_bytes = &page.data[offset + size_of::<u32>()..offset + size_of::<u32>() + len];

        // TODO: error checking
        String::from_utf8(str_bytes.to_vec()).expect("unable to create string from bytes")
    }
}

impl_endian_io_traits!(u16, write_u16, read_u16);
impl_endian_io_traits!(i16, write_i16, read_i16);
impl_endian_io_traits!(u32, write_u32, read_u32);
impl_endian_io_traits!(i32, write_i32, read_i32);
impl_endian_io_traits!(u64, write_u64, read_u64);
impl_endian_io_traits!(i64, write_i64, read_i64);

// Page is a block that has been pulled into a memory buffer.
#[derive(Debug)]
pub struct Page<const P: usize> {
    data: [u8; P],
}

impl<const P: usize> Page<P> {
    /// Create a new Page with all data initialized to 0.
    pub fn new() -> Self {
        Page { data: [0; P] }
    }

    pub fn raw(&self) -> [u8; P] {
        return self.data;
    }

    /// Write data to a page at the provided offset and return the number of bytes written.    
    ///
    /// # Arguments
    ///
    /// * `data` - Data to be written to the page.
    /// * `offset` - The offset in the page where data will be written.
    pub fn write<T: WriteTypeToPage<P>>(&mut self, data: T, offset: usize) -> usize {
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

    pub fn read<'a, T: ReadTypeFromPage<'a, P>>(&'a self, offset: usize) -> T {
        T::read(self, offset)
    }

    pub fn read_bytes<'a>(&'a self, offset: usize, length: usize) -> &'a [u8] {
        &self.data[offset..offset + length]
    }
}

// BlockId points to a block's location on disk.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockId {
    file_id: String,
    num: u64,
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}/{}]", self.file_id, self.num)
    }
}

impl BlockId {
    /// Create a new BlockId
    ///
    /// # Arguments
    ///
    /// * `file_id` - The file name where the block will be stored
    /// * `num` - The index in the file where the block lives
    pub fn new(file_id: &str, num: u64) -> Self {
        BlockId {
            file_id: file_id.to_string(),
            num,
        }
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn num(&self) -> u64 {
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

    //pub fn serialize(&self) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
    //    // TODO: error handling
    //    bincode::serialize(self)
    //}

    //pub fn deserialize(bytes: &[u8]) -> Result<BlockId, Box<bincode::ErrorKind>> {
    //    bincode::deserialize(bytes)
    //}
}

impl Default for BlockId {
    fn default() -> Self {
        Self {
            file_id: String::new(),
            num: 0,
        }
    }
}

pub struct FileManager<const P: usize> {
    files: RwLock<HashMap<String, Arc<Mutex<File>>>>,
    root_directory: PathBuf,
    page_size: usize,
}

impl<const P: usize> std::fmt::Debug for FileManager<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileManager")
            .field("files", &self.files)
            .field("root_directory", &self.root_directory)
            .finish()
    }
}

impl<const P: usize> FileManager<P> {
    pub type Page = Page<P>;

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
            page_size: P,
        }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    fn get_file_position(bid: &BlockId) -> u64 {
        (bid.num * P as u64 + HEADER_SIZE) as u64
    }

    fn get_block_file(&self, file_id: &str) -> PathBuf {
        self.root_directory.join(file_id)
    }

    pub fn get_block(&self, bid: &BlockId, page: &mut Self::Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(bid);
        let file = self.get_or_create_file(&bid.file_id);

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
        if seek_position + page.data.len() as u64 <= file.metadata()?.len() {
            file.seek(SeekFrom::Start(seek_position))?;
            file.read_exact(&mut page.data)?;
        }

        Ok(())
    }

    /// Write data in the provided page to a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId that identifies where the page should be written.
    /// * `page` - The page that will be written.
    pub fn write_block(&self, blk: &BlockId, page: &Self::Page) -> Result<(), Error> {
        let seek_position = Self::get_file_position(blk);
        let file;
        {
            let files = self.files.read().unwrap();
            file = files
                .get(&blk.file_id)
                .expect(&format!("file '{}' not found", blk.file_id))
                .clone();
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
    /// Append the provided page to the file identified by the file_id
    pub fn append_block(&self, file_id: &str, page: &Self::Page) -> Result<BlockId, Error> {
        let file = self.get_or_create_file(file_id);
        let mut file = file.lock().unwrap();
        let block_start = file.seek(SeekFrom::End(0))?;
        let block_number = (block_start - HEADER_SIZE) / P as u64;
        file.write_all(&page.data)?;
        file.sync_all()?;

        Ok(BlockId {
            file_id: file_id.to_string(),
            num: block_number,
        })
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
        Ok((file_size - 1) / P as u64)
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

    fn setup() -> (TempDir, FileManager<4096>) {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().join("data");
        fs::create_dir_all(&root_dir).expect("Failed to create root directory");
        (temp_dir, FileManager::new(&root_dir))
    }

    #[test]
    fn test_write_primitive() {
        let mut page = Page::<4096>::new();

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
        let mut page = Page::<4096>::new();
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
                let mut page = Page::<4096>::new();
                page.data = [b; 4096];

                // Append a new block
                let block_id = file_mgr.append_block(&file_name, &page).unwrap();
                file_mgr.get_block(&block_id, &mut page).unwrap();
                assert_eq!(block_id.num, b as u64);
                assert_eq!(block_id.file_id, file_name);
                assert_eq!(page.data, [b; 4096]);

                // Write over the appended block
                page.data = [b + 100; 4096];
                file_mgr.write_block(&block_id, &page).unwrap();

                // Read the re-written block into a new page
                let mut new_page = Page::<4096>::new();
                file_mgr.get_block(&block_id, &mut new_page).unwrap();
                assert_eq!(page.data, new_page.data);
            }

            assert_eq!(3, file_mgr.length(&file_name).unwrap());
        }
    }

    #[test]
    fn test_write_string_to_page() {
        let mut page = Page::<4096>::new();
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
