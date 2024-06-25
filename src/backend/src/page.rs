use byteorder::{ByteOrder, LittleEndian};
use std::mem::size_of;

pub const PAGE_SIZE: usize = 4096;

/// Page is a block that has been pulled into a memory buffer.
#[derive(Debug)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

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

impl WriteTypeToPage for &str {
    fn write(&self, page: &mut Page, offset: usize) -> usize {
        assert!(self.is_ascii(), "strings must be ASCII");

        let bytes = self.as_bytes();
        let len = bytes.len() as u32;
        assert!((offset + size_of::<u32>() + len as usize) <= PAGE_SIZE);

        page.data[offset..offset + size_of::<u32>()].copy_from_slice(&len.to_be_bytes());

        if len > 0 {
            page.data[offset + size_of::<u32>()..offset + size_of::<u32>() + len as usize]
                .copy_from_slice(bytes);
        }
        size_of::<u32>() + len as usize
    }
}

impl ReadTypeFromPage<'_> for String {
    fn read(page: &Page, offset: usize) -> String {
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

impl Page {
    /// Create a new Page with all data initialized to 0.
    pub fn new() -> Self {
        Page {
            data: [0; PAGE_SIZE],
        }
    }

    pub fn raw(&self) -> [u8; PAGE_SIZE] {
        return self.data;
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
