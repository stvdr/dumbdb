use core::fmt;

use serde::{Deserialize, Serialize};

// TODO: change `file_id` to an integer identifier and make BlockId derive `Copy`
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
