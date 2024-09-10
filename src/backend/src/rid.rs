use std::fmt::Display;

// TODO: the blocknumber should be a type definition shared by the `BlockId` class
#[derive(Debug, PartialEq, Eq)]
pub struct RID {
    block_num: u64,
    slot: i16,
}

impl RID {
    pub fn new(block_num: u64, slot: i16) -> Self {
        Self { block_num, slot }
    }

    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn slot(&self) -> i16 {
        self.slot
    }
}

impl Clone for RID {
    fn clone(&self) -> Self {
        RID::new(self.block_num, self.slot)
    }
}

impl Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RID({},{})", self.block_num(), self.slot())
    }
}
