use std::{collections::HashMap, mem};

use crate::schema::Schema;

// Space is allocated at the beginning for metadata, e.g. "inuse"
static LAYOUT_START: u64 = mem::size_of::<u32>() as u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, u64>,
    slot_size: u64,
}

impl Layout {
    pub fn new(schema: Schema, offsets: HashMap<String, u64>, slot_size: u64) -> Self {
        Self {
            schema,
            offsets,
            slot_size,
        }
    }

    pub fn from_schema(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut pos = LAYOUT_START;
        for field_name in schema.fields().iter() {
            offsets.insert(field_name.clone(), pos);

            let len = Self::byte_length(
                schema.get_field_type(field_name).unwrap(),
                schema.get_field_length(field_name).unwrap(),
            );
            pos += len;
        }

        Self::new(schema, offsets, pos)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, field_name: &str) -> u64 {
        // TODO: error handling
        *self.offsets.get(field_name).expect(&format!(
            "field '{}' does not exist in {:?}",
            field_name,
            self.schema()
        ))
    }

    pub fn slot_size(&self) -> u64 {
        self.slot_size
    }

    // TODO: at the moment this is just assuming that strings are ASCII.
    pub fn byte_length(field_type: i32, field_length: u64) -> u64 {
        match field_type {
            0 => mem::size_of::<i32>() as u64,
            // TODO: At the moment it is assumed that string are basic ASCII
            1 => mem::size_of::<u64>() as u64 + (field_length * 1),
            _ => panic!("Unknown field type: {field_type}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use crate::{layout::LAYOUT_START, make_schema};

    use super::Layout;

    #[test]
    fn test_layout() {
        let layout = Layout::from_schema(make_schema! {
            "dataval" => i32,
            "block" => i32,
            "id" => i32
        });

        assert_eq!(layout.offset("dataval"), LAYOUT_START);
        assert_eq!(
            layout.offset("block"),
            LAYOUT_START + TryInto::<u64>::try_into(size_of::<i32>()).unwrap()
        );
        assert_eq!(
            layout.offset("id"),
            LAYOUT_START + TryInto::<u64>::try_into(size_of::<i32>()).unwrap() * 2
        );
    }
}
