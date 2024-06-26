use std::fmt::Display;

use crate::{scan::scan::Scan, schema::Schema};

use super::{constant::Value, parser::FieldName};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression {
    Field(FieldName),
    Constant(Value),
}

impl Expression {
    pub fn evaluate(&self, scan: &dyn Scan) -> Value {
        // TODO: error checking
        match &self {
            Self::Field(field_name) => scan
                .get_val(field_name)
                .expect(&format!("field '{}' does not exist", field_name)),
            Self::Constant(val) => val.clone(),
        }
    }

    pub fn applies_to(&self, schema: &Schema) -> bool {
        match self {
            Self::Field(field_name) => schema.has_field(field_name),
            Self::Constant(_) => true,
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Field(field_name) => write!(f, "{}", field_name),
            Self::Constant(val) => write!(f, "{}", val),
        }
    }
}
