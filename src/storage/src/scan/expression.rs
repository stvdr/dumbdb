use std::fmt::Display;

use crate::schema::Schema;

use super::{constant::Constant, scan::Scan};

#[derive(Clone)]
pub enum Expression {
    FieldName(String),
    Value(Constant),
}

impl Expression {
    pub fn evaluate(&self, scan: &dyn Scan) -> Constant {
        match &self {
            Self::FieldName(field_name) => scan.get_val(field_name).expect("invalid"),
            Self::Value(constant) => constant.clone(),
        }
    }

    pub fn applies_to(&self, schema: &Schema) -> bool {
        match self {
            Self::FieldName(field_name) => schema.has_field(field_name),
            Self::Value(_) => true,
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldName(field_name) => write!(f, "{}", field_name),
            Self::Value(constant) => write!(f, "{}", constant.to_string()),
        }
    }
}
