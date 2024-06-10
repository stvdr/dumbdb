use crate::schema::Schema;

use super::{constant::Constant, expression::Expression, scan::Scan};

pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }

    pub fn is_satisfied(&self, scan: &dyn Scan) -> bool {
        let lhs_val = self.lhs.evaluate(scan);
        let rhs_val = self.rhs.evaluate(scan);
        lhs_val == rhs_val
    }

    pub fn applies_to(&self, schema: &Schema) -> bool {
        self.lhs.applies_to(schema) && self.rhs.applies_to(schema)
    }

    // TODO
    //pub fn reduction_factor(&self, plan: Plan) -> i32 {
    //}

    // TODO
    //pub fn equates_with_constant(&self, field_name: &str) -> Optional<Constant> {}

    // TODO
    //pub fn equates_with_field(&self, field_name: &str) -> Optional<String> {}
}
