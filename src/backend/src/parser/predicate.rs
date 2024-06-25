use crate::{planning::plan::Plan, scan::scan::Scan};

use super::term::Term;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new() -> Self {
        Self { terms: vec![] }
    }

    pub fn from_terms(terms: Vec<Term>) -> Self {
        Self { terms }
    }

    pub fn from_term(term: Term) -> Self {
        Self { terms: vec![term] }
    }

    pub fn conjoin_with(&mut self, other: &mut Predicate) {
        self.terms.append(&mut other.terms);
    }

    pub fn is_satisfied(&self, scan: &dyn Scan) -> bool {
        for t in &self.terms {
            if !t.is_satisfied(scan) {
                return false;
            }
        }

        true
    }

    pub fn reduction_factor(&self, plan: &dyn Plan) -> u64 {
        1
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        metadata::metadata_manager::MetadataManager,
        parser::{constant::Value, expression::Expression},
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::*;

    #[test]
    fn test_predicate() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let metadata_manager = MetadataManager::new(&tx);

        let lhs1 = Expression::Field("sname".to_string());
        let rhs1 = Expression::Constant(Value::Varchar("joe".to_string()));
        let t1 = Term::new(lhs1, rhs1);

        //let lhs2 = Expression::FieldName("major_id".to_string());
        //let rhs2 = Expression::FieldName("did".to_string());
        //let t2 = Term::new(lhs2, rhs2);

        let mut pred1 = Predicate::from_term(t1);
        //let mut pred2 = Predicate::from_term(t2);
        //pred1.conjoin_with(&mut pred2);

        let mut tx = Arc::new(Mutex::new(db.new_tx()));
        let layout = metadata_manager.get_table_layout("student", &tx).unwrap();
        let mut scan = Box::new(TableScan::new(tx, layout, "student"));

        scan.next();
        assert!(pred1.is_satisfied(&*scan));
    }
}
