use std::sync::{Arc, Mutex, RwLock};

use crate::{
    metadata::metadata_manager::MetadataManager,
    parser::{
        lexer::Lexer,
        parser::{Parser, RootNode, SelectNode},
    },
    transaction::Transaction,
};

use super::{
    plan::Plan, product_plan::ProductPlan, project_plan::ProjectPlan, query_planner::QueryPlanner,
    select_plan::SelectPlan, table_plan::TablePlan,
};

struct BasicQueryPlanner {
    metadata_mgr: Arc<RwLock<MetadataManager>>,
}

impl BasicQueryPlanner {
    pub fn new(metadata_mgr: Arc<RwLock<MetadataManager>>) -> Self {
        Self { metadata_mgr }
    }
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan(
        &self,
        data: &SelectNode,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Box<dyn Plan>, String> {
        let mut plans = vec![];
        for tblname in &data.tables {
            let view_def = self
                .metadata_mgr
                .read()
                .unwrap()
                .get_view_def(&tblname, &tx);

            // Check whether the table name matches a view definition
            let plan = match view_def {
                Some(def) => {
                    let lexer = Lexer::new(&def);
                    let mut parser = Parser::new(lexer);
                    let ast = parser.parse()?;
                    match ast {
                        RootNode::Select(node) => self.create_plan(&node, tx.clone())?,
                        _ => {
                            return Err(format!(
                            "did not find SELECT query statement in view defined with name '{}'",
                            tblname))
                        }
                    }
                }
                None => {
                    let mut locked_mgr = self.metadata_mgr.write().unwrap();
                    Box::new(TablePlan::new(tx.clone(), tblname, &mut locked_mgr))
                }
            };

            plans.push(plan);
        }

        let mut iter = plans.into_iter();
        let first_plan = iter
            .next()
            .ok_or_else(|| "unable to parse any plans".to_string())?;
        let mut plan = iter.fold(first_plan, |acc, next| {
            Box::new(ProductPlan::new(acc, next))
        });

        if let Some(pred) = &data.predicate {
            plan = Box::new(SelectPlan::new(plan, pred.clone()));
        }

        Ok(Box::new(ProjectPlan::new(plan, data.fields.clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        parser::{
            lexer::Lexer,
            parser::{Parser, RootNode, SelectNode},
        },
        planning::query_planner::QueryPlanner,
        tests::test_utils::{create_default_tables, default_test_db, test_db},
    };

    use super::BasicQueryPlanner;

    #[test]
    fn test_build_basic_plan() {
        let temp_dir = tempdir().unwrap();
        let mut db = default_test_db(&temp_dir);
        create_default_tables(&mut db);

        let mut planner = BasicQueryPlanner::new(db.metadata_manager());

        let lexer = Lexer::new("SELECT sid, sname, grad_year FROM student WHERE sid = 4");
        let mut parser = Parser::new(lexer);
        let ast = parser.parse().unwrap();

        let tx = Arc::new(Mutex::new(db.new_tx()));
        if let RootNode::Select(sel) = ast {
            let mut plan = planner.create_plan(&sel, tx).unwrap();
            let mut scan = plan.open();
            scan.next();
            assert_eq!(4, scan.get_int("sid").unwrap());
            assert_eq!("sue", scan.get_string("sname").unwrap());
            assert_eq!(2022, scan.get_int("grad_year").unwrap());
            assert!(!scan.next());
        } else {
            panic!("failed to parse select statement");
        }
    }
}
