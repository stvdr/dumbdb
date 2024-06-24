use std::sync::{Arc, Mutex};

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
    metadata_mgr: Arc<Mutex<MetadataManager>>,
}

impl BasicQueryPlanner {
    pub fn new(metadata_mgr: Arc<Mutex<MetadataManager>>) -> Self {
        Self { metadata_mgr }
    }
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan<const P: usize>(
        &mut self,
        data: &SelectNode,
        tx: Arc<Mutex<Transaction<P>>>,
    ) -> Result<Box<dyn Plan>, String> {
        let mut plans = vec![];
        for tblname in &data.tables {
            let view_def = self
                .metadata_mgr
                .lock()
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
                    let mut locked_mgr = self.metadata_mgr.lock().unwrap();
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
    use super::BasicQueryPlanner;

    #[test]
    fn test_build_basic_plan() {
        //let _ = env_logger::try_init();
        //let planner = BasicQueryPlanner::new();
    }
}
