use anyhow::{Result, bail, ensure};
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    logical_expr::LogicalPlan,
    sql::sqlparser::{
        ast::{Statement, Visit, Visitor},
        dialect::GenericDialect,
        parser::Parser,
    },
};
use std::ops::ControlFlow;
pub fn validate_sql(sql: &str) -> Result<()> {
    ensure!(sql.len() <= 1024 * 1024, "SQL exceeds 1 MiB");
    let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
    ensure!(statements.len() == 1, "exactly one query required");
    struct ReadOnly;
    impl Visitor for ReadOnly {
        type Break = ();
        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            match statement {
                Statement::Query(_) | Statement::Explain { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(()),
            }
        }
    }
    ensure!(
        matches!(statements.visit(&mut ReadOnly), ControlFlow::Continue(())),
        "nested statements must be read-only queries"
    );
    match &statements[0] {
        Statement::Query(_) => Ok(()),
        Statement::Explain { statement, .. }
            if matches!(statement.as_ref(), Statement::Query(_)) =>
        {
            Ok(())
        }
        _ => bail!("Neura accepts SELECT and EXPLAIN SELECT only"),
    }
}
pub fn validate_plan(plan: &LogicalPlan) -> Result<()> {
    plan.apply(|p| {
        match p {
            LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Statement(_) => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "read-only query required".into(),
                ));
            }
            _ => {}
        };
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_mutations() {
        for q in [
            "DROP TABLE t",
            "COPY t TO '/tmp/x'",
            "SELECT 1; DELETE FROM t",
            "EXPLAIN INSERT INTO t VALUES (1)",
            "SET x=1",
            "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
        ] {
            assert!(validate_sql(q).is_err(), "{q}");
        }
    }
    #[test]
    fn permits_cte() {
        validate_sql("WITH x AS (SELECT 1 AS n) SELECT n FROM x").unwrap();
    }
}
