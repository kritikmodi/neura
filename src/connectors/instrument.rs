use crate::metrics::Metrics;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Statistics, config::ConfigOptions},
    error::Result,
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream,
        filter_pushdown::{FilterDescription, FilterPushdownPhase},
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc};
#[derive(Clone)]
pub struct MeasuredTable {
    pub inner: Arc<dyn TableProvider>,
    pub metrics: Metrics,
    pub name: String,
}
impl fmt::Debug for MeasuredTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MeasuredTable({})", self.name)
    }
}
#[async_trait]
impl TableProvider for MeasuredTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(MeasuredExec {
            child,
            metrics: self.metrics.clone(),
            name: self.name.clone(),
        }))
    }
}
struct MeasuredExec {
    child: Arc<dyn ExecutionPlan>,
    metrics: Metrics,
    name: String,
}
impl fmt::Debug for MeasuredExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MeasuredExec({})", self.name)
    }
}
impl DisplayAs for MeasuredExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SourceScan: {}", self.name)
    }
}
impl ExecutionPlan for MeasuredExec {
    fn name(&self) -> &str {
        "SourceScan"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(super::df_error("SourceScan needs one child"));
        }
        Ok(Arc::new(Self {
            child: children[0].clone(),
            metrics: self.metrics.clone(),
            name: self.name.clone(),
        }))
    }
    fn partition_statistics(&self, p: Option<usize>) -> Result<Statistics> {
        self.child.partition_statistics(p)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // Instrumentation is transparent to both static and runtime predicates.
        FilterDescription::from_children(filters, &self.children())
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.child
            .try_swapping_with_projection(projection)?
            .map(|child| {
                Ok(Arc::new(Self {
                    child,
                    metrics: self.metrics.clone(),
                    name: self.name.clone(),
                }) as Arc<dyn ExecutionPlan>)
            })
            .transpose()
    }
    fn execute(&self, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut input = self.child.execute(p, ctx)?;
        let schema = input.schema();
        let metrics = self.metrics.clone();
        let name = self.name.clone();
        let s = async_stream::try_stream! {let _timer=metrics.timer(&name);while let Some(b)=input.next().await{let b=b?;metrics.update(&name,|m|{m.rows+=b.num_rows() as u64;m.arrow_bytes+=b.get_array_memory_size() as u64;});yield b;}};
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}
