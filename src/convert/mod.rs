mod decoded;
mod hybrid;
mod raw;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
pub use decoded::*;
pub use hybrid::*;
pub use raw::*;

use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use ethers::prelude::*;

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait Transcoder {
    fn schema(&self) -> SchemaRef;
    fn append(&mut self, logs: &[Log]) -> Result<(), AppendError>;
    fn len(&self) -> usize;
    fn finish(&mut self) -> RecordBatch;
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error(transparent)]
    EventDecodingError(#[from] alloy_core::dyn_abi::Error),
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Analyzes the SQL and returns a pushed-down filter that will be used when querying logs from the ETH node
pub async fn sql_to_pushdown_filter(ctx: &SessionContext, sql: &str) -> DfResult<Option<Filter>> {
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    Ok(sql_to_pushdown_filter_rec(plan.as_ref()))
}

fn sql_to_pushdown_filter_rec(plan: &dyn ExecutionPlan) -> Option<Filter> {
    let mut found = plan
        .as_any()
        .downcast_ref::<super::provider::EthGetLogs<Http>>()
        .map(|scan| scan.filter().clone());

    // Traverse all the children too to make sure there is only one scan in this query (e.g. no UNIONs)
    for child in &plan.children() {
        let child = sql_to_pushdown_filter_rec(child.as_ref());
        if child.is_some() {
            if found.is_some() {
                unimplemented!("Multiple table scans in one query are not yet supported");
            }
            found = child;
        }
    }

    found
}
