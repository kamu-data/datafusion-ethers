use alloy::primitives::{Address, B256};
use alloy::providers::RootProvider;
use alloy::rpc::types::eth::{BlockNumberOrTag, Filter, FilterBlockOption};
use alloy::transports::BoxTransport;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::BinaryExpr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::*,
    scalar::ScalarValue,
};
use futures::{Stream, TryStreamExt};
use std::{any::Any, sync::Arc};

use crate::config::EthProviderConfig;
use crate::convert::Transcoder as _;
use crate::stream::StreamOptions;
use crate::utils::*;

///////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EthCatalog {
    rpc_client: RootProvider<BoxTransport>,
}

impl EthCatalog {
    pub fn new(rpc_client: RootProvider<BoxTransport>) -> Self {
        Self { rpc_client }
    }
}

impl CatalogProvider for EthCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["eth".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "eth" => Some(Arc::new(EthSchema::new(self.rpc_client.clone()))),
            _ => None,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Schema
///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EthSchema {
    rpc_client: RootProvider<BoxTransport>,
}

impl EthSchema {
    pub fn new(rpc_client: RootProvider<BoxTransport>) -> Self {
        Self { rpc_client }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for EthSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["logs".to_string()]
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "logs" => Ok(Some(Arc::new(EthLogsTable::new(self.rpc_client.clone())))),
            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "logs"
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Table: eth_logs
///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EthLogsTable {
    schema: SchemaRef,
    rpc_client: RootProvider<BoxTransport>,
}

impl EthLogsTable {
    pub fn new(rpc_client: RootProvider<BoxTransport>) -> Self {
        Self {
            schema: crate::convert::EthRawLogsToArrow::new().schema(),
            rpc_client,
        }
    }

    fn apply_expr(filter: Filter, expr: &Expr) -> (TableProviderFilterPushDown, Filter) {
        match expr {
            Expr::BinaryExpr(e) => match (&*e.left, e.op, &*e.right) {
                (Expr::Column(left), op, right) => match (left.name.as_str(), op, right) {
                    ("address", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.address(Address::from_slice(&v[..])),
                    ),
                    ("topic0", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.event_signature(B256::from_slice(&v[..])),
                    ),
                    ("topic1", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic1(B256::from_slice(&v[..])),
                    ),
                    ("topic2", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic2(B256::from_slice(&v[..])),
                    ),
                    ("topic3", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic3(B256::from_slice(&v[..])),
                    ),
                    ("block_number", Operator::Eq, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (
                            TableProviderFilterPushDown::Exact,
                            filter.union((*v).into(), (*v).into()),
                        )
                    }
                    ("block_number", Operator::Gt, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (
                            TableProviderFilterPushDown::Exact,
                            filter.union((*v + 1).into(), BlockNumberOrTag::Latest),
                        )
                    }
                    (
                        "block_number",
                        Operator::GtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (
                        TableProviderFilterPushDown::Exact,
                        filter.union((*v).into(), BlockNumberOrTag::Latest),
                    ),
                    ("block_number", Operator::Lt, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (
                            TableProviderFilterPushDown::Exact,
                            filter.union(BlockNumberOrTag::Earliest, (*v - 1).into()),
                        )
                    }
                    (
                        "block_number",
                        Operator::LtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (
                        TableProviderFilterPushDown::Exact,
                        filter.union(BlockNumberOrTag::Earliest, (*v).into()),
                    ),
                    _ => (TableProviderFilterPushDown::Unsupported, filter),
                },
                // expr OR expr OR ...
                (Expr::BinaryExpr(_), Operator::Or, Expr::BinaryExpr(_)) => {
                    match Self::collect_or_group(e) {
                        (TableProviderFilterPushDown::Exact, Some(col), values) => {
                            match col.name.as_str() {
                                "address" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.address(
                                        values
                                            .into_iter()
                                            .map(|v| Address::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic0" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.event_signature(
                                        values
                                            .into_iter()
                                            .map(|v| B256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic1" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic1(
                                        values
                                            .into_iter()
                                            .map(|v| B256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic2" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic2(
                                        values
                                            .into_iter()
                                            .map(|v| B256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic3" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic3(
                                        values
                                            .into_iter()
                                            .map(|v| B256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                _ => (TableProviderFilterPushDown::Unsupported, filter),
                            }
                        }
                        _ => (TableProviderFilterPushDown::Unsupported, filter),
                    }
                }
                _ => (TableProviderFilterPushDown::Unsupported, filter),
            },
            _ => (TableProviderFilterPushDown::Unsupported, filter),
        }
    }

    /// Converts expressions like:
    ///   x = v1 or (x = v2 or (x = v3))
    /// into:
    ///   [v1, v2, v3]
    fn collect_or_group(
        expr: &BinaryExpr,
    ) -> (TableProviderFilterPushDown, Option<&Column>, Vec<&Vec<u8>>) {
        match (&*expr.left, expr.op, &*expr.right) {
            (Expr::Column(col), Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(val)))) => {
                (TableProviderFilterPushDown::Exact, Some(col), vec![val])
            }
            (Expr::BinaryExpr(left), Operator::Or, Expr::BinaryExpr(right)) => {
                // Merge values from two branches, but only if they refer to the same column
                let mut left = Self::collect_or_group(left);
                let mut right = Self::collect_or_group(right);
                if left.0 == right.0
                    && right.0 == TableProviderFilterPushDown::Exact
                    && left.1 == right.1
                {
                    left.2.append(&mut right.2);
                    (TableProviderFilterPushDown::Exact, left.1, left.2)
                } else {
                    (TableProviderFilterPushDown::Unsupported, None, Vec::new())
                }
            }
            _ => (TableProviderFilterPushDown::Unsupported, None, Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for EthLogsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        tracing::debug!(?filters, "EthLogs: performing filter pushdown");

        let mut filter = EthProviderConfig::default().default_filter();
        let mut res = Vec::new();

        for expr in filters {
            let (support, new_filter) = Self::apply_expr(filter, expr);
            filter = new_filter;
            res.push(support);
        }

        tracing::debug!(?filter, "EthLogs: resulting filter");
        Ok(res)
    }

    async fn scan(
        &self,
        state: &SessionState,
        mut projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let config = state
            .config_options()
            .extensions
            .get::<EthProviderConfig>()
            .cloned()
            .unwrap_or_default();

        tracing::debug!(
            ?config,
            ?projection,
            ?filters,
            ?limit,
            "EthLogs: creating execution plan"
        );

        // TODO: Datafusion always passess a projection even when one is not required
        if let Some(proj) = projection {
            let is_redundant = proj.len() == self.schema.fields.len()
                && proj[0] == 0
                && proj
                    .iter()
                    .cloned()
                    .reduce(|a, b| if a + 1 == b { b } else { a })
                    == Some(self.schema.fields.len() - 1);
            if is_redundant {
                projection = None
            }
        }

        let schema = if let Some(proj) = projection {
            Arc::new(self.schema.project(proj)?)
        } else {
            self.schema.clone()
        };

        // Get filter with range restrictions from the session config level
        let mut filter = config.default_filter();

        // Push down filter expressions from the query
        for expr in filters {
            let (support, new_filter) = Self::apply_expr(filter, expr);
            assert_eq!(support, TableProviderFilterPushDown::Exact);
            filter = new_filter;
        }

        Ok(Arc::new(EthGetLogs::new(
            self.rpc_client.clone(),
            schema,
            projection.cloned(),
            filter,
            config.stream_options(),
            limit,
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthGetLogs {
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    rpc_client: RootProvider<BoxTransport>,
    filter: Filter,
    stream_options: StreamOptions,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl EthGetLogs {
    pub fn new(
        rpc_client: RootProvider<BoxTransport>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Filter,
        stream_options: StreamOptions,
        limit: Option<usize>,
    ) -> Self {
        Self {
            projected_schema: projected_schema.clone(),
            projection,
            rpc_client,
            filter,
            stream_options,
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
                // TODO: Change to Unbounded
                datafusion::physical_plan::ExecutionMode::Bounded,
            ),
        }
    }

    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = filter;
        self
    }

    fn execute_impl(
        rpc_client: RootProvider<BoxTransport>,
        filter: Filter,
        options: StreamOptions,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = DfResult<RecordBatch>> {
        async_stream::try_stream! {
            let limit = limit.unwrap_or(usize::MAX);
            let mut coder = crate::convert::EthRawLogsToArrow::new();
            let mut total = 0;

            // TODO: Streaming/unbounded API
            let mut log_stream = Box::pin(
                crate::stream::RawLogsStream::paginate(rpc_client, filter, options, None)
            );

            while let Some(batch) = log_stream.try_next().await.map_err(|e| DataFusionError::External(e.into()))? {
                if total >= limit {
                    break;
                }
                let to_append = usize::min(batch.logs.len(), limit - total);
                coder.append(&batch.logs[0..to_append]).map_err(|e| DataFusionError::External(e.into()))?;
                total += to_append;

                // Don't form batches that are too small
                if coder.len() > 1_000 {
                    let batch = if let Some(proj) = &projection {
                        coder.finish().project(proj)?
                    } else {
                        coder.finish()
                    };
                    yield batch;
                }
            }

            // Return at least one batch, even if empty
            if !coder.is_empty() || total == 0 {
                let batch = if let Some(proj) = &projection {
                    coder.finish().project(proj)?
                } else {
                    coder.finish()
                };
                yield batch;
            }
        }
    }
}

impl ExecutionPlan for EthGetLogs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<datafusion::execution::SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        let stream = Self::execute_impl(
            self.rpc_client.clone(),
            self.filter.clone(),
            self.stream_options.clone(),
            self.projection.clone(),
            self.limit,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for EthGetLogs {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EthGetLogs: ")?;
                if self.projection.is_none() {
                    write!(f, "projection=[*]")?;
                } else {
                    let projection_str = self
                        .projected_schema
                        .fields
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ");

                    write!(f, "projection=[{}]", projection_str)?;
                }

                let mut filters = Vec::new();

                match self.filter.block_option {
                    FilterBlockOption::Range {
                        from_block,
                        to_block,
                    } => filters.push(format!("block_number=[{:?}, {:?}]", from_block, to_block)),
                    FilterBlockOption::AtBlockHash(h) => filters.push(format!("block_hash={}", h)),
                }

                if !self.filter.address.is_empty() {
                    // Provide deterministic order
                    let mut addrs: Vec<_> =
                        self.filter.address.iter().map(|h| h.to_string()).collect();
                    addrs.sort();
                    filters.push(format!("address=[{}]", addrs.join(", ")));
                }

                for (i, t) in self
                    .filter
                    .topics
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| !t.is_empty())
                {
                    // Provide deterministic order
                    let mut topics: Vec<_> = t.iter().map(|h| h.to_string()).collect();
                    topics.sort();
                    filters.push(format!("topic{}=[{}]", i, topics.join(", ")));
                }

                write!(f, ", filter=[{}]", filters.join(", "))?;

                if let Some(limit) = self.limit {
                    write!(f, ", limit={}", limit)?;
                }

                Ok(())
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
