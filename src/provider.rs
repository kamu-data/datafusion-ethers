use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::BinaryExpr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::*,
    scalar::ScalarValue,
};
use ethers::prelude::*;
use futures::Stream;
use std::{any::Any, sync::Arc};

use crate::config::EthProviderConfig;
use crate::utils::*;

///////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EthCatalog<P> {
    rpc_client: Arc<Provider<P>>,
}

impl<P: JsonRpcClient + 'static> EthCatalog<P> {
    pub fn new(rpc_client: Arc<Provider<P>>) -> Self {
        Self { rpc_client }
    }
}

impl<P: JsonRpcClient + 'static> CatalogProvider for EthCatalog<P> {
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

pub struct EthSchema<P> {
    rpc_client: Arc<Provider<P>>,
}

impl<P: JsonRpcClient + 'static> EthSchema<P> {
    pub fn new(rpc_client: Arc<Provider<P>>) -> Self {
        Self { rpc_client }
    }
}

#[async_trait::async_trait]
impl<P: JsonRpcClient + 'static> SchemaProvider for EthSchema<P> {
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

pub struct EthLogsTable<P> {
    schema: SchemaRef,
    rpc_client: Arc<Provider<P>>,
}

impl<P: JsonRpcClient + 'static> EthLogsTable<P> {
    pub fn new(rpc_client: Arc<Provider<P>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::Binary, false),
            Field::new("transaction_index", DataType::UInt64, false),
            Field::new("transaction_hash", DataType::Binary, false),
            Field::new("log_index", DataType::UInt64, false),
            Field::new("address", DataType::Binary, false),
            Field::new("topic0", DataType::Binary, true),
            Field::new("topic1", DataType::Binary, true),
            Field::new("topic2", DataType::Binary, true),
            Field::new("topic3", DataType::Binary, true),
            Field::new("data", DataType::Binary, false),
        ]));

        Self { schema, rpc_client }
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
                        filter.topic0(H256::from_slice(&v[..])),
                    ),
                    ("topic1", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic1(H256::from_slice(&v[..])),
                    ),
                    ("topic2", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic2(H256::from_slice(&v[..])),
                    ),
                    ("topic3", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => (
                        TableProviderFilterPushDown::Exact,
                        filter.topic3(H256::from_slice(&v[..])),
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
                            filter.union((*v + 1).into(), BlockNumber::Latest),
                        )
                    }
                    (
                        "block_number",
                        Operator::GtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (
                        TableProviderFilterPushDown::Exact,
                        filter.union((*v).into(), BlockNumber::Latest),
                    ),
                    ("block_number", Operator::Lt, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (
                            TableProviderFilterPushDown::Exact,
                            filter.union(BlockNumber::Earliest, (*v - 1).into()),
                        )
                    }
                    (
                        "block_number",
                        Operator::LtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (
                        TableProviderFilterPushDown::Exact,
                        filter.union(BlockNumber::Earliest, (*v).into()),
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
                                    filter.topic0(
                                        values
                                            .into_iter()
                                            .map(|v| H256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic1" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic1(
                                        values
                                            .into_iter()
                                            .map(|v| H256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic2" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic2(
                                        values
                                            .into_iter()
                                            .map(|v| H256::from_slice(&v[..]))
                                            .collect::<Vec<_>>(),
                                    ),
                                ),
                                "topic3" => (
                                    TableProviderFilterPushDown::Exact,
                                    filter.topic3(
                                        values
                                            .into_iter()
                                            .map(|v| H256::from_slice(&v[..]))
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
impl<P: JsonRpcClient + 'static> TableProvider for EthLogsTable<P> {
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        tracing::debug!(
            ?projection,
            ?filters,
            ?limit,
            "EthLogs: creating execution plan"
        );

        let schema = if let Some(projection) = projection {
            Arc::new(self.schema.project(projection)?)
        } else {
            self.schema.clone()
        };

        let mut filter =
            if let Some(cfg) = state.config_options().extensions.get::<EthProviderConfig>() {
                cfg.default_filter()
            } else {
                EthProviderConfig::default().default_filter()
            };

        // Push down filter expressions from the query
        for expr in filters {
            let (support, new_filter) = Self::apply_expr(filter, expr);
            assert_eq!(support, TableProviderFilterPushDown::Exact);
            filter = new_filter;
        }

        // Apply range restrictions from the session config level
        if let Some(cfg) = state.config_options().extensions.get::<EthProviderConfig>() {
            filter.block_option = filter.block_option.union(FilterBlockOption::Range {
                from_block: Some(cfg.block_range_from),
                to_block: Some(cfg.block_range_to),
            });
        }

        // TODO verify filter does not cause a full-scan

        Ok(Arc::new(EthGetLogs::new(
            self.rpc_client.clone(),
            schema,
            projection.cloned(),
            filter,
            limit,
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthGetLogs<P> {
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    rpc_client: Arc<Provider<P>>,
    filter: Filter,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl<P: JsonRpcClient + 'static> EthGetLogs<P> {
    pub fn new(
        rpc_client: Arc<Provider<P>>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Filter,
        limit: Option<usize>,
    ) -> Self {
        Self {
            projected_schema: projected_schema.clone(),
            projection,
            rpc_client,
            filter,
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
        rpc_client: Arc<Provider<P>>,
        filter: Filter,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = DfResult<RecordBatch>> {
        async_stream::try_stream! {
            let limit = limit.unwrap_or(usize::MAX);
            let mut coder = crate::convert::EthRawLogsToArrow::new();
            let mut returned = 0;

            // TODO: Streaming API
            let mut log_stream = rpc_client.get_logs_paginated(&filter, 100_000);

            let mut logs = Vec::new();
            while let Some(log) = log_stream.next().await.transpose().unwrap() {
                if returned >= limit {
                    break;
                }
                assert!(!log.removed.unwrap_or_default());
                logs.push(log);
                returned += 1;
            }

            coder.append(&logs);
            let batch = coder.finish();

            let batch = if let Some(projection) = projection {
                batch.project(&projection)?
            } else {
                batch
            };
            yield batch;
        }
    }
}

impl<P: JsonRpcClient + 'static> ExecutionPlan for EthGetLogs<P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
            self.projection.clone(),
            self.limit,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

impl<P: JsonRpcClient + 'static> DisplayAs for EthGetLogs<P> {
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
                match &self.filter.address {
                    Some(ValueOrArray::Value(addr)) => {
                        filters.push(format!("address={}", addr));
                    }
                    Some(ValueOrArray::Array(v)) => {
                        let vals: Vec<_> = v.iter().map(|h| h.to_string()).collect();
                        filters.push(format!("address={}", vals.join(" or ")));
                    }
                    _ => {}
                }
                match self.filter.block_option {
                    FilterBlockOption::Range {
                        from_block,
                        to_block,
                    } => filters.push(format!("block_number=[{:?}, {:?}]", from_block, to_block)),
                    FilterBlockOption::AtBlockHash(h) => filters.push(format!("block_hash={}", h)),
                }
                if self.filter.topics.iter().any(|t| t.is_some()) {
                    for (i, t) in self.filter.topics.iter().enumerate() {
                        match t {
                            Some(ValueOrArray::Value(Some(h))) => {
                                filters.push(format!("topic{}={}", i, h));
                            }
                            Some(ValueOrArray::Array(v)) => {
                                let vals: Vec<_> =
                                    v.iter().map(|h| h.as_ref().unwrap().to_string()).collect();
                                filters.push(format!("topic{}={}", i, vals.join(" or ")));
                            }
                            _ => {}
                        }
                    }
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
