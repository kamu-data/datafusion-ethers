use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
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

////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////
// Schema
////////////////////////////////////////////////////////////////////////////////////////

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
        match name {
            "logs" => true,
            _ => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Table: eth_logs
////////////////////////////////////////////////////////////////////////////////////////

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

    fn default_filter() -> Filter {
        Filter::new()
            .from_block(BlockNumber::Earliest)
            .to_block(BlockNumber::Latest)
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
                            filter.from_block(*v).to_block(*v),
                        )
                    }
                    ("block_number", Operator::Gt, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (
                            TableProviderFilterPushDown::Exact,
                            filter.from_block(*v + 1),
                        )
                    }
                    (
                        "block_number",
                        Operator::GtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (TableProviderFilterPushDown::Exact, filter.from_block(*v)),
                    ("block_number", Operator::Lt, Expr::Literal(ScalarValue::UInt64(Some(v)))) => {
                        (TableProviderFilterPushDown::Exact, filter.to_block(*v - 1))
                    }
                    (
                        "block_number",
                        Operator::LtEq,
                        Expr::Literal(ScalarValue::UInt64(Some(v))),
                    ) => (TableProviderFilterPushDown::Exact, filter.to_block(*v)),
                    _ => (TableProviderFilterPushDown::Unsupported, filter),
                },
                // expr OR expr OR ...
                (Expr::BinaryExpr(_), Operator::Or, Expr::BinaryExpr(_)) => {
                    todo!()
                }
                _ => (TableProviderFilterPushDown::Unsupported, filter),
            },
            _ => (TableProviderFilterPushDown::Unsupported, filter),
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

        let mut filter = Self::default_filter();
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
        _state: &SessionState,
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
            Arc::new(self.schema.project(&projection)?)
        } else {
            self.schema.clone()
        };

        let mut filter = Self::default_filter();

        for expr in filters {
            let (support, new_filter) = Self::apply_expr(filter, expr);
            assert_eq!(support, TableProviderFilterPushDown::Exact);
            filter = new_filter;
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

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct EthGetLogs<P> {
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

    fn execute_impl(
        rpc_client: Arc<Provider<P>>,
        filter: Filter,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = DfResult<RecordBatch>> {
        async_stream::try_stream! {
            let mut returned = 0;
            let limit = limit.unwrap_or(usize::MAX);
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

            if !logs.is_empty() {
                let batch = crate::sql_server::logs_to_record_batch(logs);

                let batch = if let Some(projection) = projection {
                    batch.project(&projection)?
                } else {
                    batch
                };
                yield batch;
            }
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
                if let Some(ValueOrArray::Value(addr)) = &self.filter.address {
                    filters.push(format!("address={}", addr));
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
                        if let Some(ValueOrArray::Value(Some(h))) = t {
                            filters.push(format!("topic{}={}", i, h));
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

////////////////////////////////////////////////////////////////////////////////////////
