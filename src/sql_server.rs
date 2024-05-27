use datafusion::arrow::array;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{LogicalPlan, Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::*,
    scalar::ScalarValue,
};
use ethers::prelude::*;
use std::{any::Any, sync::Arc};

////////////////////////////////////////////////////////////////////////////////////////

pub struct EthSqlServer<P> {
    df_ctx: SessionContext,
    rpc_client: Arc<Provider<P>>,
}

impl<P: JsonRpcClient> EthSqlServer<P> {
    pub fn new(rpc_client: Arc<Provider<P>>) -> Self {
        let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");
        let mut df_ctx = SessionContext::new_with_config(config);
        super::udf::register_all(&mut df_ctx).unwrap();
        df_ctx
            .register_table("eth_logs", Arc::new(EthLogsTableProvider))
            .unwrap();
        Self { df_ctx, rpc_client }
    }

    pub async fn query_logs(&self, sql: &str) -> RecordBatch {
        let plan = self.df_ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = self.df_ctx.state().optimize(&plan).unwrap();
        let filter = self.plan_to_filter(plan);

        let chain_id = self.rpc_client.get_chainid().await.unwrap();
        let last_block = self.rpc_client.get_block_number().await.unwrap();
        tracing::info!(chain_id = %chain_id, last_block = %last_block, "Chain info");

        let filter = filter.from_block(0).to_block(last_block);
        tracing::info!(filter = ?filter, "Final filter");

        let mut log_stream = self.rpc_client.get_logs_paginated(&filter, 100_000);

        let mut logs = Vec::new();
        while let Some(log) = log_stream.next().await.transpose().unwrap() {
            assert!(!log.removed.unwrap_or_default());
            logs.push(log);
        }

        logs_to_record_batch(logs)
    }

    fn plan_to_filter(&self, plan: LogicalPlan) -> Filter {
        tracing::info!(plan = %plan.display_indent(), "Raw plan");

        let LogicalPlan::TableScan(table_scan) = plan else {
            panic!("Plan is too complex");
        };

        let mut filter = Filter::new();

        for expr in table_scan.filters {
            let Expr::BinaryExpr(expr) = expr else {
                panic!("Filter must be a binary expression: {}", expr);
            };
            let Expr::Column(col) = expr.left.as_ref() else {
                panic!(
                    "Expected filter in the form of binary expression 'column = value' but got: {}",
                    expr
                );
            };
            match (col.name.as_str(), expr.op, expr.right.as_ref()) {
                ("address", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => {
                    filter = filter.address(Address::from_slice(&v[..]))
                }
                ("topic0", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => {
                    filter = filter.topic0(H256::from_slice(&v[..]))
                }
                ("topic1", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => {
                    filter = filter.topic1(H256::from_slice(&v[..]))
                }
                ("topic2", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => {
                    filter = filter.topic2(H256::from_slice(&v[..]))
                }
                ("topic3", Operator::Eq, Expr::Literal(ScalarValue::Binary(Some(v)))) => {
                    filter = filter.topic3(H256::from_slice(&v[..]))
                }
                _ => panic!("Filter is not supported: {}", expr),
            }
        }

        tracing::info!(filter = ?filter, "Plan-based filter");
        filter
    }
}

////////////////////////////////////////////////////////////////////////////////////////

struct EthLogsTableProvider;

#[async_trait::async_trait]
impl TableProvider for EthLogsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
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
        ]))
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

////////////////////////////////////////////////////////////////////////////////////////

pub fn logs_to_record_batch(logs: Vec<Log>) -> array::RecordBatch {
    let mut block_number = array::UInt64Builder::new();
    let mut block_hash = array::BinaryBuilder::new();
    let mut transaction_index = array::UInt64Builder::new();
    let mut transaction_hash = array::BinaryBuilder::new();
    let mut log_index = array::UInt64Builder::new();
    let mut address = array::BinaryBuilder::new();
    let mut topic0 = array::BinaryBuilder::new();
    let mut topic1 = array::BinaryBuilder::new();
    let mut topic2 = array::BinaryBuilder::new();
    let mut topic3 = array::BinaryBuilder::new();
    let mut data = array::BinaryBuilder::new();

    for log in logs {
        block_number.append_value(log.block_number.unwrap().as_u64());
        block_hash.append_value(log.block_hash.unwrap().as_bytes());
        transaction_index.append_value(log.transaction_index.unwrap().as_u64());
        transaction_hash.append_value(log.transaction_hash.unwrap().as_bytes());
        log_index.append_value(log.log_index.unwrap().as_u64());
        address.append_value(log.address.as_bytes());

        assert!(log.topics.len() <= 4);
        topic0.append_option(log.topics.get(0).map(|v| v.as_bytes()));
        topic1.append_option(log.topics.get(1).map(|v| v.as_bytes()));
        topic2.append_option(log.topics.get(2).map(|v| v.as_bytes()));
        topic3.append_option(log.topics.get(3).map(|v| v.as_bytes()));

        data.append_value(&log.data);
    }

    array::RecordBatch::try_new(
        Arc::new(Schema::new(vec![
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
        ])),
        vec![
            Arc::new(block_number.finish()),
            Arc::new(block_hash.finish()),
            Arc::new(transaction_index.finish()),
            Arc::new(transaction_hash.finish()),
            Arc::new(log_index.finish()),
            Arc::new(address.finish()),
            Arc::new(topic0.finish()),
            Arc::new(topic1.finish()),
            Arc::new(topic2.finish()),
            Arc::new(topic3.finish()),
            Arc::new(data.finish()),
        ],
    )
    .unwrap()
}
