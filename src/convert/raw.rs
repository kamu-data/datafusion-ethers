use datafusion::arrow::array::{self, ArrayBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use ethers::prelude::*;
use std::sync::Arc;

use super::{AppendError, Transcoder};

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Transcodes decoded Ethereum log events into Arrow record batches
pub struct EthRawLogsToArrow {
    schema: SchemaRef,
    block_number: array::UInt64Builder,
    block_hash: array::BinaryBuilder,
    transaction_index: array::UInt64Builder,
    transaction_hash: array::BinaryBuilder,
    log_index: array::UInt64Builder,
    address: array::BinaryBuilder,
    topic0: array::BinaryBuilder,
    topic1: array::BinaryBuilder,
    topic2: array::BinaryBuilder,
    topic3: array::BinaryBuilder,
    data: array::BinaryBuilder,
}

impl EthRawLogsToArrow {
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![
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
            block_number: array::UInt64Builder::new(),
            block_hash: array::BinaryBuilder::new(),
            transaction_index: array::UInt64Builder::new(),
            transaction_hash: array::BinaryBuilder::new(),
            log_index: array::UInt64Builder::new(),
            address: array::BinaryBuilder::new(),
            topic0: array::BinaryBuilder::new(),
            topic1: array::BinaryBuilder::new(),
            topic2: array::BinaryBuilder::new(),
            topic3: array::BinaryBuilder::new(),
            data: array::BinaryBuilder::new(),
        }
    }
}

impl Transcoder for EthRawLogsToArrow {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[allow(clippy::get_first)]
    fn append(&mut self, logs: &[Log]) -> Result<(), AppendError> {
        for log in logs {
            self.block_number
                .append_value(log.block_number.unwrap().as_u64());
            self.block_hash
                .append_value(log.block_hash.unwrap().as_bytes());
            self.transaction_index
                .append_value(log.transaction_index.unwrap().as_u64());
            self.transaction_hash
                .append_value(log.transaction_hash.unwrap().as_bytes());
            self.log_index.append_value(log.log_index.unwrap().as_u64());
            self.address.append_value(log.address.as_bytes());

            assert!(log.topics.len() <= 4);
            self.topic0
                .append_option(log.topics.get(0).map(|v| v.as_bytes()));
            self.topic1
                .append_option(log.topics.get(1).map(|v| v.as_bytes()));
            self.topic2
                .append_option(log.topics.get(2).map(|v| v.as_bytes()));
            self.topic3
                .append_option(log.topics.get(3).map(|v| v.as_bytes()));

            self.data.append_value(&log.data);
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.block_number.len()
    }

    fn finish(&mut self) -> array::RecordBatch {
        array::RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.block_number.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.transaction_hash.finish()),
                Arc::new(self.log_index.finish()),
                Arc::new(self.address.finish()),
                Arc::new(self.topic0.finish()),
                Arc::new(self.topic1.finish()),
                Arc::new(self.topic2.finish()),
                Arc::new(self.topic3.finish()),
                Arc::new(self.data.finish()),
            ],
        )
        .unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
