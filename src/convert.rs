use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use ethers::prelude::*;
use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::get_first)]
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
