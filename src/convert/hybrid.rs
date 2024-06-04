use alloy_core::dyn_abi::{DecodedEvent, DynSolEvent, Specifier};
use alloy_core::json_abi::Event;
use alloy_core::primitives::B256;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use ethers::prelude::*;
use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Transcodes raw Ethereum log events into Arrow record batches while also creating a nested
/// struct with fields decoded according to the provided log signature
pub struct EthRawAndDecodedLogsToArrow {
    schema: SchemaRef,
    event_decoder: DynSolEvent,
    raw: super::EthRawLogsToArrow,
    decoded: super::EthDecodedLogsToArrow,
    decoding_buf: Vec<DecodedEvent>,
}

impl EthRawAndDecodedLogsToArrow {
    pub fn new(event_type: &Event) -> Self {
        let raw = super::EthRawLogsToArrow::new();
        let decoded = super::EthDecodedLogsToArrow::new(event_type);

        let mut builder = SchemaBuilder::from(&raw.schema().fields);
        builder.push(Field::new(
            "event",
            DataType::Struct(decoded.schema().fields.clone()),
            false,
        ));
        Self {
            schema: Arc::new(builder.finish()),
            event_decoder: event_type.resolve().unwrap(),
            raw,
            decoded,
            decoding_buf: Vec::new(),
        }
    }

    #[allow(clippy::get_first)]
    pub fn append(&mut self, logs: &[Log]) -> Result<(), alloy_core::dyn_abi::Error> {
        for log in logs {
            let decoded = self.event_decoder.decode_log_parts(
                log.topics.iter().map(|t| B256::new(t.0)),
                &log.data,
                true,
            )?;

            self.decoding_buf.push(decoded);
        }

        self.raw.append(logs);
        self.decoded.append(&self.decoding_buf);
        self.decoding_buf.clear();

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn finish(&mut self) -> RecordBatch {
        let raw = self.raw.finish();
        let decoded = self.decoded.finish();

        let event_col = datafusion::arrow::array::StructArray::new(
            decoded.schema().fields().clone(),
            decoded.columns().to_vec(),
            None,
        );

        let mut columns = raw.columns().to_vec();
        columns.push(Arc::new(event_col));

        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
