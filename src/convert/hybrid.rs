use alloy_core::json_abi::Event;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use ethers::prelude::*;
use std::sync::Arc;

use super::{AppendError, Transcoder};

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Transcodes raw Ethereum log events into Arrow record batches while also creating a nested
/// struct with fields decoded according to the provided log signature
pub struct EthRawAndDecodedLogsToArrow {
    schema: SchemaRef,
    raw: super::EthRawLogsToArrow,
    decoded: super::EthDecodedLogsToArrow,
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
            raw,
            decoded,
        }
    }
}

impl Transcoder for EthRawAndDecodedLogsToArrow {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[allow(clippy::get_first)]
    fn append(&mut self, logs: &[Log]) -> Result<(), AppendError> {
        self.raw.append(logs)?;
        self.decoded.append(logs)?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.raw.len()
    }

    fn finish(&mut self) -> RecordBatch {
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
