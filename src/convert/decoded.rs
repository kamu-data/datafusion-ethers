use alloy_core::dyn_abi::{DecodedEvent, DynSolType, DynSolValue, Specifier};
use alloy_core::json_abi::{Event, EventParam};
use alloy_core::primitives::Address;
use datafusion::arrow::array::{self, Array, ArrayBuilder, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Transcodes decoded Ethereum log events into Arrow record batches
pub struct EthDecodedLogsToArrow {
    schema: SchemaRef,
    /// The layout is indexed fields first, then data - to correspond to how [DecodedEvent] stores fields
    field_builders: Vec<Box<dyn SolidityArrayBuilder>>,
}

impl EthDecodedLogsToArrow {
    pub fn new(event_type: &Event) -> Self {
        let resolved_type = event_type.resolve().unwrap();

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();

        for (typ, param) in resolved_type
            .indexed()
            .iter()
            .chain(resolved_type.body().iter())
            .zip(
                event_type
                    .inputs
                    .iter()
                    .filter(|f| f.indexed)
                    .chain(event_type.inputs.iter().filter(|f| !f.indexed)),
            )
        {
            let (field, builder) = Self::event_param_to_field(param, typ);
            fields.push(field);
            field_builders.push(builder);
        }

        Self {
            schema: Arc::new(Schema::new(fields)),
            field_builders,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn append(&mut self, logs: &[DecodedEvent]) {
        for log in logs {
            for (val, builder) in log
                .indexed
                .iter()
                .chain(log.body.iter())
                .zip(self.field_builders.iter_mut())
            {
                builder.append_value(val);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.field_builders[0].len()
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns = self.field_builders.iter_mut().map(|b| b.finish()).collect();
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    fn event_param_to_field(
        param: &EventParam,
        typ: &DynSolType,
    ) -> (Field, Box<dyn SolidityArrayBuilder>) {
        match typ {
            DynSolType::Bool => (
                Field::new(&param.name, DataType::Boolean, false),
                Box::new(SolidityArrayBuilderBool::default()),
            ),
            DynSolType::Uint(64) => (
                Field::new(&param.name, DataType::UInt64, false),
                Box::new(SolidityArrayBuilderUInt64::default()),
            ),
            DynSolType::Address => (
                Field::new(
                    &param.name,
                    DataType::FixedSizeBinary(Address::len_bytes() as i32),
                    false,
                ),
                Box::new(SolidityArrayBuilderAddress::default()),
            ),
            DynSolType::Bytes => (
                Field::new(&param.name, DataType::Binary, false),
                Box::new(SolidityArrayBuilderBytes::default()),
            ),
            _ => unimplemented!(
                "Support for transcoding {typ} solidity type to arrow is not yet implemented",
            ),
        }
    }
}

trait SolidityArrayBuilder {
    fn append_value(&mut self, value: &DynSolValue);
    fn len(&self) -> usize;
    fn finish(&mut self) -> Arc<dyn Array>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct SolidityArrayBuilderBool {
    builder: array::BooleanBuilder,
}

impl SolidityArrayBuilder for SolidityArrayBuilderBool {
    fn append_value(&mut self, value: &DynSolValue) {
        match value {
            DynSolValue::Bool(v) => self.builder.append_value(*v),
            _ => panic!("Unexpected value {value:?}"),
        }
    }
    fn len(&self) -> usize {
        self.builder.len()
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct SolidityArrayBuilderUInt64 {
    builder: array::UInt64Builder,
}

impl SolidityArrayBuilder for SolidityArrayBuilderUInt64 {
    fn append_value(&mut self, value: &DynSolValue) {
        match value {
            DynSolValue::Uint(v, 64) => self.builder.append_value(v.as_limbs()[0]),
            _ => panic!("Unexpected value {value:?}"),
        }
    }
    fn len(&self) -> usize {
        self.builder.len()
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

struct SolidityArrayBuilderAddress {
    builder: array::FixedSizeBinaryBuilder,
}

impl Default for SolidityArrayBuilderAddress {
    fn default() -> Self {
        Self {
            builder: array::FixedSizeBinaryBuilder::new(Address::len_bytes() as i32),
        }
    }
}

impl SolidityArrayBuilder for SolidityArrayBuilderAddress {
    fn append_value(&mut self, value: &DynSolValue) {
        match value {
            DynSolValue::Address(v) => self.builder.append_value(v.as_slice()).unwrap(),
            _ => panic!("Unexpected value {value:?}"),
        }
    }
    fn len(&self) -> usize {
        self.builder.len()
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct SolidityArrayBuilderBytes {
    builder: array::BinaryBuilder,
}

impl SolidityArrayBuilder for SolidityArrayBuilderBytes {
    fn append_value(&mut self, value: &DynSolValue) {
        match value {
            DynSolValue::Bytes(v) => self.builder.append_value(&v),
            _ => panic!("Unexpected value {value:?}"),
        }
    }
    fn len(&self) -> usize {
        self.builder.len()
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}
