use std::any::Any;
use std::sync::Arc;

use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt as _};
use alloy::hex::ToHexExt;
use alloy::json_abi::Event;
use alloy::primitives::B256;
use datafusion::arrow::array::{self, Array as _};
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::{
    arrow::datatypes::DataType,
    common::plan_err,
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};

///////////////////////////////////////////////////////////////////////////////////////////////////

pub fn event_to_json(schema: &Event, event: &DecodedEvent) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("name".to_string(), schema.name.clone().into());

    let mut i_indexed = 0;
    let mut i_body = 0;

    for input in &schema.inputs {
        let value = if input.indexed {
            let v = &event.indexed[i_indexed];
            i_indexed += 1;
            v
        } else {
            let v = &event.body[i_body];
            i_body += 1;
            v
        };

        obj.insert(input.name.clone(), solidity_value_to_json(value));
    }

    obj.into()
}

pub fn solidity_value_to_json(value: &DynSolValue) -> serde_json::Value {
    use serde_json::Value;
    match value {
        DynSolValue::Bool(v) => Value::Bool(*v),
        DynSolValue::Int(v, bits) if *bits <= 64 => Value::Number(v.as_i64().into()),
        DynSolValue::Int(v, _) => Value::String(v.to_string()),
        DynSolValue::Uint(v, bits) if *bits <= 64 => Value::Number(v.to::<u64>().into()),
        DynSolValue::Uint(v, _) => Value::String(v.to_string()),
        DynSolValue::FixedBytes(v, _) => Value::String(v.encode_hex()),
        DynSolValue::Address(v) => Value::String(v.encode_hex()),
        DynSolValue::Function(_) => todo!(),
        DynSolValue::Bytes(v) => Value::String(v.encode_hex()),
        DynSolValue::String(v) => Value::String(v.clone()),
        DynSolValue::Array(_) => todo!(),
        DynSolValue::FixedArray(_) => todo!(),
        DynSolValue::Tuple(_) => todo!(),
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
struct UdfEthDecodeEvent {
    signature: Signature,
}

impl UdfEthDecodeEvent {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UdfEthDecodeEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "eth_decode_event"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    #[allow(clippy::get_first)]
    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> {
        if args.len() != 6 {
            return plan_err!(
                "eth_decode_event accepts 6 arguments: event_signature, topic0, topic1, topic2, topic3, data"
            );
        }
        if !matches!(args.get(0), Some(&DataType::Utf8)) {
            return plan_err!("event_signature must be a Utf8 scalar");
        }
        if !matches!(args.get(1), Some(&DataType::Binary)) {
            return plan_err!("topic0 must be a Binary");
        }
        if !matches!(args.get(2), Some(&DataType::Binary)) {
            return plan_err!("topic1 must be a Binary");
        }
        if !matches!(args.get(3), Some(&DataType::Binary)) {
            return plan_err!("topic2 must be a Binary");
        }
        if !matches!(args.get(4), Some(&DataType::Binary)) {
            return plan_err!("topic3 must be a Binary");
        }
        if !matches!(args.get(5), Some(&DataType::Binary)) {
            return plan_err!("data must be a Binary");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let signature = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => v,
            _ => return plan_err!("Event signature must be a Utf8 scalar"),
        };

        let event = Event::parse(signature)
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let args = ColumnarValue::values_to_arrays(&args.args[1..])?;
        let c_topic0 = datafusion::common::cast::as_binary_array(&args[0])?;
        let c_topic1 = datafusion::common::cast::as_binary_array(&args[1])?;
        let c_topic2 = datafusion::common::cast::as_binary_array(&args[2])?;
        let c_topic3 = datafusion::common::cast::as_binary_array(&args[3])?;
        let c_data = datafusion::common::cast::as_binary_array(&args[4])?;

        let mut builder = array::StringBuilder::new();

        for i in 0..c_topic0.len() {
            let mut topics = Vec::with_capacity(4);

            for c_topic in [c_topic0, c_topic1, c_topic2, c_topic3] {
                if c_topic.is_null(i) {
                    break;
                }
                topics.push(B256::try_from(c_topic.value(i)).expect("??"));
            }
            let data = c_data.value(i);

            let decoded = event
                .decode_log_parts(topics, data)
                .map_err(|e| DataFusionError::External(e.into()))?;

            builder.append_value(event_to_json(&event, &decoded).to_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
struct UdfEthTryDecodeEvent {
    signature: Signature,
}

impl UdfEthTryDecodeEvent {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UdfEthTryDecodeEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "eth_try_decode_event"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    #[allow(clippy::get_first)]
    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> {
        if args.len() != 6 {
            return plan_err!(
                "eth_decode_event accepts 6 arguments: event_signature, topic0, topic1, topic2, topic3, data"
            );
        }
        if !matches!(args.get(0), Some(&DataType::Utf8)) {
            return plan_err!("event_signature must be a Utf8 scalar");
        }
        if !matches!(args.get(1), Some(&DataType::Binary)) {
            return plan_err!("topic0 must be a Binary");
        }
        if !matches!(args.get(2), Some(&DataType::Binary)) {
            return plan_err!("topic1 must be a Binary");
        }
        if !matches!(args.get(3), Some(&DataType::Binary)) {
            return plan_err!("topic2 must be a Binary");
        }
        if !matches!(args.get(4), Some(&DataType::Binary)) {
            return plan_err!("topic3 must be a Binary");
        }
        if !matches!(args.get(5), Some(&DataType::Binary)) {
            return plan_err!("data must be a Binary");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let signature = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => v,
            _ => return plan_err!("Event signature must be a Utf8 scalar"),
        };

        let event = Event::parse(signature)
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let args = ColumnarValue::values_to_arrays(&args.args[1..])?;
        let c_topic0 = datafusion::common::cast::as_binary_array(&args[0])?;
        let c_topic1 = datafusion::common::cast::as_binary_array(&args[1])?;
        let c_topic2 = datafusion::common::cast::as_binary_array(&args[2])?;
        let c_topic3 = datafusion::common::cast::as_binary_array(&args[3])?;
        let c_data = datafusion::common::cast::as_binary_array(&args[4])?;

        let mut builder = array::StringBuilder::new();

        for i in 0..c_topic0.len() {
            let mut topics = Vec::with_capacity(4);

            for c_topic in [c_topic0, c_topic1, c_topic2, c_topic3] {
                if c_topic.is_null(i) {
                    break;
                }
                topics.push(B256::try_from(c_topic.value(i)).expect("??"));
            }
            let data = c_data.value(i);

            match event.decode_log_parts(topics, data) {
                Ok(decoded) => builder.append_value(event_to_json(&event, &decoded).to_string()),
                Err(_) => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
struct UdfEthEventSelector {
    signature: Signature,
}

impl UdfEthEventSelector {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UdfEthEventSelector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "eth_event_selector"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let signature = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => v,
            _ => return plan_err!("Event signature must be a Utf8 scalar"),
        };

        let event = Event::parse(signature)
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let mut builder = array::BinaryBuilder::new();
        builder.append_value(event.selector().as_slice());
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_all(registry: &mut dyn FunctionRegistry) -> datafusion::error::Result<()> {
    registry.register_udf(Arc::new(ScalarUDF::from(UdfEthDecodeEvent::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(UdfEthTryDecodeEvent::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(UdfEthEventSelector::new())))?;
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////////////////////////
