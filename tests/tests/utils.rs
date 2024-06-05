use std::io::Write;

use datafusion::common::DFSchema;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use pretty_assertions::assert_eq;

///////////////////////////////////////////////////////////////////////////////////////////////////

pub fn assert_schema_eq(schema: &DFSchema, expected: &str) {
    let parquet_schema = datafusion::parquet::arrow::arrow_to_parquet_schema(&schema.into())
        .unwrap()
        .root_schema_ptr();
    let actual = format_schema_parquet(&parquet_schema);
    assert_eq!(expected.trim(), actual.trim());
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn assert_data_eq(df: DataFrame, expected: &str) {
    use datafusion::arrow::util::pretty;

    let batches = df.collect().await.unwrap();
    let actual = pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert_eq!(expected.trim(), actual.trim());
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Prints schema in a style of `parquet-schema` output
pub fn write_schema_parquet(output: &mut dyn Write, schema: &Type) -> Result<(), std::io::Error> {
    datafusion::parquet::schema::printer::print_schema(output, schema);
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Same as [`write_schema_parquet`] but outputs into a String
pub fn format_schema_parquet(schema: &Type) -> String {
    let mut buf = Vec::new();
    write_schema_parquet(&mut buf, schema).unwrap();
    String::from_utf8(buf).unwrap()
}
