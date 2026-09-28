use crate::config::Column;
use anyhow::{Result, bail, ensure};
use datafusion::arrow::{
    array::{ArrayRef, StringArray},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

pub fn data_type(s: &str) -> Result<DataType> {
    Ok(match s {
        "bool" => DataType::Boolean,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ if s.starts_with("decimal(") && s.ends_with(')') => {
            let parts: Vec<_> = s[8..s.len() - 1].split(',').collect();
            ensure!(parts.len() == 2, "decimal requires precision,scale");
            let p: u8 = parts[0].parse()?;
            let scale: i8 = parts[1].parse()?;
            ensure!(
                p > 0 && p <= 38 && scale >= 0 && scale <= p as i8,
                "invalid decimal"
            );
            DataType::Decimal128(p, scale)
        }
        _ => bail!(
            "unsupported type {s}; nested/binary types must be explicitly transformed in a source view"
        ),
    })
}
pub fn schema(cols: &[Column]) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(
        cols.iter()
            .map(|c| Ok(Field::new(&c.name, data_type(&c.data_type)?, c.nullable)))
            .collect::<Result<Vec<_>>>()?,
    )))
}
/// Strict conversion: parsing failures are errors, never substituted with NULL.
pub fn batch(schema: SchemaRef, rows: &[Vec<Option<String>>]) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for row in rows {
        ensure!(
            row.len() == schema.fields().len(),
            "remote column count mismatch"
        );
    }
    for (i, f) in schema.fields().iter().enumerate() {
        if let DataType::Decimal128(_, scale) = f.data_type() {
            for value in rows.iter().filter_map(|r| r[i].as_deref()) {
                ensure!(
                    !value.contains(['e', 'E']),
                    "exponential decimal encoding requires an explicit source cast"
                );
                if let Some((_, fraction)) = value.split_once('.') {
                    let scale = (*scale).max(0) as usize;
                    ensure!(
                        fraction.len() <= scale || fraction[scale..].bytes().all(|b| b == b'0'),
                        "decimal would lose fractional precision"
                    );
                }
            }
        }

        let a = StringArray::from(rows.iter().map(|r| r[i].as_deref()).collect::<Vec<_>>());
        let a = cast_with_options(
            &a,
            f.data_type(),
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        ensure!(
            f.is_nullable() || a.null_count() == 0,
            "NULL in required column {}",
            f.name()
        );
        arrays.push(a);
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_is_exact() {
        let s = Arc::new(Schema::new(vec![Field::new(
            "n",
            data_type("decimal(38,2)").unwrap(),
            true,
        )]));
        let b = batch(s, &[vec![Some("123456789012345678901234567890.12".into())]]).unwrap();
        assert_eq!(
            datafusion::arrow::util::display::array_value_to_string(b.column(0), 0).unwrap(),
            "123456789012345678901234567890.12"
        );
    }
    #[test]
    fn invalid_value_fails() {
        let s = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        assert!(batch(s, &[vec![Some("oops".into())]]).is_err());
    }
}
