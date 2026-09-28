//! Governed HTTP SQL with bounded Arrow IPC streaming; no access to backing storage.
use super::{Remote, arrow_stream::BoundedIpc, df_error};
use crate::config::{Connection, secret};
use anyhow::{Result, ensure};
use datafusion::{
    arrow::{
        array::ArrayRef,
        compute::{CastOptions, cast_with_options},
        datatypes::{DataType, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use std::{collections::HashSet, sync::atomic::Ordering, time::Duration};
use tokio_util::io::StreamReader;

// The same identity may cancel only its own UUID. Never retry a SELECT under a new snapshot.
struct QueryGuard {
    request: Option<reqwest::RequestBuilder>,
}
impl Drop for QueryGuard {
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            tokio::spawn(async move {
                let _ = request.send().await;
            });
        }
    }
}

fn buffer_footprint(batch: &RecordBatch) -> usize {
    fn count(data: &datafusion::arrow::array::ArrayData, seen: &mut HashSet<usize>) -> usize {
        let own = data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
            .filter(|b| seen.insert(b.data_ptr().as_ptr() as usize))
            .map(|b| b.capacity())
            .sum::<usize>();
        own + data
            .child_data()
            .iter()
            .map(|child| count(child, seen))
            .sum::<usize>()
    }
    // IPC columns are slices of one allocation. Arrow's per-array estimates count
    // that allocation repeatedly; use allocation identity for the connector cap.
    let mut seen = HashSet::new();
    batch
        .columns()
        .iter()
        .map(|a| count(&a.to_data(), &mut seen))
        .sum()
}

fn conform(batch: RecordBatch, schema: SchemaRef, limit: usize) -> Result<RecordBatch> {
    ensure!(
        batch.num_columns() == schema.fields().len(),
        "ClickHouse column count mismatch"
    );
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| -> Result<ArrayRef> {
            let options = CastOptions {
                safe: false,
                ..Default::default()
            };
            // ClickHouse's Arrow format represents Date/DateTime as epoch integers.
            // Normalize their documented units before applying the configured Arrow type.
            let normalized = match (array.data_type(), field.data_type()) {
                (DataType::UInt16, DataType::Date32) => {
                    let days = cast_with_options(array, &DataType::Int32, &options)?;
                    cast_with_options(&days, &DataType::Date32, &options)?
                }
                (DataType::UInt32, DataType::Timestamp(_, zone)) => {
                    let seconds = cast_with_options(array, &DataType::Int64, &options)?;
                    cast_with_options(
                        &seconds,
                        &DataType::Timestamp(TimeUnit::Second, zone.clone()),
                        &options,
                    )?
                }
                _ => array.clone(),
            };
            let array = &normalized;
            let converted = if array.data_type() == field.data_type() {
                array.clone()
            } else {
                let options = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                let cast = cast_with_options(array, field.data_type(), &options)?;
                // Fail on overflow, decimal/timestamp truncation, or other lossy coercion.
                let back = cast_with_options(&cast, array.data_type(), &options)?;
                ensure!(
                    array.to_data() == back.to_data(),
                    "ClickHouse conversion to {} would lose precision; use an explicit source view",
                    field.data_type()
                );
                cast
            };
            ensure!(
                field.is_nullable() || converted.null_count() == 0,
                "ClickHouse NULL in required column"
            );
            Ok(converted)
        })
        .collect::<Result<Vec<_>>>()?;
    let result = RecordBatch::try_new(schema, arrays)?;
    ensure!(
        buffer_footprint(&result) <= limit,
        "ClickHouse decoded batch exceeds buffer limit"
    );
    Ok(result)
}

pub fn execute(
    remote: Remote,
    query: String,
    schema: SchemaRef,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let output_schema = schema.clone();
    let stream = async_stream::try_stream! {
        let Connection::Clickhouse { endpoint, user, password_env, allow_http, .. } = &remote.source.connection else { unreachable!() };
        if remote.clickhouse_started.swap(true, Ordering::AcqRel) {
            Err(df_error("ClickHouse plan requires multiple remote statements; a shared snapshot cannot be guaranteed. Consolidate into one source view/subplan."))?;
        }
        let password = secret(password_env).map_err(df_error)?;
        let client = reqwest::Client::builder()
            .https_only(!allow_http).redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(remote.limits.query_timeout_secs))
            .no_gzip().build().map_err(df_error)?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut guard = QueryGuard { request: Some(client.post(endpoint).basic_auth(user, Some(&password))
            .timeout(Duration::from_secs(5)).body(format!("KILL QUERY WHERE query_id = '{id}' ASYNC"))) };
        let limit = remote.limits.max_remote_page_mb * 1024 * 1024;
        let _timer = remote.metrics.timer(&remote.source.name);
        let response = client.post(endpoint).basic_auth(user, Some(&password))
            .query(&[
                ("query_id", id), ("readonly", "1".into()),
                ("max_block_size", remote.limits.batch_rows.to_string()),
                ("preferred_block_size_bytes", limit.min(1024*1024).to_string()),
                ("max_execution_time", remote.limits.query_timeout_secs.to_string()),
                ("max_memory_usage", (remote.limits.memory_mb*1024*1024).to_string()),
                ("max_threads", remote.limits.partitions.to_string()),
                ("join_use_nulls", "1".into()), ("session_timezone", "UTC".into()),
                ("cancel_http_readonly_queries_on_client_close", "1".into()),
                ("output_format_arrow_low_cardinality_as_dictionary", "0".into()),
                ("output_format_arrow_string_as_string", "1".into()),
                ("output_format_arrow_compression_method", "none".into()),
                ("wait_end_of_query", "0".into()), ("buffer_size", "65536".into()),
                ("enable_http_compression", "0".into()),
            ]).body(format!("{query} FORMAT ArrowStream")).send().await
            .map_err(|_| df_error("ClickHouse request failed"))?;
        if !response.status().is_success() {
            Err(df_error(format!("ClickHouse HTTP {}; consult source query history", response.status().as_u16())))?;
        }
        let metrics = remote.metrics.clone(); let name = remote.source.name.clone();
        let input = response.bytes_stream().map(move |chunk| {
            let chunk = chunk.map_err(|_| std::io::Error::other("ClickHouse response interrupted"))?;
            if chunk.len() > limit { return Err(std::io::Error::other("ClickHouse HTTP chunk exceeds buffer limit")); }
            metrics.update(&name, |m| m.response_bytes = Some(m.response_bytes.unwrap_or(0) + chunk.len() as u64));
            Ok(chunk)
        });
        let mut ipc = BoundedIpc::new(StreamReader::new(input), limit);
        loop {
            let batch = tokio::select! {
                _ = remote.cancel.cancelled() => Err(df_error("query cancelled")),
                batch = ipc.next() => batch.map_err(df_error),
            }?;
            let Some(batch) = batch else { break; };
            let batch = conform(batch, schema.clone(), limit).map_err(df_error)?;
            remote.metrics.update(&remote.source.name, |m| { m.rows += batch.num_rows() as u64; m.arrow_bytes += batch.get_array_memory_size() as u64; });
            for start in (0..batch.num_rows()).step_by(remote.limits.batch_rows) {
                yield batch.slice(start, (batch.num_rows()-start).min(remote.limits.batch_rows));
            }
        }
        guard.request = None;
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::{Decimal128Array, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;
    #[test]
    fn casts_cannot_silently_overflow_or_round() {
        let from = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
        let to = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let b =
            RecordBatch::try_new(from, vec![Arc::new(UInt64Array::from(vec![u64::MAX]))]).unwrap();
        assert!(conform(b, to, 4096).is_err());
        let a = Decimal128Array::from(vec![123])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let b = RecordBatch::try_from_iter(vec![("n", Arc::new(a) as ArrayRef)]).unwrap();
        let to = Arc::new(Schema::new(vec![Field::new(
            "n",
            DataType::Decimal128(10, 1),
            false,
        )]));
        assert!(conform(b, to, 4096).is_err());
    }
}
