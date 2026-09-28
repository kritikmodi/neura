//! Bounded, uncompressed IPC framing. Never infer success from HTTP 200 or bare EOF.
use anyhow::{Result, bail, ensure};
use datafusion::arrow::{
    buffer::Buffer,
    ipc::{MessageHeader, reader::StreamDecoder, root_as_message},
    record_batch::RecordBatch,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct BoundedIpc<R> {
    input: R,
    decoder: StreamDecoder,
    limit: usize,
    done: bool,
}
impl<R: AsyncRead + Unpin> BoundedIpc<R> {
    pub fn new(input: R, limit: usize) -> Self {
        Self {
            input,
            decoder: StreamDecoder::new(),
            limit,
            done: false,
        }
    }
    pub async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        loop {
            let mut header = [0; 4];
            self.input
                .read_exact(&mut header)
                .await
                .map_err(|_| anyhow::anyhow!("ClickHouse Arrow stream ended without EOS"))?;
            ensure!(
                header == [255; 4],
                "invalid ClickHouse IPC continuation (possibly a remote query error)"
            );
            self.input.read_exact(&mut header).await?;
            let len = u32::from_le_bytes(header) as usize;
            if len == 0 {
                let mut eos = Buffer::from(vec![255_u8, 255, 255, 255, 0, 0, 0, 0]);
                let final_batch = self.decoder.decode(&mut eos)?;
                if !eos.is_empty() {
                    ensure!(
                        self.decoder.decode(&mut eos)?.is_none(),
                        "unexpected batch after EOS"
                    );
                }
                ensure!(
                    self.decoder.schema().is_some(),
                    "ClickHouse Arrow schema missing"
                );
                self.decoder.finish()?;
                let mut trailing = [0; 1];
                ensure!(
                    self.input.read(&mut trailing).await? == 0,
                    "data or remote error after ClickHouse Arrow EOS"
                );
                self.done = true;
                return Ok(final_batch);
            }
            // Check lengths before allocating. Do not accept compressed bodies or accumulating dictionaries.
            ensure!(
                len <= self.limit.min(1024 * 1024),
                "ClickHouse IPC metadata exceeds buffer limit"
            );
            let mut frame = Vec::with_capacity(len + 8);
            frame.extend_from_slice(&[255; 4]);
            frame.extend_from_slice(&header);
            frame.resize(len + 8, 0);
            self.input.read_exact(&mut frame[8..]).await?;
            let message = root_as_message(&frame[8..])
                .map_err(|_| anyhow::anyhow!("invalid ClickHouse IPC metadata"))?;
            let body_len = usize::try_from(message.bodyLength())?;
            ensure!(
                body_len <= self.limit && len + 8 <= self.limit - body_len,
                "ClickHouse IPC frame exceeds buffer limit"
            );
            match message.header_type() {
                MessageHeader::Schema => ensure!(body_len == 0, "unexpected schema body"),
                MessageHeader::RecordBatch => {
                    let batch = message
                        .header_as_record_batch()
                        .ok_or_else(|| anyhow::anyhow!("invalid IPC batch"))?;
                    ensure!(
                        batch.compression().is_none(),
                        "compressed IPC is disabled to bound decoded memory"
                    );
                    ensure!(
                        batch.length() >= 0 && batch.length() as usize <= self.limit,
                        "IPC row count exceeds buffer limit"
                    );
                }
                _ => bail!("ClickHouse IPC dictionaries and extension messages are unsupported"),
            }
            frame.resize(len + 8 + body_len, 0);
            self.input.read_exact(&mut frame[len + 8..]).await?;
            let batch = self.decoder.decode(&mut Buffer::from(frame))?;
            if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
    };
    use std::sync::Arc;
    fn encoded() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut bytes = vec![];
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        bytes
    }
    #[tokio::test]
    async fn fragmented_stream_requires_eos_and_clean_eof() {
        use tokio::io::AsyncWriteExt;
        let bytes = encoded();
        let (mut writer, reader) = tokio::io::duplex(7);
        tokio::spawn(async move {
            for chunk in bytes.chunks(3) {
                writer.write_all(chunk).await.unwrap();
            }
        });
        let mut ipc = BoundedIpc::new(reader, 4096);
        assert_eq!(ipc.next().await.unwrap().unwrap().num_rows(), 3);
        assert!(ipc.next().await.unwrap().is_none());
        let mut bytes = encoded();
        bytes.truncate(bytes.len() - 8);
        let mut ipc = BoundedIpc::new(bytes.as_slice(), 4096);
        ipc.next().await.unwrap();
        assert!(ipc.next().await.is_err());
        let mut bytes = encoded();
        bytes.extend_from_slice(b"Code: late query error");
        let mut ipc = BoundedIpc::new(bytes.as_slice(), 4096);
        ipc.next().await.unwrap();
        assert!(ipc.next().await.is_err());
    }
    #[tokio::test]
    async fn oversized_frame_is_rejected_before_body_allocation() {
        let bytes = encoded();
        assert!(BoundedIpc::new(bytes.as_slice(), 32).next().await.is_err());
        let malicious = [255, 255, 255, 255, 255, 255, 255, 127];
        assert!(
            BoundedIpc::new(malicious.as_slice(), 4096)
                .next()
                .await
                .is_err()
        );
    }
}
