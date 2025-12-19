use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
pub use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    Resp2,
    Resp3,
}

#[derive(Debug, Clone)]
pub enum Frame {
    Resp2(Resp2Frame),
    Resp3(Resp3Frame),
}

#[derive(Debug)]
pub struct RespStream {
    stream: TcpStream,
    buf: BytesMut,
    version: RespVersion,
}

impl RespStream {
    pub fn new(stream: TcpStream, version: RespVersion) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            version,
        }
    }

    pub fn set_version(&mut self, v: RespVersion) {
        self.version = v;
    }

    pub fn version(&self) -> RespVersion {
        self.version
    }

    /// Read exactly one RESP frame from the stream.
    ///
    /// Returns `Ok(None)` on clean EOF.
    pub async fn read_frame(&mut self) -> Result<Option<(Frame, Bytes)>> {
        loop {
            let decoded = match self.version {
                RespVersion::Resp2 => {
                    match redis_protocol::resp2::decode::decode_bytes_mut(&mut self.buf) {
                        Ok(Some((frame, _amt, out))) => Some((Frame::Resp2(frame), out)),
                        Ok(None) => None,
                        Err(e) => return Err(anyhow!("RESP2 decode error: {e}")),
                    }
                }
                RespVersion::Resp3 => {
                    match redis_protocol::resp3::decode::complete::decode_bytes_mut(&mut self.buf) {
                        Ok(Some((frame, _amt, out))) => Some((Frame::Resp3(frame), out)),
                        Ok(None) => None,
                        Err(e) => return Err(anyhow!("RESP3 decode error: {e}")),
                    }
                }
            };

            if let Some((frame, raw)) = decoded {
                return Ok(Some((frame, raw)));
            }

            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Ok(None);
            }
        }
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

/// Encode a Redis command as a RESP Array of Bulk/Blob Strings.
///
/// Redis expects requests in this form for both RESP2 and RESP3.
pub fn encode_command(parts: &[bytes::Bytes]) -> BytesMut {
    // For an internal proxy command we can accept UTF-8-ish parts.
    // We still treat them as raw bytes (Bytes).
    let mut out = BytesMut::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Convenience helper for encoding ASCII command parts.
pub fn encode_command_str(parts: &[&str]) -> BytesMut {
    let b: Vec<bytes::Bytes> = parts
        .iter()
        .map(|s| bytes::Bytes::copy_from_slice(s.as_bytes()))
        .collect();
    encode_command(&b)
}
