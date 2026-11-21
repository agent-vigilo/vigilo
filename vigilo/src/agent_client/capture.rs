//! Bounded capture of HTTP content before text decoding or interpretation.
//!
//! Transfer framing is handled by the HTTP client. This module explicitly decodes
//! gzip and zlib-wrapped deflate, preserves the declared Content-Type, and publishes
//! immutable bytes only after complete, validated collection. Encoded input and
//! decoded output are independently bounded; failures never return partial bodies.

use std::io;

use async_compression::tokio::bufread::{
    GzipDecoder,
    ZlibDecoder,
};
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
    },
    time::Instant,
};
use tokio_util::io::StreamReader;
use vigilo::response::CapturedBody;

use super::{
    failure::{
        Failure,
        Kind,
    },
    limits::Body,
};

const ENCODING_OVERHEAD: u64 = 64 * 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 1024;

/// Capture under the original invocation deadline, including decompression work.
/// Unsupported or stacked content encodings are rejected rather than guessed.
pub(crate) async fn collect(
    response: reqwest::Response,
    limit: u64,
    deadline: Instant,
) -> Result<CapturedBody, Failure> {
    let status = response.status().as_u16();
    if limit == 0 {
        return Err(Failure::terminal(
            Kind::Configuration,
            "response capture limit must be positive",
        )
        .with_status(status));
    }
    if Instant::now() >= deadline {
        let mut failure = Failure::terminal(Kind::Timeout, "agent HTTP capture deadline exceeded")
            .with_status(status);
        failure.retryable = true;
        return Err(failure);
    }
    if response
        .headers()
        .get_all(reqwest::header::CONTENT_TYPE)
        .iter()
        .count()
        > 1
    {
        return Err(Failure::terminal(
            Kind::InvalidResponse,
            "multiple response Content-Type values are unsupported",
        )
        .with_status(status));
    }

    let invalid =
        |message: &str| Failure::terminal(Kind::InvalidResponse, message).with_status(status);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|value| {
            let value = value
                .to_str()
                .map_err(|_| invalid("invalid response Content-Type"))?;
            if value.len() > MAX_CONTENT_TYPE_BYTES {
                return Err(invalid("response Content-Type exceeds metadata limit"));
            }
            Ok(value.to_owned())
        })
        .transpose()?;
    let encodings = response
        .headers()
        .get_all(reqwest::header::CONTENT_ENCODING);
    if encodings.iter().count() > 1 {
        return Err(invalid(
            "multiple response content encodings are unsupported",
        ));
    }
    let encoding = encodings
        .iter()
        .next()
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| invalid("invalid response Content-Encoding"))?
        .unwrap_or("identity")
        .trim()
        .to_ascii_lowercase();
    if !matches!(encoding.as_str(), "identity" | "gzip" | "deflate") {
        return Err(invalid("unsupported response Content-Encoding"));
    }
    // Allow bounded encoding headers/trailers even for tiny decoded-body limits.
    let encoded_limit = limit.saturating_add(ENCODING_OVERHEAD);
    let stream = futures_util::stream::try_unfold(
        (response, 0_u64),
        move |(mut response, consumed)| async move {
            match response.chunk().await.map_err(|err| {
                io::Error::other(Failure::transport(
                    err,
                    "failed to read agent HTTP response body",
                    Some(status),
                ))
            })? {
                None => Ok(None),
                Some(chunk) => {
                    if chunk.len() as u64 > encoded_limit.saturating_sub(consumed) {
                        return Err(io::Error::other(
                            Failure::terminal(
                                Kind::BodyLimit,
                                "encoded response body exceeds byte limit",
                            )
                            .with_status(status),
                        ));
                    }
                    let consumed = consumed + chunk.len() as u64;
                    Ok(Some((chunk, (response, consumed))))
                }
            }
        },
    );
    let reader = StreamReader::new(Box::pin(stream));
    let operation = async {
        let bytes = match encoding.as_str() {
            "gzip" => {
                let mut decoder = GzipDecoder::new(reader);
                decoder.multiple_members(true);
                read_body(&mut decoder, limit, status).await?
            }
            "deflate" => {
                let mut decoder = ZlibDecoder::new(reader);
                let bytes = read_body(&mut decoder, limit, status).await?;
                let mut source = decoder.into_inner();
                let mut trailing = [0; 1];
                if source
                    .read(&mut trailing)
                    .await
                    .map_err(|err| read_failure(err, status))?
                    != 0
                {
                    return Err(invalid("trailing bytes after deflate response"));
                }
                bytes
            }
            _ => {
                let mut reader = reader;
                read_body(&mut reader, limit, status).await?
            }
        };
        Ok(CapturedBody::new(bytes, content_type))
    };
    tokio::time::timeout_at(deadline, operation)
        .await
        .map_err(|_| {
            // Keep the same retry class as an HTTP read timeout without storing data.
            let mut failure =
                Failure::terminal(Kind::Timeout, "agent HTTP capture deadline exceeded")
                    .with_status(status);
            failure.retryable = true;
            failure
        })?
}

async fn read_body(
    reader: &mut (impl AsyncRead + Unpin),
    limit: u64,
    status: u16,
) -> Result<Vec<u8>, Failure> {
    let mut body = Body::new(limit);
    let mut buffer = [0; 8192];
    loop {
        let length = reader
            .read(&mut buffer)
            .await
            .map_err(|err| read_failure(err, status))?;
        if length == 0 {
            return Ok(body.finish());
        }
        body.append(&buffer[..length]).map_err(|_| {
            Failure::terminal(Kind::BodyLimit, "agent HTTP response limit exceeded")
                .with_status(status)
        })?;
        // Ready compressed input must not monopolize the executor or starve timeout polling.
        tokio::task::yield_now().await;
    }
}

fn read_failure(error: io::Error, status: u16) -> Failure {
    if let Some(failure) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Failure>())
    {
        return failure.clone();
    }
    Failure::terminal(
        Kind::InvalidResponse,
        "invalid or incomplete compressed response",
    )
    .with_status(status)
}
