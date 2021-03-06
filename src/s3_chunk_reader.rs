use core::panic;
use core::time::Duration;
use std::io::{self, Read};
use std::sync::Arc;

use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use chunked_bytes::ChunkedBytes;
use futures_retry::{FutureRetry, RetryPolicy};
use lazy_static::lazy_static;
use parquet::data_type::AsBytes;
use parquet::errors::Result;
use parquet::file::reader::{ChunkReader, Length};
use regex::Regex;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectError, GetObjectOutput, GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;

enum Range {
    FromPositionTo(u64, u64),
    FromEnd(u64),
}

struct ContentRange {
    // start_pos: u64,
    // end_pos: u64,
    total_length: u64,
}

fn get_content_range(response: &GetObjectOutput) -> ContentRange {
    lazy_static! {
        static ref BYTES_REGEX: Regex = Regex::new(r"bytes (\d+)-(\d+)/([0-9*]+)").unwrap();
    };

    let content_range_captures = BYTES_REGEX
        .captures(response.content_range.as_ref().unwrap().as_str())
        .unwrap();

    ContentRange {
        // start_pos: content_range_captures.get(1).unwrap().as_str().parse::<u64>().unwrap(),
        // end_pos: content_range_captures.get(2).unwrap().as_str().parse::<u64>().unwrap(),
        total_length: content_range_captures
            .get(3)
            .unwrap()
            .as_str()
            .parse::<u64>()
            .unwrap(),
    }
}

async fn fetch_range(client: &S3Client, url: (String, String), range: Range) -> GetObjectOutput {
    let bucket = &url.0;
    let key = &url.1;
    let range_str = &match range {
        Range::FromPositionTo(start_pos, length) => format!("bytes={}-{}", start_pos, length),
        Range::FromEnd(length) => format!("bytes=-{}", length),
    };

    let delay_ms = 200;
    let mut remaining_retries: usize = 3;
    let handle_get_object_error =
        |e: RusotoError<GetObjectError>| -> RetryPolicy<RusotoError<GetObjectError>> {
            if remaining_retries == 0 {
                return RetryPolicy::ForwardError(e);
            }
            remaining_retries -= 1;
            match e {
                RusotoError::HttpDispatch(_) | RusotoError::ParseError(_) => {
                    RetryPolicy::WaitRetry(Duration::from_millis(delay_ms))
                }
                RusotoError::Unknown(r) => {
                    if r.status.is_server_error() {
                        RetryPolicy::WaitRetry(Duration::from_millis(delay_ms))
                    } else {
                        RetryPolicy::ForwardError(RusotoError::Unknown(r))
                    }
                }
                _ => RetryPolicy::ForwardError(e),
            }
        };

    let (output, _) = FutureRetry::new(
        move || {
            let get_obj_req = GetObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                range: Some(range_str.clone()),
                ..Default::default()
            };
            client.get_object(get_obj_req)
        },
        handle_get_object_error,
    )
    .await
    .unwrap();
    output
}

struct DownloadPart {
    start_pos: u64,
    length: u64,
    reader_channel: Sender<Bytes>,
}

pub struct S3ChunkReader {
    url: (String, String),
    length: u64,
    read_size: u64,
    total_size: u64,
    coordinator: Option<Sender<Option<DownloadPart>>>,
    reader_channel: Option<Receiver<Bytes>>,
    buf: Reader<ChunkedBytes>,
}

impl S3ChunkReader {
    pub fn new(url: (String, String), total_size: u64) -> S3ChunkReader {
        S3ChunkReader {
            url,
            length: total_size,
            read_size: 0,
            total_size,
            coordinator: None,
            reader_channel: None,
            buf: ChunkedBytes::new().reader(),
        }
    }

    pub async fn new_unknown_size(url: (String, String), client: S3Client) -> S3ChunkReader {
        let response = fetch_range(&client, url.clone(), Range::FromEnd(4)).await;
        let content_range = get_content_range(&response);
        let mut magic_number: Vec<u8> = vec![];
        response
            .body
            .unwrap()
            .into_async_read()
            .read_to_end(&mut magic_number)
            .await
            .unwrap();
        if magic_number.as_bytes() != "PAR1".as_bytes() {
            panic!("Not a parquet file");
        }
        Self::new(url, content_range.total_length)
    }

    pub async fn start(&mut self, base_client: S3Client, timeout: Duration) {
        let (s, mut r) = channel(1);
        let url = self.url.clone();
        self.coordinator = Some(s);
        let semaphore = Arc::new(Semaphore::new(32));
        tokio::spawn(async move {
            while let Some(download_part) = r.recv().await.unwrap_or(None) {
                let client = base_client.clone();
                let url = url.clone();
                let semaphore_clone = Arc::clone(&semaphore);
                tokio::spawn(async move {
                    let permit = semaphore_clone.acquire().await.unwrap();
                    let response = fetch_range(
                        &client,
                        url,
                        Range::FromPositionTo(
                            download_part.start_pos,
                            download_part.start_pos + download_part.length - 1,
                        ),
                    )
                    .await;

                    let body = response.body.unwrap().timeout(timeout);
                    tokio::pin!(body);

                    drop(permit);
                    while let Ok(Some(data)) = body.try_next().await {
                        let reader_channel = download_part.reader_channel.clone();
                        reader_channel.send(data.unwrap()).await.unwrap_or(());
                    }
                });
            }
        });
    }
}

impl Length for S3ChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl Read for S3ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining_size = (self.length - self.read_size) as usize;
        let self_buf = self.buf.get_mut();
        if remaining_size > 0 && buf.len() > self_buf.remaining() {
            match self.reader_channel.take() {
                Some(mut reader_channel) => {
                    let data = reader_channel.blocking_recv().unwrap();
                    let added_size = data.len();
                    self.read_size += added_size as u64;
                    if self_buf.is_empty() && buf.len() >= added_size {
                        buf[0..added_size].copy_from_slice(&data);
                        self.reader_channel = Some(reader_channel);
                        return Ok(added_size);
                    }
                    self_buf.put_bytes(data);
                    self.reader_channel = Some(reader_channel);
                }
                None => unimplemented!(),
            };
        }
        self.buf.read(buf)
    }
}

impl ChunkReader for S3ChunkReader {
    type T = S3ChunkReader;

    fn get_read(&self, start_pos: u64, length: usize) -> Result<Self::T> {
        let (s, r) = channel(16);

        self.coordinator
            .clone()
            .unwrap()
            .blocking_send(Some(DownloadPart {
                start_pos,
                length: length as u64,
                reader_channel: s,
            }))
            .unwrap_or_else(|err| {
                eprintln!("Error {}", err);
                unimplemented!()
            });

        Ok(S3ChunkReader {
            url: self.url.clone(),
            length: length as u64,
            read_size: 0,
            total_size: self.total_size,
            coordinator: self.coordinator.clone(),
            reader_channel: Some(r),
            buf: ChunkedBytes::new().reader(),
        })
    }
}
