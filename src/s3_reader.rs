use core::panic;
use std::io::{self, Read};

use aws_sdk_s3::output::GetObjectOutput;
use lazy_static::lazy_static;
use parquet::errors::Result;
use parquet::file::reader::{ChunkReader, Length};
use regex::Regex;

use aws_sdk_s3::{Client, Config, Region};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
    println!("Content Range {}", response.content_length);
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

async fn fetch_range(client: Client, url: (String, String), range: Range) -> GetObjectOutput {
    let range_str = match range {
        Range::FromPositionTo(start_pos, length) => format!("bytes={}-{}", start_pos, length),
        Range::FromEnd(length) => format!("bytes=-{}", length),
    };

    println!("Fetching {}", range_str);

    let result = client
        .get_object()
        .bucket(url.0)
        .key(url.1)
        .range(range_str)
        .send()
        .await
        .unwrap();

    result
}

struct DownloadPart {
    start_pos: u64,
    length: u64,
    reader_channel: Sender<Vec<u8>>,
}

pub struct S3ChunkReader {
    url: (String, String),
    length: u64,
    read_size: u64,
    total_size: u64,
    coordinator: Option<Sender<Option<DownloadPart>>>,
    reader_channel: Option<Receiver<Vec<u8>>>,
    buf: Vec<u8>,
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
            buf: Vec::new(),
        }
    }

    pub async fn new_unknown_size(url: (String, String)) -> S3ChunkReader {
        let conf = Config::builder().region(Region::new("us-east-1")).build();
        let client = Client::from_conf(conf);

        let response = fetch_range(client, url.clone(), Range::FromEnd(4)).await;
        let content_range = get_content_range(&response);
        let magic_number = response.body.collect().await.unwrap().into_bytes().to_vec();
        if magic_number != "PAR1".as_bytes() {
            panic!("Not a parquet file");
        }
        Self::new(url, content_range.total_length)
    }

    pub async fn start(&mut self) {
        let (s, mut r) = channel(1);
        let url = self.url.clone();
        self.coordinator = Some(s);
        tokio::spawn(async move {
            while let Some(download_part) = r.recv().await.unwrap_or(None) {
                let url = url.clone();
                tokio::spawn(async move {
                    let conf = Config::builder().region(Region::new("us-east-1")).build();
                    let client = Client::from_conf(conf);

                    let mut response = fetch_range(
                        client,
                        url,
                        Range::FromPositionTo(
                            download_part.start_pos,
                            download_part.start_pos + download_part.length - 1,
                        ),
                    )
                    .await;

                    while let Some(bytes) = response.body.try_next().await.unwrap_or(None) {
                        let mut data = bytes.to_vec();
                        data.truncate(data.len());
                        let reader_channel = download_part.reader_channel.clone();
                        reader_channel.send(data.to_vec()).await.unwrap_or(());
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
        match self.reader_channel.take() {
            Some(mut reader_channel) => {
                let remaining_size = (self.length - self.read_size) as usize;
                if remaining_size > 0 {
                    let mut data = reader_channel.blocking_recv().unwrap();
                    let added_size = data.len();
                    self.read_size += added_size as u64;
                    if self.buf.is_empty() && buf.len() >= added_size {
                        buf[0..added_size].copy_from_slice(&data[0..added_size]);
                        self.reader_channel = Some(reader_channel);
                        return Ok(added_size);
                    }
                    if self.buf.is_empty() {
                        self.buf = data;
                    } else {
                        self.buf.append(&mut data);
                    }
                }
                let readable_size = std::cmp::min(buf.len(), self.buf.len());
                let drain = self.buf.drain(0..readable_size);
                let data = drain.as_slice();
                buf[0..readable_size as usize].copy_from_slice(&data[0..readable_size]);
                self.reader_channel = Some(reader_channel);
                Ok(readable_size)
            }
            None => unimplemented!(),
        }
    }
}

impl ChunkReader for S3ChunkReader {
    type T = S3ChunkReader;

    fn get_read(&self, start_pos: u64, length: usize) -> Result<Self::T> {
        let (s, r) = channel(1);

        self.coordinator
            .clone()
            .unwrap()
            .blocking_send(Some(DownloadPart {
                start_pos,
                length: length as u64,
                reader_channel: s,
            }))
            .unwrap_or_else(|err| {
                println!("Error {}", err);
                unimplemented!()
            });

        Ok(S3ChunkReader {
            url: self.url.clone(),
            length: length as u64,
            read_size: 0,
            total_size: self.total_size,
            coordinator: self.coordinator.clone(),
            reader_channel: Some(r),
            buf: Vec::new(),
        })
    }
}
