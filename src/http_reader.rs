use core::panic;
use core::time::Duration;
use std::io::{self, Read};
use std::result;
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};
use lazy_static::lazy_static;
use parquet::errors::Result;
use parquet::file::reader::{ChunkReader, Length};
use regex::Regex;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue};

enum Range {
    FromPositionTo(u64, u64),
    FromEnd(u64),
}

fn construct_headers(range: Range) -> HeaderMap {
    let mut headers = HeaderMap::new();

    let range_str = match range {
        Range::FromPositionTo(start_pos, length) => format!("bytes={}-{}", start_pos, length),
        Range::FromEnd(length) => format!("bytes=-{}", length),
    };

    headers.insert("Range", HeaderValue::from_str(range_str.as_str()).unwrap());
    headers
}

struct ContentRange {
    // start_pos: u64,
    // end_pos: u64,
    total_length: u64,
}

static CONTENT_RANGE_HEADER_KEY: &str = "Content-Range";

fn get_content_range(response: &Response) -> ContentRange {
    if !response.headers().contains_key(CONTENT_RANGE_HEADER_KEY) {
        panic!("Range header not supported");
    }
    let content_range_value = response
        .headers()
        .get(CONTENT_RANGE_HEADER_KEY)
        .unwrap()
        .to_str()
        .unwrap();
    lazy_static! {
        static ref BYTES_REGEX: Regex = Regex::new(r"bytes (\d+)-(\d+)/([0-9*]+)").unwrap();
    };
    let content_range_captures = BYTES_REGEX.captures(content_range_value).unwrap();

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

fn fetch_range(client: Client, url: String, range: Range) -> Response {
    client
        .get(url)
        .headers(construct_headers(range))
        .send()
        .unwrap()
}

struct DownloadPart {
    start_pos: u64,
    length: u64,
    reader_channel: Sender<Vec<u8>>,
}

pub struct HttpChunkReader {
    url: String,
    length: u64,
    read_size: u64,
    total_size: u64,
    coordinator: Option<Sender<Option<DownloadPart>>>,
    reader_channel: Option<Receiver<Vec<u8>>>,
    buf: Vec<u8>,
}

impl HttpChunkReader {
    pub fn new(url: String, total_size: u64) -> HttpChunkReader {
        HttpChunkReader {
            url,
            length: total_size,
            read_size: 0,
            total_size,
            coordinator: None,
            reader_channel: None,
            buf: Vec::new(),
        }
    }

    pub async fn new_unknown_size(url: String) -> HttpChunkReader {
        tokio::task::spawn_blocking(move || {
            let response = fetch_range(Client::new(), url.clone(), Range::FromEnd(4));
            let content_range = get_content_range(&response);
            let magic_number = response.text().unwrap();
            if magic_number != "PAR1" {
                panic!("Not a parquet file");
            }
            Self::new(url, content_range.total_length)
        })
        .await
        .unwrap()
    }

    pub fn start(&mut self, timeout: Duration) {
        let (s, r) = bounded(0);
        let url = self.url.clone();
        self.coordinator = Some(s);
        thread::spawn(move || {
            while let result::Result::Ok(download_part_option) = r.recv() {
                let download_part = download_part_option.unwrap();
                let url = url.clone();
                thread::spawn(move || {
                    let client = Client::builder().timeout(timeout).build().unwrap();
                    let mut response = fetch_range(
                        client,
                        url,
                        Range::FromPositionTo(
                            download_part.start_pos,
                            download_part.start_pos + download_part.length - 1,
                        ),
                    );
                    loop {
                        let mut data: Vec<u8> = vec![0; 1024 * 64];

                        match response.read(&mut data) {
                            io::Result::Ok(len) => {
                                let reader_channel = download_part.reader_channel.clone();
                                if len == 0 {
                                    return;
                                }
                                data.truncate(len);
                                reader_channel.send(data).unwrap();
                            }
                            io::Result::Err(_) => unimplemented!(),
                        };
                    }
                });
            }
        });
    }
}

impl Length for HttpChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl Read for HttpChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.reader_channel.take() {
            Some(reader_channel) => {
                let remaining_size = (self.length - self.read_size) as usize;
                if remaining_size > 0 {
                    let mut data = reader_channel.recv().unwrap();
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

impl ChunkReader for HttpChunkReader {
    type T = HttpChunkReader;

    fn get_read(&self, start_pos: u64, length: usize) -> Result<Self::T> {
        let (s, r) = bounded(0);

        self.coordinator
            .clone()
            .unwrap()
            .send(Some(DownloadPart {
                start_pos,
                length: length as u64,
                reader_channel: s,
            }))
            .unwrap();

        Ok(HttpChunkReader {
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
