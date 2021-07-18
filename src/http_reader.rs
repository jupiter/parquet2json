use lazy_static::lazy_static;
use std::thread;
use std::sync::Arc;
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use parquet::errors::{Result};
use parquet::file::reader::{ChunkReader, Length};
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::ops::Deref;
use bytes::Bytes;
use regex::Regex;
use bytes::BufMut;

enum Range {
  FromPositionTo(u64, u64),
  FromEnd(i64)
}

fn construct_headers(range: Range) -> HeaderMap {
  let mut headers = HeaderMap::new();

  let range_str = match range {
    Range::FromPositionTo(start_pos, end_pos) => format!("bytes={}-{}", start_pos, end_pos),
    Range::FromEnd(end_pos) => format!("bytes={}", end_pos)
  };

  headers.insert("Range", HeaderValue::from_str(range_str.as_str()).unwrap());
  headers
}

struct ContentRange {
  start_pos: u64,
  end_pos: u64,
  total_length: u64,
}

fn get_content_range(response: &Response) -> ContentRange {
  let content_range_value = response.headers().get("Content-Range").unwrap().to_str().unwrap();
  lazy_static! {
    static ref BYTES_REGEX: Regex = Regex::new(
        r"bytes (\d+)-(\d+)/([0-9*]+)"
    ).unwrap();
  };
  println!("{}", content_range_value);
  let content_range_captures = BYTES_REGEX.captures(content_range_value).unwrap();
  
  ContentRange {
    start_pos: content_range_captures.get(1).unwrap().as_str().parse::<u64>().unwrap(),
    end_pos: content_range_captures.get(2).unwrap().as_str().parse::<u64>().unwrap(),
    total_length: content_range_captures.get(3).unwrap().as_str().parse::<u64>().unwrap(),
  }
}

fn fetch_range(client: Arc<Client>, url: String, range: Range) -> Response {
  // match range {
  //   Range::FromPositionTo(from, to) => println!("{}-{}", from, to),
  //   Range::FromEnd(length) => println!("{}", length)
  // }
  client.get(url).headers(construct_headers(range)).send().unwrap()
}

pub struct HttpChunkReader {
  client: Arc<Client>,
  url: String,
  start_pos: u64,
  end_pos: u64,
  total_size: u64,
  writer: Option<Response>
}

impl HttpChunkReader {
  pub fn new(client: Arc<Client>, url: String, total_size: u64) -> HttpChunkReader {
    HttpChunkReader {
      client,
      url,
      start_pos: 0,
      end_pos: 0,
      total_size,
      writer: None
    }
  }

  pub fn new_unknown_size(client: Arc<Client>, url: String) -> HttpChunkReader {
    let response = fetch_range(client.clone(), url.clone(), Range::FromEnd(-4));
    let content_range = get_content_range(&response);
    Self::new(client.clone(), url.clone(), content_range.total_length)
  }
}

impl Length for HttpChunkReader {
  fn len(&self) -> u64 {
      self.total_size
  }
}

impl Read for HttpChunkReader {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    println!("Read {}({})", self.start_pos, self.end_pos);
    match self.writer.take() {
      Some(mut writer) => {        
        let result = Read::read(&mut writer, buf);
        self.writer = Some(writer);
        result
      },
      None => {
        let mut writer = fetch_range(self.client.clone(), self.url.clone(), Range::FromPositionTo(self.start_pos, self.end_pos));
        let result = Read::read(&mut writer, buf);
        self.writer = Some(writer);        
        result
      }
    }
  }
}

impl ChunkReader for HttpChunkReader {
  type T = HttpChunkReader;

  fn get_read(&self, start_pos: u64, length: usize) -> Result<Self::T> {
    println!("New ChunkReader {}({})", start_pos, length);
    Ok(HttpChunkReader {
      client: self.client.clone(),
      url: self.url.clone(),
      start_pos,
      end_pos: start_pos + (length as u64) - 1,
      total_size: self.total_size,
      writer: None
    })
  }
}
