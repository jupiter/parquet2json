use std::convert::TryInto;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use clap::{App, Arg};
use parquet::file::reader::{FileReader, Length, SerializedFileReader};
use parquet::record::reader::RowIter;
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Output, ListObjectsV2Request, S3Client, S3};
use url::Url;

mod http_reader;
use http_reader::HttpChunkReader;
mod buzz;
use buzz::{s3, CachedFile, RangeCache};

enum Source {
    File(String),
    Http(String),
    S3(String, usize),
}

fn output_rows(iter: RowIter, offset: u32, limit: i32) {
    let mut input_rows_count = 0;
    let mut output_rows_count = 0;
    for record in iter {
        input_rows_count += 1;
        if input_rows_count < offset {
            continue;
        }

        output_rows_count += 1;
        if limit > -1 && output_rows_count > limit {
            return;
        }

        println!("{}", record.to_json_value());
    }
}

async fn print_json_from(source: Source, offset: u32, limit: i32) {
    match source {
        Source::File(path) => {
            let file = File::open(&Path::new(&path)).unwrap();
            let file_reader = SerializedFileReader::new(file).unwrap();
            output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
        }
        Source::Http(url_str) => {
            let mut reader = HttpChunkReader::new_unknown_size(url_str).await;
            reader.start();

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();
                output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
            });
            blocking_task.await.unwrap();
        }
        Source::S3(url_str, concurrency) => {
            let url = Url::parse(&url_str).unwrap();
            let host_str = url.host_str().unwrap();
            let key = &url.path()[1..];

            let s3_client = S3Client::new(Region::default());
            let list_req = ListObjectsV2Request {
                bucket: String::from(host_str),
                prefix: Some(String::from(key)),
                ..Default::default()
            };
            let list_res: ListObjectsV2Output = s3_client.list_objects_v2(list_req).await.unwrap();
            let object_found = &list_res.contents.unwrap()[0];
            let size = object_found.size.unwrap().try_into().unwrap();

            let is_parallel = concurrency > 1;
            let cache = RangeCache::new(concurrency).await;
            let (dler_id, dler_creator) = s3::downloader_creator(Region::default().name());
            let file_id = s3::file_id(host_str, key);
            let file = CachedFile::new(
                file_id,
                size,
                Arc::new(cache),
                dler_id,
                dler_creator,
                is_parallel,
            );

            let pre_size: u64 = if is_parallel { 1024 * 1024 } else { size - 4 };
            file.prefetch(file.len() - pre_size, pre_size as usize);

            let file_reader = SerializedFileReader::new(file).unwrap();

            let blocking_task = tokio::task::spawn_blocking(move || {
                output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
            });
            blocking_task.await.unwrap();
        }
    };
}

#[tokio::main]
async fn main() {
    let matches = App::new("parquet2json")
        .version("1.1.0")
        .about("Outputs Parquet as JSON")
        .arg(
            Arg::new("FILE")
                .about("Location of Parquet input file (path, HTTP or S3 URL)")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("offset")
                .short('o')
                .long("offset")
                .value_name("NUMBER")
                .about("Starts outputting from this row")
                .takes_value(true),
        )
        .arg(
            Arg::new("limit")
                .short('l')
                .long("limit")
                .value_name("NUMBER")
                .about("Maximum number of rows to output")
                .takes_value(true),
        )
        .arg(
            Arg::new("concurrency")
                .short('c')
                .long("concurrency")
                .value_name("NUMBER")
                .about("Maximum number of concurrent column downloads (S3 only, default: 10)")
                .takes_value(true),
        )
        .get_matches();

    let offset: u32 = matches.value_of_t("offset").unwrap_or(0);
    let limit: i32 = matches.value_of_t("limit").unwrap_or(-1);
    let file: String = matches.value_of_t("FILE").unwrap_or_else(|e| e.exit());
    let concurrency: usize = matches.value_of_t("concurrency").unwrap_or(10);

    if file.as_str().starts_with("s3://") {
        print_json_from(Source::S3(file, concurrency), offset, limit).await;
    } else if file.as_str().starts_with("http") {
        print_json_from(Source::Http(file), offset, limit).await;
    } else {
        print_json_from(Source::File(file), offset, limit).await;
    }
}
