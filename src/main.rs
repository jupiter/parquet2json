use core::time::Duration;
use std::fs::File;
use std::path::Path;

use clap::{value_t, App, Arg};
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::schema::printer::print_schema;
use rusoto_core::Region;
use url::Url;

mod http_reader;
use http_reader::HttpChunkReader;
mod s3_reader;
use s3_reader::S3ChunkReader;

enum Source {
    File(String),
    Http(String),
    S3(String),
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

fn output_thrift_schema<R: 'static + ChunkReader>(file_reader: &SerializedFileReader<R>) {
    let parquet_metadata = file_reader.metadata();
    print_schema(
        &mut std::io::stdout(),
        parquet_metadata.file_metadata().schema(),
    );
}

async fn print_json_from(
    source: Source,
    offset: u32,
    limit: i32,
    should_output_schema: bool,
    timeout: Duration,
) {
    match source {
        Source::File(path) => {
            let file = File::open(&Path::new(&path)).unwrap();
            let file_reader = SerializedFileReader::new(file).unwrap();

            if should_output_schema {
                output_thrift_schema(&file_reader);
            } else {
                output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
            }
        }
        Source::Http(url_str) => {
            let mut reader = HttpChunkReader::new_unknown_size(url_str).await;
            reader.start(timeout);

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();

                if should_output_schema {
                    output_thrift_schema(&file_reader);
                } else {
                    output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
                }
            });
            blocking_task.await.unwrap();
        }
        Source::S3(url_str) => {
            let url = Url::parse(&url_str).unwrap();
            let host_str = url.host_str().unwrap();
            let key = &url.path()[1..];

            let mut reader = S3ChunkReader::new_unknown_size(
                (String::from(host_str), String::from(key)),
                Region::default(),
            )
            .await;
            reader.start(Region::default(), timeout).await;

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();

                if should_output_schema {
                    output_thrift_schema(&file_reader);
                } else {
                    output_rows(file_reader.get_row_iter(None).unwrap(), offset, limit);
                }
            });
            blocking_task.await.unwrap();
        }
    };
}

#[tokio::main]
async fn main() {
    let matches = App::new("parquet2json")
        .about("Outputs Parquet as JSON")
        .arg(
            Arg::with_name("FILE")
                .help("Location of Parquet input file (path, HTTP or S3 URL)")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("output_thrift_schema")
                .short(String::from("t"))
                .long("output_thrift_schema")
                .value_name("BOOLEAN")
                .help("Outputs thrift schema first")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("offset")
                .short(String::from('o'))
                .long("offset")
                .value_name("NUMBER")
                .help("Starts outputting from this row")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("limit")
                .short(String::from('l'))
                .long("limit")
                .value_name("NUMBER")
                .help("Maximum number of rows to output")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("timeout")
                .short(String::from('t'))
                .long("timeout")
                .value_name("NUMBER")
                .help("Request timeout in seconds")
                .takes_value(true),
        )
        .get_matches();

    let output_thrift_schema: bool =
        value_t!(matches, "output_thrift_schema", bool).unwrap_or(false);
    let offset: u32 = value_t!(matches, "offset", u32).unwrap_or(0);
    let limit: i32 = value_t!(matches, "limit", i32).unwrap_or(-1);
    let timeout = Duration::from_secs(value_t!(matches, "timeout", u32).unwrap_or(60).into());
    let file: String = value_t!(matches, "FILE", String).unwrap_or_else(|e| e.exit());

    if file.as_str().starts_with("s3://") {
        print_json_from(
            Source::S3(file),
            offset,
            limit,
            output_thrift_schema,
            timeout,
        )
        .await;
    } else if file.as_str().starts_with("http") {
        print_json_from(
            Source::Http(file),
            offset,
            limit,
            output_thrift_schema,
            timeout,
        )
        .await;
    } else {
        print_json_from(
            Source::File(file),
            offset,
            limit,
            output_thrift_schema,
            timeout,
        )
        .await;
    }
}
