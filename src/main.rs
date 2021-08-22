use std::fs::File;
use std::path::Path;

use clap::{App, Arg};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
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
        Source::S3(url_str) => {
            let url = Url::parse(&url_str).unwrap();
            let host_str = url.host_str().unwrap();
            let key = &url.path()[1..];

            let mut reader = S3ChunkReader::new_unknown_size(
                (String::from(host_str), String::from(key)),
                Region::default(),
            )
            .await;
            reader.start(Region::default()).await;

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();
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
        .get_matches();

    let offset: u32 = matches.value_of_t("offset").unwrap_or(0);
    let limit: i32 = matches.value_of_t("limit").unwrap_or(-1);
    let file: String = matches.value_of_t("FILE").unwrap_or_else(|e| e.exit());

    if file.as_str().starts_with("s3://") {
        print_json_from(Source::S3(file), offset, limit).await;
    } else if file.as_str().starts_with("http") {
        print_json_from(Source::Http(file), offset, limit).await;
    } else {
        print_json_from(Source::File(file), offset, limit).await;
    }
}
