use clap::{App, Arg};
use std::sync::Arc;
use parquet::file::reader::{FileReader, ChunkReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use reqwest::blocking::Client;
use http_reader::HttpChunkReader;
mod http_reader;

enum Source {
    File(String),
    Url(String)
}

fn output_rows<T: 'static>(reader: T, offset: u32, limit: i32) 
where 
    T: ChunkReader
{
    let file_reader = SerializedFileReader::new(reader).unwrap();
    let iter = file_reader.get_row_iter(None).unwrap();

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

fn print_json_from(source: Source, offset: u32, limit: i32) {
    match source {
        Source::File(path) => {
            let file = File::open(&Path::new(&path)).unwrap();
            output_rows(file, offset, limit);
        }
        Source::Url(url) => {
            let reader = HttpChunkReader::new_unknown_size(Arc::new(Client::new()), url);
            output_rows(reader, offset, limit);
        }
    };
}

fn main() {
    let matches = App::new("parquet2json")
        .version("1.0")
        .about("Outputs Parquet as JSON")
        .arg(
            Arg::new("INPUT")
                .about("Sets the Parquet formatted input file")
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
    let input: String = matches.value_of_t("INPUT").unwrap_or_else(|e| e.exit());

    if input.as_str().starts_with("http") {
        print_json_from(Source::Url(input), offset, limit);
    } else {
        print_json_from(Source::File(input), offset, limit);
    }
}
