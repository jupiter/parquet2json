use clap::{App, Arg};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

fn print_json_from(file: String, offset: u32, limit: i32) {
    let file = File::open(&Path::new(&file)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    let iter = reader.get_row_iter(None).unwrap();
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

    print_json_from(input, offset, limit);
}
