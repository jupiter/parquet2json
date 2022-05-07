use core::time::Duration;
use std::convert::TryInto;
use std::fs::File;
use std::ops::Add;
use std::path::Path;
use std::sync::Arc;

use clap::{AppSettings, Parser, Subcommand};
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::schema::printer::print_schema;
use parquet::schema::types::Type as SchemaType;
use rusoto_core::Region;
use rusoto_s3::S3Client;
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

fn output_rows<R: 'static + ChunkReader>(
    file_reader: &SerializedFileReader<R>,
    projection: Option<SchemaType>,
    offset: u32,
    limit: i32,
) {
    let mut input_rows_count = 0;
    let mut output_rows_count = 0;
    let mut i = 0;

    for row_group in file_reader.metadata().row_groups() {
        let row_group_rows = row_group.num_rows();
        if input_rows_count + row_group_rows < offset.into() {
            i += 1;
            input_rows_count += row_group_rows;
            continue;
        }

        let row_group_reader = file_reader.get_row_group(i).unwrap();
        i += 1;

        let iter = RowIter::from_row_group(projection.clone(), row_group_reader.as_ref()).unwrap();
        for record in iter {
            input_rows_count += 1;
            if input_rows_count < offset.into() {
                continue;
            }

            output_rows_count += 1;
            if limit > -1 && output_rows_count > limit {
                return;
            }

            println!("{}", record.to_json_value());
        }
    }
}

fn output_thrift_schema<R: 'static + ChunkReader>(file_reader: &SerializedFileReader<R>) {
    let parquet_metadata = file_reader.metadata();
    print_schema(
        &mut std::io::stdout(),
        parquet_metadata.file_metadata().schema(),
    );
}

fn get_projection<R: 'static + ChunkReader>(
    file_reader: &SerializedFileReader<R>,
    column_names: Option<String>,
) -> Option<SchemaType> {
    match column_names {
        Some(names) => {
            let parquet_metadata = file_reader.metadata();
            let schema = parquet_metadata.file_metadata().schema();
            let column_names = names.split(',');
            let mut fields: Vec<Arc<SchemaType>> = vec![];

            for column_name in column_names {
                let found = schema
                    .get_fields()
                    .as_ref()
                    .iter()
                    .find(|field| field.name().eq(column_name));

                match found {
                    Some(field) => fields.push(field.clone()),
                    None => panic!("Column not found ({})", column_name),
                }
            }
            return Some(SchemaType::GroupType {
                basic_info: schema.get_basic_info().clone(),
                fields,
            });
        }
        None => None,
    }
}

fn output_for_command<R: 'static + ChunkReader>(
    file_reader: SerializedFileReader<R>,
    command: &Commands,
) {
    match command {
        Commands::Cat {
            offset,
            limit,
            columns,
        } => {
            let absolute_offset: u32 = if offset.is_negative() {
                let parquet_metadata = file_reader.metadata();
                parquet_metadata
                    .file_metadata()
                    .num_rows()
                    .add(offset + 1)
                    .try_into()
                    .unwrap()
            } else {
                offset.abs().try_into().unwrap()
            };
            let projection = get_projection(&file_reader, columns.clone());
            output_rows(&file_reader, projection, absolute_offset, *limit);
        }
        Commands::Schema {} => {
            output_thrift_schema(&file_reader);
        }
        Commands::Rowcount {} => {
            let parquet_metadata = file_reader.metadata();

            println!("{}", parquet_metadata.file_metadata().num_rows());
        }
    }
}

async fn handle_command(source: Source, timeout: Duration, command: Commands) {
    match source {
        Source::File(path) => {
            let file = File::open(&Path::new(&path)).unwrap();
            let file_reader = SerializedFileReader::new(file).unwrap();

            output_for_command(file_reader, &command);
        }
        Source::Http(url_str) => {
            let mut reader = HttpChunkReader::new_unknown_size(url_str).await;
            reader.start(timeout);

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();

                output_for_command(file_reader, &command);
            });
            blocking_task.await.unwrap();
        }
        Source::S3(url_str) => {
            let url = Url::parse(&url_str).unwrap();
            let host_str = url.host_str().unwrap();
            let key = &url.path()[1..];
            let client = S3Client::new(Region::default());

            let mut reader = S3ChunkReader::new_unknown_size(
                (String::from(host_str), String::from(key)),
                client.clone(),
            )
            .await;
            reader.start(client.clone(), timeout).await;

            let blocking_task = tokio::task::spawn_blocking(move || {
                let file_reader = SerializedFileReader::new(reader).unwrap();

                output_for_command(file_reader, &command);
            });
            blocking_task.await.unwrap();
        }
    };
}

#[derive(Parser)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
#[clap(version, about, long_about = None)]
struct Cli {
    /// Location of Parquet input file (file path, HTTP or S3 URL)
    file: String,

    /// Request timeout in seconds
    #[clap(default_value_t = 60, short, long, parse(try_from_str))]
    timeout: u16,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Outputs data as JSON lines
    Cat {
        /// Starts outputting from this row (first row: 0, last row: -1)
        #[clap(default_value_t = 0, short, long, parse(try_from_str))]
        offset: i64,

        /// Maximum number of rows to output
        #[clap(short, long, parse(try_from_str), default_value_t = -1)]
        limit: i32,

        /// Select columns by name (comma,separated)
        #[clap(short, long)]
        columns: Option<String>,
    },

    /// Outputs the Thrift schema
    Schema {},

    /// Outputs only the total row count
    Rowcount {},
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let file = cli.file;
    let timeout = Duration::from_secs(cli.timeout.into());

    let source = if file.as_str().starts_with("s3://") {
        Source::S3(file)
    } else if file.as_str().starts_with("http") {
        Source::Http(file)
    } else {
        Source::File(file)
    };

    handle_command(source, timeout, cli.command).await;
}
