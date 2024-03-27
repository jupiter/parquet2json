use std::ops::Add;
use std::sync::Arc;

use arrow_json::writer::LineDelimitedWriter;
use aws_config::profile::load;
use aws_config::profile::profile_file::ProfileFiles;
use aws_types::os_shim_internal::{Env, Fs};
use clap::{Parser, Subcommand};
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::{async_reader::ParquetObjectReader, ProjectionMask};
use parquet::schema::printer::print_schema;
use tokio_stream::StreamExt;
use url::Url;
use urlencoding::decode;

#[derive(Parser, Clone)]
#[clap(version, about, long_about = None)]
struct Cli {
    /// Location of Parquet input file (file path, HTTP or S3 URL)
    file: String,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Clone)]
enum Commands {
    /// Outputs data as JSON lines
    Cat {
        /// Starts outputting from this row (first row: 0, last row: -1)
        #[clap(default_value_t = 0, short, long)]
        offset: i64,

        /// Maximum number of rows to output
        #[clap(short, long)]
        limit: Option<usize>,

        /// Select columns by name (comma,separated,?prefixed_optional)
        #[clap(short, long)]
        columns: Option<String>,
    },

    /// Outputs the Thrift schema
    Schema {},

    /// Outputs only the total row count
    Rowcount {},
}

async fn output_for_command(mut reader: ParquetObjectReader, command: &Commands) {
    let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
        .await
        .unwrap();
    let metadata_clone = metadata.clone();
    let parquet_metadata = metadata_clone.metadata();
    let mut async_reader_builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata);

    match command {
        Commands::Cat {
            offset,
            limit,
            columns,
        } => {
            let absolute_offset: usize = if offset.is_negative() {
                parquet_metadata
                    .file_metadata()
                    .num_rows()
                    .add(offset)
                    .try_into()
                    .unwrap()
            } else {
                offset.abs().try_into().unwrap()
            };
            async_reader_builder = async_reader_builder.with_offset(absolute_offset);

            if let Some(limit) = limit {
                async_reader_builder = async_reader_builder.with_limit(*limit)
            }

            if let Some(columns) = columns {
                let column_names = columns.split(',');

                let schema_descr = parquet_metadata.file_metadata().schema_descr();
                let root_schema = schema_descr.root_schema().get_fields();

                let mut indices: Vec<usize> = vec![];
                for column_name in column_names {
                    let is_optional = column_name.starts_with('?');
                    let found = root_schema.iter().position(|field| {
                        field.name().eq(if is_optional {
                            &column_name[1..]
                        } else {
                            column_name
                        })
                    });

                    match found {
                        Some(field) => indices.push(field),
                        None => {
                            if !is_optional {
                                panic!("Column not found ({})", column_name)
                            }
                        }
                    }
                }
                let projection_mask = ProjectionMask::roots(schema_descr, indices);
                async_reader_builder = async_reader_builder.with_projection(projection_mask);
            }

            let mut iter = async_reader_builder.build().unwrap();
            let mut json_writer = LineDelimitedWriter::new(std::io::stdout());

            while let Some(Ok(batch)) = iter.next().await {
                let _ = json_writer.write(&batch);
            }
            json_writer.finish().unwrap();
        }
        Commands::Schema {} => {
            print_schema(
                &mut std::io::stdout(),
                parquet_metadata.file_metadata().schema(),
            );
        }
        Commands::Rowcount {} => {
            println!("{}", parquet_metadata.file_metadata().num_rows());
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let file = cli.file;

    if file.as_str().starts_with("s3://") {
        let mut s3_builder: AmazonS3Builder = AmazonS3Builder::from_env();

        if let Ok(profile_set) = load(
            &Fs::default(),
            &Env::default(),
            &ProfileFiles::default(),
            None,
        )
        .await
        {
            if let Some(aws_access_key_id) = profile_set.get("aws_access_key_id") {
                s3_builder = s3_builder.with_access_key_id(aws_access_key_id);
            }
            if let Some(aws_secret_access_key) = profile_set.get("aws_secret_access_key") {
                s3_builder = s3_builder.with_secret_access_key(aws_secret_access_key);
            }
            if let Some(aws_session_token) = profile_set.get("aws_session_token") {
                s3_builder = s3_builder.with_token(aws_session_token);
            }
            if let Some(region) = profile_set.get("region") {
                s3_builder = s3_builder.with_region(region);
            }
        }

        let url = Url::parse(file.as_ref()).unwrap();

        let storage_container = Arc::new(
            s3_builder
                .with_bucket_name(decode(url.host_str().unwrap()).unwrap())
                .build()
                .unwrap(),
        );
        let location = Path::from(decode(url.path()).unwrap().as_ref());
        let meta = storage_container.head(&location).await.unwrap();
        let reader = ParquetObjectReader::new(storage_container, meta);

        output_for_command(reader, &cli.command).await;
    } else if file.as_str().starts_with("http") {
        let url = Url::parse(file.as_ref()).unwrap();

        let storage_container = Arc::new(HttpBuilder::new().with_url(url).build().unwrap());
        let location = Path::from("");
        let meta = storage_container.head(&location).await.unwrap();
        let reader = ParquetObjectReader::new(storage_container, meta);

        output_for_command(reader, &cli.command).await;
    } else {
        let storage_container = Arc::new(LocalFileSystem::new());
        let str: &str = file.as_ref();
        let file_path_buf = std::fs::canonicalize(str).unwrap();
        let file_path = file_path_buf.to_str().unwrap();
        let location = Path::from(file_path);
        let meta = storage_container.head(&location).await.unwrap();
        let reader = ParquetObjectReader::new(storage_container, meta);

        output_for_command(reader, &cli.command).await;
    };
}
