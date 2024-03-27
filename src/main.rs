use std::sync::Arc;

use arrow_json::writer::LineDelimitedWriter;
use aws_config::profile::load;
use aws_config::profile::profile_file::ProfileFiles;
use aws_types::os_shim_internal::{Env, Fs};
use clap::{Parser, Subcommand};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::schema::printer::print_schema;
use tokio_stream::{self as stream, StreamExt};
use url::Url;
use urlencoding::decode;

#[derive(Parser, Clone)]
#[clap(version, about, long_about = None)]
struct Cli {
    /// Location of Parquet input file (file path, HTTP or S3 URL)
    file: String,

    /// Request timeout in seconds
    #[clap(default_value_t = 60, short, long)]
    timeout: u16,

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
        #[clap(short, long, default_value_t = -1)]
        limit: i32,

        /// Select columns by name (comma,separated,?prefixed_optional)
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

    // let timeout = Duration::from_secs(cli.timeout.into());

    if file.as_str().starts_with("s3://") {
        // Needs compatibility with https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        // Source::S3(file)

        let mut s3_builder: AmazonS3Builder = AmazonS3Builder::from_env();

        match load(
            &Fs::default(),
            &Env::default(),
            &ProfileFiles::default(),
            None,
        )
        .await
        {
            Ok(profile_set) => {
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
            Err(_) => {}
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
        println!("Found Blob with {}B at {}", meta.size, meta.location);

        let reader = ParquetObjectReader::new(storage_container, meta);
        let async_reader_builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let parquet_metadata = async_reader_builder.metadata();
        print_schema(
            &mut std::io::stdout(),
            parquet_metadata.file_metadata().schema(),
        );

        let mut json_writer = LineDelimitedWriter::new(std::io::stdout());
        let mut iter = async_reader_builder.build().unwrap();
        while let Ok(batch) = iter.next().await.unwrap() {
            let _ = json_writer.write(&batch);
        }
        json_writer.finish().unwrap();
    } else if file.as_str().starts_with("http") {
        // Source::Http(file)
    } else {
        // Source::File(file)
    };

    // handle_command(source, timeout, cli.command).await;
}
