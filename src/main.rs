use clap::{Parser, Subcommand};

#[derive(Parser)]
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

#[derive(Subcommand)]
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
    // let file = cli.file;
    // let timeout = Duration::from_secs(cli.timeout.into());

    // let source = if file.as_str().starts_with("s3://") {
    //     Source::S3(file)
    // } else if file.as_str().starts_with("http") {
    //     Source::Http(file)
    // } else {
    //     Source::File(file)
    // };

    // handle_command(source, timeout, cli.command).await;
}
