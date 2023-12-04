# parquet2json

A command-line tool for streaming [Parquet](https://parquet.apache.org) as [line-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON).

It reads only required ranges from file, HTTP or S3 locations, and supports offset/limit and column selection.

It uses the [Apache Parquet Official Native Rust Implementation](https://github.com/apache/arrow-rs/tree/master/parquet) which has excellent support for compression formats and complex types.

## How to use

Install from [crates.io](https://crates.io) and execute from the command line, e.g.:

```shell
$ cargo install parquet2json
$ parquet2json --help

USAGE:
    parquet2json [OPTIONS] <FILE> <SUBCOMMAND>

ARGS:
    <FILE>    Location of Parquet input file (file path, HTTP or S3 URL)

OPTIONS:
    -t, --timeout <TIMEOUT>    Request timeout in seconds [default: 60]
    -h, --help                 Print help information
    -V, --version              Print version information

SUBCOMMANDS:
    cat         Outputs data as JSON lines
    schema      Outputs the Thrift schema
    rowcount    Outputs only the total row count
    help        Print this message or the help of the given subcommand(s)

$ parquet2json cat --help

USAGE:
    parquet2json <FILE> cat [OPTIONS]

OPTIONS:
    -o, --offset <OFFSET>      Starts outputting from this row (first row: 0, last row: -1)
                               [default: 0]
    -l, --limit <LIMIT>        Maximum number of rows to output [default: -1]
    -c, --columns <COLUMNS>    Select columns by name (comma,separated,?prefixed_optional)
    -h, --help                 Print help information
```

### S3 Settings

Credentials are provided as per standard AWS toolchain, i.e. per environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`), AWS credentials file or IAM ECS container/instance profile.

The default AWS region must be set per environment variable (`AWS_DEFAULT_REGION`) in AWS credentials file and must match region of the object's bucket.

### Examples

Use it to stream output to files and other tools such as `grep` and [jq](https://stedolan.github.io/jq/).

#### Output to a file

```shell
$ parquet2json ./myfile.pq cat > output.jsonl
```

#### From S3 or HTTP (S3)

```shell
$ parquet2json s3://noaa-ghcn-pds/parquet/by_year/YEAR=2022/ELEMENT=ADPT/c771f8c0ea844998a1c8a9d5b8f269db_0.snappy.parquet cat
```

```shell
$ parquet2json https://noaa-ghcn-pds.s3.amazonaws.com/parquet/by_year/YEAR%3D2022/ELEMENT%3DADPT/c771f8c0ea844998a1c8a9d5b8f269db_0.snappy.parquet cat
```

#### Filter selected columns with jq

```shell
$ parquet2json ./myfile.pq cat --columns=url,level | jq 'select(.level==3) | .url'
```

## License

[MIT](LICENSE.md)
