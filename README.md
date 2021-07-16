# parquet2json

A command-line tool for converting [Parquet](https://parquet.apache.org) to [newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON).

It uses the excellent [Apache Parquet Official Native Rust Implementation](https://github.com/apache/arrow-rs/tree/master/parquet).

## How to use it

Install from [crates.io](https://crates.io) or download [released executables for your platform](https://github.com/jupiter/parquet2json/releases), and execute from the command line, e.g.:

```shell
$ cargo install parquet2json
$ parquet2json --help

USAGE:
    parquet2json [OPTIONS] <INPUT>

ARGS:
    <INPUT>    Sets the input file

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -l, --limit <NUMBER>     Maximum number of rows to output
    -o, --offset <NUMBER>    Starts outputting from this row
```

### Examples

Use it to stream output to files and other tools such as `grep` and [jq](https://stedolan.github.io/jq/).

#### Output to a file

```shell
$ parquet2json ./myfile.pq > output.ndjson
```

#### Filter with jq

```shell
$ parquet2json ./myfile.pq | jq 'select(.level==3) | .id'
```

## License

Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.
