//! modules that help connecting to the outside world

mod cached_file;
#[allow(clippy::all)]
pub mod error;
#[allow(clippy::all)]
mod range_cache;
#[allow(clippy::all)]
pub mod s3;

pub use cached_file::CachedFile;
pub use range_cache::{Downloader, RangeCache};
