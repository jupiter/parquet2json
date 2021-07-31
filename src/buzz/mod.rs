//! modules that help connecting to the outside world

mod cached_file;
mod range_cache;
pub mod error;
pub mod s3;

pub use cached_file::CachedFile;
pub use range_cache::{Downloader, RangeCache};
