use std::fmt;
use std::sync::Arc;

use super::error::BuzzError;
use super::range_cache::{CachedRead, Downloader, RangeCache};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

#[derive(Clone)]
pub struct CachedFile {
    dler_id: String,
    file_id: String,
    length: u64,
    cache: Arc<RangeCache>,
    auto_schedule: bool,
}

impl fmt::Debug for CachedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedFile")
            .field("downloader_id", &self.dler_id)
            .field("file_id", &self.file_id)
            .field("length", &self.length)
            .finish()
    }
}

impl CachedFile {
    pub fn new<F>(
        file_id: String,
        length: u64,
        cache: Arc<RangeCache>,
        dler_id: String,
        dler_creator: F,
        auto_schedule: bool,
    ) -> Self
    where
        F: Fn() -> Arc<dyn Downloader>,
    {
        cache.register_downloader(&dler_id, dler_creator);
        CachedFile {
            dler_id,
            file_id,
            length,
            cache,
            auto_schedule,
        }
    }

    pub fn prefetch(&self, start: u64, length: usize) {
        self.cache
            .schedule(self.dler_id.clone(), self.file_id.clone(), start, length);
    }
}

impl Length for CachedFile {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for CachedFile {
    type T = CachedRead;

    fn get_read(&self, start: u64, length: usize) -> ParquetResult<Self::T> {
        // println!("get_read() {} {} {}", start, length, self.dler_id);

        if self.auto_schedule {
            self.cache
                .schedule(self.dler_id.clone(), self.file_id.clone(), start, length);
        }
        self.cache
            .get(self.dler_id.clone(), self.file_id.clone(), start, length)
            .map_err(|e| match e {
                BuzzError::ParquetError(err) => err,
                err => ParquetError::General(format!("{}", err)),
            })
    }
}
