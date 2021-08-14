#[allow(clippy::all)]
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use super::error::Result;
use crate::{ensure, internal_err};
use async_trait::async_trait;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel, Receiver};

/// A reader that points to a cached chunk
/// TODO this cannot read from multiple concatenated chunks
pub struct CachedReadData {
    data: Vec<u8>,
    position: u64,
    remaining: u64,
}

impl Read for CachedReadData {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // println!("read() {} {}", buf.len(), self.remaining);
        // compute len to read
        let len = std::cmp::min(buf.len(), self.remaining as usize);
        // get downloaded data
        buf[0..len]
            .copy_from_slice(&self.data[self.position as usize..(self.position as usize + len)]);

        // update reader position
        self.remaining -= len as u64;
        self.position += len as u64;
        Ok(len)
    }
}

pub struct CachedRead {
    cached_read_data: Option<CachedReadData>,
    rx: Receiver<Result<CachedReadData>>,
}

impl Read for CachedRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.cached_read_data {
            None => {
                loop {
                    match self.rx.try_recv() {
                        Ok(download_result) => {
                            let mut read = download_result.unwrap();
                            let result = read.read(buf);
                            self.cached_read_data = Some(read);
                            return result;
                        }
                        Err(_) => {
                            // println!("{}", e);
                            continue;
                        }
                    }
                }
            }
            Some(read) => {
                let result = read.read(buf);
                result
            }
        }
    }
}

/// The status and content of the download
enum Download {
    Pending,
    Done(Mutex<Option<Vec<u8>>>),
    Error(String),
}

/// An "all or nothing" representation of the download.
#[async_trait]
pub trait Downloader: Send + Sync {
    async fn download(&self, file_id: String, start: u64, length: usize) -> Result<Vec<u8>>;
}

type DownloaderId = String;
type FileId = String;
type FileData = BTreeMap<u64, Download>;
type CacheKey = (DownloaderId, FileId);
type CacheData = Arc<Mutex<HashMap<CacheKey, FileData>>>;
type DownloaderMap = Arc<Mutex<HashMap<DownloaderId, Arc<dyn Downloader>>>>;
type DownloadRequest = (DownloaderId, FileId, u64, usize);

/// A caching struct that queues up download requests and executes them with
/// the appropriate registered donwloader.
pub struct RangeCache {
    data: CacheData,
    downloaders: DownloaderMap,
    cv: Arc<std::sync::Condvar>,
    tx: UnboundedSender<DownloadRequest>,
}

impl RangeCache {
    /// Spawns a task that will listen for new chunks to download and schedule them for download
    pub async fn new(concurrent_downloads: usize) -> Self {
        let (tx, rx) = unbounded_channel::<DownloadRequest>();
        let cache = Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            downloaders: Arc::new(Mutex::new(HashMap::new())),
            cv: Arc::new(std::sync::Condvar::new()),
            tx,
        };
        cache.start(rx, concurrent_downloads).await;
        cache
    }

    pub async fn start(
        &self,
        mut rx: UnboundedReceiver<DownloadRequest>,
        concurrent_downloads: usize,
    ) {
        let data_ref = Arc::clone(&self.data);
        let cv_ref = Arc::clone(&self.cv);
        let downloaders_ref = Arc::clone(&self.downloaders);
        tokio::spawn(async move {
            let pool = Arc::new(tokio::sync::Semaphore::new(concurrent_downloads));
            while let Some(message) = rx.recv().await {
                // obtain a permit, it will be released in the spawned download task
                let permit = pool.acquire().await.unwrap();
                permit.forget();
                // run download in a dedicated task
                let downloaders_ref = Arc::clone(&downloaders_ref);
                let data_ref = Arc::clone(&data_ref);
                let cv_ref = Arc::clone(&cv_ref);
                let pool_ref = Arc::clone(&pool);
                tokio::spawn(async move {
                    // get ref to donwloader
                    let downloader;
                    {
                        let downloaders_guard = downloaders_ref.lock().unwrap();
                        let downloader_ref = downloaders_guard
                            .get(&message.0)
                            .expect("Downloader not found");
                        downloader = Arc::clone(downloader_ref);
                    }
                    // download using that ref
                    let downloaded_res = downloader
                        .download(message.1.clone(), message.2, message.3)
                        .await;

                    pool_ref.add_permits(1);
                    // update the cache data with the result
                    let mut data_guard = data_ref.lock().unwrap();
                    let file_map = data_guard
                        .entry((message.0, message.1))
                        .or_insert_with(|| BTreeMap::new());
                    match downloaded_res {
                        Ok(downloaded_chunk) => {
                            file_map.insert(
                                message.2,
                                Download::Done(Mutex::new(Some(downloaded_chunk))),
                            );
                        }
                        Err(err) => {
                            file_map.insert(message.2, Download::Error(err.reason()));
                        }
                    }
                    cv_ref.notify_all();
                });
            }
        });
    }

    /// Registers a new downloader for the given id if necessary
    pub fn register_downloader<F>(&self, downloader_id: &str, downloader_creator: F)
    where
        F: Fn() -> Arc<dyn Downloader>,
    {
        let mut dls_guard = self.downloaders.lock().unwrap();
        let current = dls_guard.get(downloader_id);
        if current.is_none() {
            dls_guard.insert(downloader_id.to_owned(), downloader_creator());
        }
    }

    /// Add a new chunk to the download queue
    pub fn schedule(
        &self,
        downloader_id: DownloaderId,
        file_id: FileId,
        start: u64,
        length: usize,
    ) {
        let mut data_guard = self.data.lock().unwrap();
        let file_map = data_guard
            .entry((downloader_id.clone(), file_id.clone()))
            .or_insert_with(|| BTreeMap::new());
        file_map.insert(start, Download::Pending);
        self.tx
            .send((downloader_id, file_id, start, length))
            .unwrap();
    }

    /// Get a chunk from the cache
    /// For now the cache can only get get single chunck readers and fails if the dl was not scheduled
    /// If the download is not finished, this waits synchronously for the chunk to be ready
    pub fn get(
        &self,
        downloader_id: DownloaderId,
        file_id: FileId,
        start: u64,
        length: usize,
    ) -> Result<CachedRead> {
        use std::ops::Bound::{Included, Unbounded};

        let (tx, rx) = channel::<Result<CachedReadData>>();
        let data_ref = Arc::clone(&self.data);
        let cv_ref = Arc::clone(&self.cv);

        std::thread::spawn(move || {
            let mut data_guard = data_ref.lock().unwrap();
            let identifier = (downloader_id.clone(), file_id.clone());
            let file_map = data_guard.get(&identifier).ok_or(internal_err!(
                "No download scheduled for file: (donwloader={},file_id={})",
                &downloader_id,
                &file_id,
            ))?;

            let mut before = file_map.range((Unbounded, Included(start))).next_back();

            while let Some((_, Download::Pending)) = before {
                // wait for the dl to be finished
                data_guard = cv_ref.wait(data_guard).unwrap();
                before = data_guard
                    .get(&identifier)
                    .expect("files should not disappear during download")
                    .range((Unbounded, Included(start)))
                    .next_back();
            }

            let before = before.ok_or(internal_err!(
                "Download not scheduled: (start={},length={})",
                start,
                length,
            ))?;

            let unused_start = start - before.0;

            let result = match before.1 {
                Download::Done(bytes_lock) => {
                    let bytes = bytes_lock.lock().unwrap().take().unwrap();
                    ensure!(
                        bytes.len() >= unused_start as usize + length,
                        "Download not scheduled (overflow right): (start={},length={})",
                        start,
                        length,
                    );

                    Ok(CachedReadData {
                        data: bytes,
                        position: unused_start,
                        remaining: length as u64,
                    })
                }
                Download::Error(_) => unreachable!(),
                Download::Pending => unreachable!(),
            };
            let _ = tx.send(result);
            Ok(())
        });
        Ok(CachedRead {
            cached_read_data: None,
            rx,
        })
    }
}
