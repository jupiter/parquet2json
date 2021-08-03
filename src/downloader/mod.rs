
use std::thread;
use std::sync::mpsc::channel;


struct Downloader {
    url: String,
    length: u64,
}


enum DownloadMessage {
    Create(),
    Extend(),
}


impl Downloader {
    pub fn new<F>(
        url: String,
        length: u64,
        // cache: Arc<RangeCache>,
        // dler_id: String,
        // dler_creator: F,
    ) -> Self
    // where
    //     F: Fn() -> Arc<dyn Downloader>,
    {
        Downloader {
            url,
            length,
        }
    }

    pub fn start(&self) {
        let (tx, rx) = channel();
        thread::spawn(move|| {
            tx.send(10).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 10);
    }

    pub fn schedule(&self, start: u64, length: usize) {

    }
}