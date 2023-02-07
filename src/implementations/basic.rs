use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tokio::io;
use crate::implementations::ifc::Database;

pub struct Basic<'a> {
    folder_path: &'a Path,
}

impl<'a> Basic<'a> {
    fn new() -> Basic<'a> {
        Basic {
            folder_path: Path::new("./.basic"),
        }
    }
}

#[async_trait]
impl Database for Basic {
    async fn init<'a>(&'a self) -> io::Result<()> {
        fs::create_dir(&self.folder_path).await?;
        fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(self.folder_path.join("data"));

        Ok(())
    }

    async fn write<K: ToString, V: ToString>(&self, key: &K, value: &V) -> () {

    }

    async fn read(&self, key: &str) -> String {
        String::from("")
    }
}
