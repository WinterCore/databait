use std::collections::{HashMap, VecDeque};
use std::ops::BitAnd;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::{fs, io};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, AsyncReadExt};

use crate::implementations::ifc::Database;
use super::segment::FileSegment;

#[derive(Debug)]
pub struct Hash {
    segments: VecDeque<FileSegment>,
    page_size: u64,
    path: PathBuf,
}

impl Hash {
    pub fn new(path: PathBuf, page_size: u64) -> Hash {
        Hash {
            segments: VecDeque::new(),
            page_size,
            path,
        }
    }
}

#[async_trait]
impl Database for Hash {
    async fn reset(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn init(&mut self) -> io::Result<()> {
        fs::create_dir_all(&self.path).await?;
        self.segments = FileSegment::from_dir(&self.path).await?;
        
        if self.segments.len() == 0 {
            let initial_segment = FileSegment::create(&self.path).await?;
            self.segments.push_front(initial_segment);
        }

        Ok(())
    }

    async fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
        let most_recent_segment = self
            .segments
            .front_mut()
            .expect("write: Uninitialized database");

        most_recent_segment.write(key, value).await?;

        Ok(())
    }

    async fn read(&mut self, key: &str) -> io::Result<Option<String>> {

        for segment in self.segments.iter_mut() {
            let data_maybe = match segment.read(key).await {
                Ok(data_maybe) => data_maybe,
                Err(_)         => return Ok(None),
            };

            if let Some(data) = data_maybe {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    async fn delete(&mut self, key: &str) -> io::Result<bool> {

        for segment in self.segments.iter_mut() {
            if segment.delete(key).await? {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use super::*;

    async fn setup() -> io::Result<Hash> {
        let mut instance = Hash::new(PathBuf::from("./.hash-tests"), 1000);
        instance.init().await?;
        instance.reset().await?;

        Ok(instance)
    }

    #[tokio::test]
    async fn should_write_data() -> io::Result<()> {
        let mut db = setup().await?;
        db.write("john", "smith").await?;

        Ok(())
    }

    #[tokio::test]
    async fn should_read_the_last_written_value() -> io::Result<()> {
        let mut db = setup().await?;
        db.write("john", "smith").await?;
        db.write("johnny", "sins").await?;
        db.write("iphone", "5").await?;
        db.write("john", "5").await?;
        db.write("m3n", "hund").await?;

        let value = db.read("john").await?;

        assert!(value.is_some());
        assert_eq!(value.unwrap(), "5");

        Ok(())
    }

    #[tokio::test]
    async fn should_return_none_for_non_existing_keys() -> io::Result<()> {
        let mut db = setup().await?;
        db.write("john", "smith").await?;

        let value = db.read("potato").await?;

        assert!(value.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn should_delete_a_key() -> io::Result<()> {
        let mut db = setup().await?;
        db.write("john", "smith").await?;
        db.write("john", "5").await?;
        db.write("john", "foo").await?;
        db.write("foo", "bar").await?;

        assert!(db.delete("john").await?);

        let value = db.read("john").await?;
        assert!(value.is_none());

        let other = db.read("foo").await?;
        assert!(other.is_some());

        Ok(())
    }
}

