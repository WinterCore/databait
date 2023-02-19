use std::collections::{HashMap, VecDeque};
use std::ops::BitAnd;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::{fs, io};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, AsyncReadExt};

use crate::implementations::ifc::Database;

#[derive(Debug)]
struct FileSegment {
    fd: fs::File,
    num: u32,
}

#[derive(Debug)]
struct HashMemEntry {
    segment_num: u32,
    pos: u64,
}

#[derive(Debug)]
pub struct Hash {
    segments: VecDeque<FileSegment>,
    hash: HashMap<String, HashMemEntry>,
    page_size: u64,
    path: PathBuf,
}

impl Hash {
    const KEY_MAX_LEN: usize = 128;

    pub fn new(path: PathBuf, page_size: u64) -> Hash {
        Hash {
            segments: VecDeque::new(),
            hash: HashMap::new(),
            page_size,
            path,
        }
    }

    fn get_segment_name(num: u32) -> String {
        format!("segment-{:0>6}", num)
    }

    fn get_segment_number(name: &str) -> u32 {
        name[8..]
            .parse()
            .unwrap()
    }

    async fn read_entry(reader: &mut io::BufReader<&mut fs::File>) -> io::Result<Option<(String, String)>> {
        let mut value_len_buf = [0u8; 8];
        let value_len_result = reader.read_exact(&mut value_len_buf).await;

        if let Err(_) = &value_len_result {
            return Ok(None);
        }

        let value_len = u64::from_ne_bytes(value_len_buf);

        let mut tombstone_buf = vec![0u8; 1];
        reader.read_exact(&mut tombstone_buf).await?;

        if tombstone_buf[0] != 0 {
            return Ok(None);
        }

        let mut key_buf = vec![0u8; Hash::KEY_MAX_LEN];
        reader.read_exact(&mut key_buf).await?;
        let mut value_buf = vec![0u8; value_len as usize];
        reader.read_exact(&mut value_buf).await?;

        let key = String::from_utf8_lossy(&key_buf)
            .trim_end_matches(char::from(0))
            .to_owned();
        let value = String::from_utf8_lossy(&value_buf).into_owned();

        Ok(Some((key, value)))
    }

    async fn init_hash(&mut self) -> io::Result<()> {
        self.hash.clear();

        for FileSegment { fd, num } in self.segments.iter_mut() {
            fd.rewind().await?;

            let mut seg_hash: HashMap<String, HashMemEntry> = HashMap::new();

            let mut reader = io::BufReader::new(fd);

            let mut pos = reader.stream_position().await?;

            while let Some((key, _)) = Self::read_entry(&mut reader).await? {
                // TODO: Perf improvement. it might be better to write all the values into seg_hash
                // and then filter them before copying to the master hash
                let exists = self.hash.contains_key(&key);

                if exists {
                    continue;
                }

                seg_hash.insert(key, HashMemEntry { segment_num: *num, pos });

                pos = reader.stream_position().await?;
            }

            self.hash.extend(seg_hash);
        }

        Ok(())
    }

    async fn split(&mut self) -> io::Result<()> {
        let FileSegment { fd, num } = self.segments.front_mut().expect("Uninitialized database");
        let pos = fd.seek(io::SeekFrom::End(0)).await?;

        if pos >= self.page_size {
            let new_seg_num = *num + 1;
            let name = Self::get_segment_name(new_seg_num);

            let fd = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(self.path.join(&name))
                .await?;

            self.segments.push_front(FileSegment {
                fd,
                num: new_seg_num,
            });
        }
        
        Ok(())
    }

    async fn merge(&mut self) -> io::Result<()> {
    }
}

#[async_trait]
impl Database for Hash {
    async fn init(&mut self) -> io::Result<()> {
        fs::create_dir_all(&self.path).await?;

        let mut folders_stream = fs::read_dir(&self.path).await?;
        let mut all_files: Vec<String> = vec![];

        while let Some(item) = folders_stream.next_entry().await? {
            all_files.push(item.file_name().to_string_lossy().to_string());
        }

        let mut segment_files: Vec<String> = all_files
            .into_iter()
            .filter(|f| f.starts_with("segment-"))
            .collect();

        segment_files.sort();

        // If we have no existing segment files, create one.
        if segment_files.len() == 0 {
            segment_files.push(Self::get_segment_name(0));
        }

        // TODO Improvement: Open these files in parallel
        for name in segment_files.into_iter() {
            let fd = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(self.path.join(&name))
                .await?;

            let num = Self::get_segment_number(&name);

            self.segments.push_front(FileSegment {
                fd,
                num,
            });
        }


        self.init_hash().await?;

        Ok(())
    }

    async fn reset(&mut self) -> io::Result<()> {
        let path = &self.path;

        for FileSegment { fd, num } in self.segments.iter() {
            let name = Self::get_segment_name(*num);
            fs::remove_file(path.join(name)).await?;
            drop(fd);
        }

        self.segments.truncate(0);
        
        self.init().await?;
        Ok(())
    }

    async fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
        let key_len = key.len();

        if key_len > Self::KEY_MAX_LEN as usize {
            return Err(io::Error::new(io::ErrorKind::Other, "Key too large"));
        }

        self.split().await?;

        let FileSegment { fd, num } = self
            .segments
            .front_mut()
            .expect("Uninitialized database");

        let pos = fd.stream_position().await?;
        let value_len = value.len() as u64;
        let mut padded_key = key.as_bytes().to_vec();
        padded_key.extend(vec![0; Self::KEY_MAX_LEN as usize - key_len]);

        let mut buf: Vec<u8> = vec![];
        buf.extend(value_len.to_ne_bytes());
        buf.extend([0]);
        buf.extend(padded_key);
        buf.extend(value.as_bytes());

        fd.write(&buf).await?;

        self.hash.insert(
            key.to_owned(),
            HashMemEntry {
                segment_num: *num,
                pos,
            }
        );

        Ok(())
    }

    async fn read(&mut self, key: &str) -> io::Result<Option<String>> {
        let existing = self.hash.get(key);
        
        if let Some(&HashMemEntry { segment_num, pos }) = existing {
            let segment_maybe = self.segments
                .iter_mut()
                .find(|x| x.num == segment_num);

            if let Some(FileSegment { fd, num: _ }) = segment_maybe {
                let mut reader = io::BufReader::new(fd);
                reader.seek(io::SeekFrom::Start(pos)).await?;
                let maybe_data = Self::read_entry(&mut reader).await?;

                if let None = maybe_data {
                    return Ok(None);
                }

                let (found_key, value) = maybe_data.unwrap();

                if found_key != key {
                    println!("Key mismatch {} != {}", found_key, key);
                    return Ok(None);
                }

                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn delete(&mut self, key: &str) -> io::Result<bool> {
        let existing = self.hash.get(key);

        if let None = existing {
            return Ok(false);
        }

        let HashMemEntry { segment_num, pos } = existing.unwrap();

        let segment_maybe = self.segments
            .iter_mut()
            .find(|x| x.num == *segment_num);

        if let None = segment_maybe {
            return Ok(false);
        }

        let FileSegment { fd, num: _ } = segment_maybe.unwrap();
        fd.seek(io::SeekFrom::Start(*pos + Self::KEY_MAX_LEN as u64)).await?;
        fd.write_u8(1).await?;
        self.hash.remove(key);

        Ok(true)
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

