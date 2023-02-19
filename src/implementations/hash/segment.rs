use tokio::{fs, io::{self, AsyncSeekExt}};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use super::data::{read_entry, write_entry};

#[derive(Debug)]
pub struct FileSegment {
    fd: fs::File,
    num: u32,
    hash: HashMap<String, u64>
}


impl FileSegment {
    const SEG_ID: &str = "segment-";

    fn get_segment_number(name: &str) -> Option<u32> {
        let result = name[Self::SEG_ID.len()..].parse();

        result.ok()
    }

    fn get_segment_name(num: u32) -> String {
        format!("{}{:0>6}", Self::SEG_ID, num)
    }

    async fn generate_hash_for_file(fd: &mut fs::File) -> io::Result<HashMap<String, u64>> {
        fd.rewind().await?;
        let mut hash: HashMap<String, u64> = HashMap::new();

        let mut reader = io::BufReader::new(fd);

        let mut pos = reader.stream_position().await?;

        while let Some((key, _)) = read_entry(&mut reader).await? {
            hash.insert(key, pos);

            pos = reader.stream_position().await?;
        }

        Ok(hash)
    }

    async fn get_filesystem_segments(path: &PathBuf) -> io::Result<Vec<String>> {
        let mut folders_stream = fs::read_dir(&path).await?;
        let mut all_files: Vec<String> = vec![];

        while let Some(item) = folders_stream.next_entry().await? {
            all_files.push(item.file_name().to_string_lossy().to_string());
        }

        let mut segment_files: Vec<String> = all_files
            .into_iter()
            .filter(|f| f.starts_with("segment-"))
            .collect();

        
        segment_files.sort();

        Ok(segment_files)
    }

    pub async fn create(path: &PathBuf) -> io::Result<Self> {
        let segment_files = Self::get_filesystem_segments(path).await?;

        let num = segment_files
            .last()
            .and_then(|name| Self::get_segment_number(name))
            .map(|x| x + 1)
            .unwrap_or(0);

        let fd = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join(Self::get_segment_name(num)))
            .await?;

        Ok(Self {
            fd,
            num,
            hash: HashMap::new(),
        })
    }

    pub async fn from_dir(path: &PathBuf) -> io::Result<VecDeque<Self>> {
        let segment_files = Self::get_filesystem_segments(path).await?;

        let mut segments: VecDeque<FileSegment> = VecDeque::new();

        for name in segment_files.into_iter() {
            let mut fd = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join(&name))
                .await?;

            let num = match Self::get_segment_number(&name) {
                Some(value) => value,
                None => continue,
            };

            let hash = Self::generate_hash_for_file(&mut fd).await?;

            segments.push_front(Self {
                fd,
                num,
                hash,
            });
        }

        Ok(segments)
    }

    pub async fn read(&mut self, key: &str) -> io::Result<Option<String>> {
        
        let pos = *match self.hash.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };


        let mut reader = io::BufReader::new(&mut self.fd);
        reader.seek(io::SeekFrom::Start(pos)).await?;
        let maybe_data = read_entry(&mut reader).await?;

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

    async fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
        let mut reader = io::BufReader::new(&mut self.fd);
        reader.seek(io::SeekFrom::End(0)).await?;

        let pos = write_entry(&mut reader, key, value).await?;
        self.hash.insert(key.to_owned(), pos);

        Ok(())
    }
}
