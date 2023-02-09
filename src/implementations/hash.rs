use std::collections::HashMap;
use std::collections::VecDeque;

use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

use crate::implementations::ifc::Database;

pub struct Hash {
    segments: VecDeque<fs::File>,
    hash: HashMap<String, u64>,
    page_size: usize,
}

impl Hash {
    pub fn new(page_size: usize) -> Hash {
        Hash {
            segments: VecDeque::new(),
            hash: HashMap::new(),
            page_size,
        }
    }
}

#[async_trait]
impl Database for Hash {
    async fn init(&mut self) -> io::Result<()> {
        let path = Path::new("./.hash");
        fs::create_dir_all(path).await?;

        let mut folders_stream = fs::read_dir(path).await?;
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
            segment_files.push(format!("segment-{:0>6}", 0));
        }

        // TODO Improvement: Open these files in parallel
        for file in segment_files.iter() {
            let fd = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join(file))
                .await?;

            self.segments.push_front(fd);
        }

        /*
        let highest_segment: u32 = segment_files
            .last()
            .unwrap_or(&String::from("0"))
            .as_str()
            .parse()
            .unwrap();
        */

        
        Ok(())
    }

    async fn reset(&mut self) -> io::Result<()> {
        
        Ok(())
    }

    async fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
        let file = self.segments.front_mut().expect("Uninitialized database");

        file.write(format!("{}\u{038D}{}\n", key, value).as_bytes()).await?;

        Ok(())
    }

    async fn read(&mut self, key: &str) -> io::Result<Option<String>> {
        for file in self.segments.iter_mut() {
            file.rewind().await?;

            let mut reader = io::BufReader::new(file);

            let mut found: Option<String> = None;

            // Read the file line by line
            loop {
                let mut buffer = String::new();
                let bytes = reader.read_line(&mut buffer).await?;

                // Reached EOF
                if bytes == 0 {
                    break;
                }

                let curr_key: String = buffer.chars().take_while(|c| c != &'\u{038D}').collect();

                if curr_key == key {
                    let value: String = buffer.chars().skip(curr_key.len() + 1).collect();

                    found = Some(value);
                }
            }

            if let Some(value) = found {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }
}
