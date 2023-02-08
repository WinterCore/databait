use std::collections::HashMap;

use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tokio::io;

use crate::implementations::ifc::Database;

pub struct Hash {
    segments: Vec<fs::File>,
    hash: HashMap<String, u64>,
    page_size: usize,
}

impl Hash {
    pub fn new(page_size: usize) -> Hash {
        Hash {
            segments: Vec::new(),
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

        Ok(())
    }
}
