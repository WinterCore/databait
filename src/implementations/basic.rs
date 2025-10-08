use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use std::{io::SeekFrom, path::PathBuf};
use tokio::{fs, io::{self, BufReader}};
use crate::implementations::ifc::Database;

pub struct Basic {
    file: Option<fs::File>,
    path: PathBuf,
}

impl Basic {
    pub fn new(path: PathBuf) -> Basic {
        Basic {
            file: None,
            path,
        }
    }
}

/**
 * A very basic implementation of a key-value store using a single file.
 * Write ahead log style, with each line in the file representing a key-value pair.
 */
#[async_trait]
impl Database for Basic {
    async fn init(&mut self) -> io::Result<()> {
        fs::create_dir_all(&self.path).await?;

        let file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(self.path.join("data"))
            .await?;

        self.file = Some(file);

        Ok(())
    }

    async fn reset(&mut self) -> io::Result<()> {
        let file = self.file.as_mut().expect("Uninitialized database");
        file.set_len(0).await?;

        Ok(())
    }

    async fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
        let file = self.file.as_mut().expect("Uninitialized database");
        let key_len = key.len();
        let value_len = value.len();

        if key_len > u16::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Key length exceeds maximum of 65535 bytes"));
        }

        if value_len > u16::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Value length exceeds maximum of 65535 bytes"));
        }

        file.write("BAIT".as_bytes()).await?;

        file.write_u16_le(key_len as u16).await?;
        file.write_u16_le(value_len as u16).await?;

        file.write(key.as_bytes()).await?;
        file.write(value.as_bytes()).await?;

        file.write_u32_le((4 + 4 + key_len + value_len) as u32).await?;
        file.flush().await?;

        Ok(())
    }

    async fn read(&mut self, key: &str) -> io::Result<Option<String>> {
        let file = self.file.as_mut().expect("Uninitialized database");
        let mut reader = BufReader::new(file);
        let len = reader.seek(SeekFrom::End(0)).await?;
        let mut buffer = vec![0u8; u16::MAX as usize * 2 + 4 + 2 + 2 + 4]; // Max key + max value + header + key_len + value_len + entry_len
        let mut found: Option<String> = None;
        let mut pos = len;

        while pos > 0 {
            println!("pos: {}", pos);
            reader.seek(SeekFrom::Start(pos - 4)).await?;
            let entry_len = reader.read_u32_le().await?;

            pos -= entry_len as u64 + 4;
            
            reader.seek(SeekFrom::Start(pos)).await?;
            reader.read_exact(&mut buffer[..8]).await?;
            reader.seek(SeekFrom::Start(pos)).await?;
            
            for b in &buffer[..8] {
                print!("{:02x} ", b);
            }
            println!("");

            if &buffer[..4] != b"BAIT" {
                println!("Invalid entry header");
                break; // Invalid entry, stop reading
            }
            
            reader.read_exact(&mut buffer[..entry_len as usize]).await?;

            let key_len = u16::from_le_bytes([buffer[4], buffer[5]]) as usize;
            let value_len = u16::from_le_bytes([buffer[6], buffer[7]]) as usize;

            println!("{:?}: {:?} != {:?}", key, key.as_bytes(), &buffer[0..20]);
            if key.as_bytes() != &buffer[8..8 + key_len] {
                continue; // Not the key we're looking for
            }

            String::from_utf8(buffer[8 + key_len..8 + key_len + value_len].to_vec())
                .map(|s| found = Some(s))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            break;
        }

        Ok(found)
    }

    async fn delete(&mut self, key: &str) -> io::Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> io::Result<Basic> {
        let mut instance = Basic::new(PathBuf::from("./.basic-tests"));
        instance.init().await?;
        instance.reset().await?;

        Ok(instance)
    }

    #[tokio::test]
    async fn should_write_data() -> io::Result<()> {
        let mut db = setup().await?;
        db.write("john", "smith").await?;
        db.write("balls", "deep").await?;

        assert_eq!(db.read("john").await?, Some("smith".to_string()));

        Ok(())
    }
}
