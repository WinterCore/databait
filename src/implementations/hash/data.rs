use tokio::io::{self, AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::fs;

const KEY_MAX_LEN: usize = 128;

pub enum ReadResult {
    Success(String, String),
    Deleted(String),
}

pub async fn read_entry(
    reader: &mut io::BufReader<&mut fs::File>,
) -> io::Result<ReadResult> {
    let mut value_len_buf = [0u8; 8];
    reader.read_exact(&mut value_len_buf).await?;

    let value_len = u64::from_ne_bytes(value_len_buf);

    let is_deleted = reader.read_u8().await?;

    let mut key_buf = vec![0u8; KEY_MAX_LEN];
    reader.read_exact(&mut key_buf).await?;

    let key = String::from_utf8_lossy(&key_buf)
        .trim_end_matches(char::from(0))
        .to_owned();

    if is_deleted == 1 {
        return Ok(ReadResult::Deleted(key));
    }

    let mut value_buf = vec![0u8; value_len as usize];
    reader.read_exact(&mut value_buf).await?;

    let value = String::from_utf8_lossy(&value_buf).into_owned();

    Ok(ReadResult::Success(key, value))
}


pub async fn write_entry(
    reader: &mut io::BufReader<&mut fs::File>,
    key: &str,
    value: &str,
) -> io::Result<u64> {
    let key_len = key.len();

    if key_len > KEY_MAX_LEN as usize {
        return Err(io::Error::new(io::ErrorKind::Other, "Key too large"));
    }


    let pos = reader.stream_position().await?;
    let value_len = value.len() as u64;

    let mut buf: Vec<u8> = vec![];
    buf.extend(value_len.to_ne_bytes());
    buf.extend([0]);
    buf.extend(key_to_bin(key));
    buf.extend(value.as_bytes());

    reader.write(&buf).await?;

    Ok(pos)
}

pub async fn delete_entry(
    reader: &mut io::BufReader<&mut fs::File>,
    key: &str,
) -> io::Result<()> {
    let key_len = key.len();

    if key_len > KEY_MAX_LEN as usize {
        return Err(io::Error::new(io::ErrorKind::Other, "Key too large"));
    }

    let mut buf: Vec<u8> = vec![];
    buf.extend([0u8; 64 / 8]);
    buf.extend([1]);
    buf.extend(key_to_bin(key));

    reader.write(&buf).await?;

    Ok(())
}

fn key_to_bin(key: &str) -> Vec<u8> {
    let mut padded_key = key.as_bytes().to_vec();
    padded_key.extend(vec![0; KEY_MAX_LEN - key.len()]);

    padded_key
}
