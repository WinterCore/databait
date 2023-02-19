use async_trait::async_trait;
use tokio::io;

#[async_trait]
pub trait Database {
    async fn reset(&mut self) -> io::Result<()>;
    async fn init(&mut self) -> io::Result<()>;
    async fn write(&mut self, key: &str, value: &str) -> io::Result<()>;
    async fn read(&mut self, key: &str) -> io::Result<Option<String>>;
    async fn delete(&mut self, key: &str) -> io::Result<bool>;
}
