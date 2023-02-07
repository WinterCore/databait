use async_trait::async_trait;
use tokio::io;

#[async_trait]
pub trait Database {
    async fn init(&self) -> io::Result<()>;
    async fn write<K: ToString, V: ToString>(&self, key: &K, value: &V) -> ();
    async fn read(&self, key: &str) -> String;
}
