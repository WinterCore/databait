mod implementations;

use tokio::io;
use implementations::basic::Basic;
use implementations::ifc::Database;
use implementations::hash::Hash;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut db = Hash::new(1024 * 1024);

    db.init().await?;
    db.reset().await?;
    db.write(&"1", &"onev2").await?;
    db.write(&"2", &"twov2").await?;
    for i in 0..=10000 {
        db.write(&"5", &format!("{}f ahsdklghasdlkghkl", i)).await?;
    }
    let value = db.read(&"5").await?;

    match value {
        Some(v) => println!("Found {}", v),
        None    => println!("Not found"),
    }

    Ok(())
}
