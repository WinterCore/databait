mod implementations;

use std::path::PathBuf;
use tokio::io;
use implementations::Basic;
use implementations::ifc::Database;
use implementations::Hash;
use std::str;

#[tokio::main]
async fn main() -> io::Result<()> {
    let path = PathBuf::from("./.hash");
    let mut db = Hash::new(path, 1024 * 1024);

    db.init().await?;
    /*
    db.reset().await?;
    db.write(&"8", &"1").await?;
    db.write(&"2", &"twov2").await?;
    db.write(&"5", &format!("{}f value", 15)).await?;

    for i in 0..=9984 {
        db.write(&"5", &format!("{}f value", i)).await?;
    }
    */

    let value = db.read(&"5").await?;

    match value {
        Some(v) => println!("Found {}", v),
        None    => println!("Not found"),
    }

    Ok(())
}
