mod implementations;

use tokio::io;
use implementations::basic::Basic;
use implementations::ifc::Database;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut basic_db = Basic::new();

    basic_db.init().await?;
    basic_db.reset().await?;
    /*
    basic.write(&"1", &"onev2").await?;
    basic.write(&"2", &"twov2").await?;
    */
    basic_db.write(&"5", &"hasan").await?;
    let value = basic_db.read(&"5").await?;

    match value {
        Some(v) => println!("Found {}", v),
        None    => println!("Not found"),
    }

    Ok(())
}
