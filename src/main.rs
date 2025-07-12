// /src/main.rs

use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // call into your lib’s serve() function
    kvstore_rs::serve().await
}
