#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    kvstore_rs::serve().await
}
