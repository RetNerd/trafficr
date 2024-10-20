mod proxy;
use proxy::proxy::Proxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = Proxy::new().await?;
    proxy.start().await?;
    Ok(())
}
