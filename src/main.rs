mod proxy;

pub use proxy::Proxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = Proxy::new()?;
    proxy.run("127.0.0.1:8080").await
}
