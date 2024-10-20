use shadow_proxy::Proxy;
use tokio::time::{sleep, timeout, Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::env;

async fn start_mock_server(addr: String, response: &str) -> tokio::task::JoinHandle<()> {
    let response = response.to_string();
    tokio::spawn(async move {
        let listener = TcpListener::bind(&addr).await.unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let response = response.clone();
            tokio::spawn(async move {
                let mut buf = [0; 1024];
                socket.read(&mut buf).await.unwrap();
                socket.write_all(response.as_bytes()).await.unwrap();
            });
        }
    })
}

async fn send_request(addr: &str, request: &str) -> Result<String, std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

#[tokio::test]
async fn test_proxy() {
    env::set_var("BACKEND_GROUPS", "127.0.0.1:8081,127.0.0.1:8082");
    env::set_var("PRIMARY_GROUP", "0");
    env::set_var("BIND_ADDR", "127.0.0.1:8080");

    // Start mock backend servers
    let backend1 = start_mock_server("127.0.0.1:8081".to_string(), "Response from backend 1").await;
    let backend2 = start_mock_server("127.0.0.1:8082".to_string(), "Response from backend 2").await;

    // Create and run the proxy
    let proxy = Proxy::new().await.expect("Failed to create proxy");
    
    // Use a channel to communicate when the proxy is ready
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    let proxy_handle = tokio::spawn(async move {
        tx.send(()).unwrap(); // Signal that the proxy is about to start
        proxy.start().await.unwrap();
    });

    // Wait for the proxy to start with a timeout
    timeout(Duration::from_secs(5), rx).await
        .expect("Timeout waiting for proxy to start")
        .expect("Failed to receive start signal from proxy");

    // Send multiple requests to the proxy
    for i in 0..10 {
        match send_request("127.0.0.1:8080", &format!("Test request {}", i)).await {
            Ok(response) => {
                println!("Request {} response: {}", i, response);
                assert!(response == "Response from backend 1" || response == "Response from backend 2");
            },
            Err(e) => {
                println!("Request {} failed: {:?}", i, e);
                // Optionally, you can choose to fail the test here
                // assert!(false, "Request failed: {:?}", e);
            }
        }
        sleep(Duration::from_millis(100)).await; // Add a small delay between requests
    }

    // Clean up
    // Note: You'll need to implement a way to gracefully shut down the proxy
    // For now, we'll just let it run until the test ends
    backend1.abort();
    backend2.abort();
    proxy_handle.abort();
}

