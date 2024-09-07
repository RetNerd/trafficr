use shadow_proxy::Proxy;
use std::env;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

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
async fn test_proxy_multiple_groups() {
    // Set up environment variables for multiple groups
    env::set_var("BACKEND_GROUPS", "127.0.0.1:7081,127.0.0.1:7082;127.0.0.1:7083,127.0.0.1:7084");
    env::set_var("PRIMARY_GROUP", "0");
    env::set_var("BIND_ADDR", "127.0.0.1:7080");

    // Start mock backend servers
    let backend1 = start_mock_server("127.0.0.1:7081".to_string(), "Response from group 1, backend 1").await;
    let backend2 = start_mock_server("127.0.0.1:7082".to_string(), "Response from group 1, backend 2").await;
    let backend3 = start_mock_server("127.0.0.1:7083".to_string(), "Response from group 2, backend 1").await;
    let backend4 = start_mock_server("127.0.0.1:7084".to_string(), "Response from group 2, backend 2").await;

    // Create and run the proxy
    let proxy = Proxy::new().expect("Failed to create proxy");
    
    // Use a channel to communicate when the proxy is ready
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    let proxy_handle = tokio::spawn(async move {
        tx.send(()).unwrap(); // Signal that the proxy is about to start
        proxy.run().await.unwrap();
    });

    // Wait for the proxy to start with a timeout
    timeout(Duration::from_secs(5), rx).await
        .expect("Timeout waiting for proxy to start")
        .expect("Failed to receive start signal from proxy");

    // Send multiple requests to the proxy
    for i in 0..10 {
        match send_request("127.0.0.1:7080", &format!("Test request {}", i)).await {
            Ok(response) => {
                println!("Request {} response: {}", i, response);
                assert!(response.starts_with("Response from group 1"),
                        "Unexpected response: {}", response);
            },
            Err(e) => {
                panic!("Request {} failed: {:?}", i, e);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Clean up
    proxy_handle.abort();
    backend1.abort();
    backend2.abort();
    backend3.abort();
    backend4.abort();
}