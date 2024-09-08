use http_body_util::{BodyExt, Empty, Full};
use hyper::http::Result;
use shadow_proxy::Proxy;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;
use tokio::time::timeout;

async fn primary_handler(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>> {
    println!("Received request from client prim {:?}", req.uri());
    Response::builder().status(StatusCode::OK).body(Full::new(Bytes::from("I'm primary")))
}

async fn secondary_handler(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>> {

    println!("Received request from client sec {:?}", req.uri());
    Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Full::new(Bytes::new()))
}

async fn test_http_proxy_groups() {
    // Start backend servers
    let primary_addr = SocketAddr::from(([127, 0, 0, 1], 6081));
    let secondary_addr = SocketAddr::from(([127, 0, 0, 1], 6082));
    let primary_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(primary_addr).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
        if let Err(err) = http1::Builder::new()
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(stream), service_fn(primary_handler))
            .await
        {
            println!("Error serving connection: {:?}", err);
        }
    }
    });
    let secondary_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(secondary_addr).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
        if let Err(err) = http1::Builder::new()
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(stream), service_fn(secondary_handler))
            .await
        {
            println!("Error serving connection: {:?}", err);
        }
    }
    });

    // Configure and start proxy
    std::env::set_var("BACKEND_GROUPS", "127.0.0.1:6081;127.0.0.1:6082");
    std::env::set_var("PRIMARY_GROUP", "0");
    std::env::set_var("BIND_ADDR", "127.0.0.1:6080");
    let proxy = Proxy::new().expect("Failed to create proxy");
    let proxy_handle = tokio::spawn(async move { proxy.run().await.unwrap() });

    // Wait for servers to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Send request to proxy
    let addr = "127.0.0.1:6080";
    println!("Connecting to proxy on {:?}", addr);
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    println!("Handshaking with proxy");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });
    println!("Sending request to proxy");
    let req = Request::builder()
        .uri("/")
        .body(Empty::<Bytes>::new())
        .unwrap();

    match timeout(Duration::from_secs(5), sender.send_request(req)).await {
        Ok(Ok(resp)) => {
            println!("Received response from proxy");
            assert_eq!(resp.status(), StatusCode::OK);
            let body = resp.collect().await.unwrap().to_bytes();
            assert_eq!(body, Bytes::from("I'm primary"));
        },
        Ok(Err(e)) => panic!("Error sending request: {:?}", e),
        Err(_) => panic!("Request timed out after 5 seconds"),
    }

    primary_handle.abort();
    secondary_handle.abort();
    proxy_handle.abort();
}