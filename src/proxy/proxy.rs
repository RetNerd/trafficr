use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::env;

#[derive(Clone)]
pub struct Backend {
    addr: String,
    connections: Arc<AtomicUsize>,
}

pub struct Proxy {
    backend_groups: Vec<Vec<Backend>>,
    primary_group: usize,
}

impl Proxy {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        dotenv::dotenv().ok();

        let backend_groups_str = env::var("BACKEND_GROUPS")
            .expect("BACKEND_GROUPS must be set");
        let primary_group: usize = env::var("PRIMARY_GROUP")
            .expect("PRIMARY_GROUP must be set")
            .parse()?;

        let backend_groups: Vec<Vec<Backend>> = backend_groups_str
            .split(';')
            .map(|group| {
                group.split(',')
                    .map(|addr| Backend {
                        addr: addr.trim().to_string(),
                        connections: Arc::new(AtomicUsize::new(0)),
                    })
                    .collect()
            })
            .collect();

        Ok(Proxy {
            backend_groups,
            primary_group,
        })
    }

    pub async fn run(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("Proxy listening on {}", addr);

        loop {
            let (mut socket, _) = listener.accept().await?;
            let backend_groups = self.backend_groups.clone();
            let primary_group = self.primary_group;

            tokio::spawn(async move {
                let mut buf = [0; 1024];

                match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => return,
                    Ok(n) => {
                        let group = &backend_groups[primary_group];
                        let backend = group
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, b)| b.connections.load(Ordering::Relaxed))
                            .map(|(i, _)| i)
                            .unwrap_or(0);

                        let backend = &group[backend];
                        backend.connections.fetch_add(1, Ordering::SeqCst);

                        if let Ok(mut stream) = TcpStream::connect(&backend.addr).await {
                            if stream.write_all(&buf[..n]).await.is_ok() {
                                let mut response = Vec::new();
                                if stream.read_to_end(&mut response).await.is_ok() {
                                    let _ = socket.write_all(&response).await;
                                }
                            }
                        }

                        backend.connections.fetch_sub(1, Ordering::SeqCst);
                    }
                    Err(_) => return,
                }
            });
        }
    }
}