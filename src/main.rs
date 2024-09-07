use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

struct Backend {
    addr: String,
    connections: usize,
}

struct Proxy {
    backends: Vec<Backend>,
    sessions: Arc<Mutex<HashMap<String, usize>>>,
}

impl Proxy {
    fn new(backend_addrs: Vec<String>) -> Self {
        let backends = backend_addrs.into_iter()
            .map(|addr| Backend { addr, connections: 0 })
            .collect();
        Proxy {
            backends,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_backend(&mut self, client_addr: &str) -> &mut Backend {
        let mut sessions = self.sessions.lock().unwrap();
        let backend_index = sessions.entry(client_addr.to_string())
            .or_insert_with(|| {
                self.backends.iter()
                    .enumerate()
                    .min_by_key(|(_, b)| b.connections)
                    .map(|(i, _)| i)
                    .unwrap_or(0)
            });
        let backend = &mut self.backends[*backend_index];
        backend.connections += 1;
        backend
    }

    fn handle_client(&mut self, mut client: TcpStream) {
        let client_addr = client.peer_addr().unwrap().to_string();
        let backend = self.get_backend(&client_addr);
        
        match TcpStream::connect(&backend.addr) {
            Ok(mut server) => {
                let mut client_clone = client.try_clone().unwrap();
                let mut server_clone = server.try_clone().unwrap();

                thread::spawn(move || {
                    std::io::copy(&mut client_clone, &mut server_clone).unwrap();
                });

                std::io::copy(&mut server, &mut client).unwrap();
            }
            Err(e) => eprintln!("Failed to connect to backend: {}", e),
        }

        backend.connections -= 1;
    }

    fn run(&mut self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();
        println!("Proxy listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut proxy = self.clone();
                    thread::spawn(move || {
                        proxy.handle_client(stream);
                    });
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

impl Clone for Proxy {
    fn clone(&self) -> Self {
        Proxy {
            backends: self.backends.clone(),
            sessions: Arc::clone(&self.sessions),
        }
    }
}

fn main() {
    let backend_addrs = vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
        "127.0.0.1:8083".to_string(),
    ];

    let mut proxy = Proxy::new(backend_addrs);
    proxy.run("127.0.0.1:8080");
}
