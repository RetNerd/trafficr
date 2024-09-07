use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::env;

struct Backend {
    addr: String,
    connections: usize,
}

struct BackendGroup {
    backends: Vec<Backend>,
    is_primary: bool,
}

struct Proxy {
    backend_groups: Vec<BackendGroup>,
    sessions: Arc<Mutex<HashMap<String, usize>>>,
}

impl Proxy {
    fn new(backend_groups: Vec<Vec<String>>, primary_group: usize) -> Self {
        let backend_groups = backend_groups.into_iter()
            .enumerate()
            .map(|(i, addrs)| BackendGroup {
                backends: addrs.into_iter()
                    .map(|addr| Backend { addr, connections: 0 })
                    .collect(),
                is_primary: i == primary_group,
            })
            .collect();
        Proxy {
            backend_groups,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_backends(&mut self, client_addr: &str) -> Vec<&mut Backend> {
        let mut sessions = self.sessions.lock().unwrap();
        let primary_group_index = self.backend_groups.iter().position(|g| g.is_primary).unwrap();
        
        sessions.entry(client_addr.to_string())
            .or_insert_with(|| {
                self.backend_groups[primary_group_index].backends.iter()
                    .enumerate()
                    .min_by_key(|(_, b)| b.connections)
                    .map(|(i, _)| i)
                    .unwrap_or(0)
            });

        self.backend_groups.iter_mut()
            .map(|group| {
                let backend_index = if group.is_primary {
                    *sessions.get(client_addr).unwrap()
                } else {
                    group.backends.iter()
                        .enumerate()
                        .min_by_key(|(_, b)| b.connections)
                        .map(|(i, _)| i)
                        .unwrap_or(0)
                };
                let backend = &mut group.backends[backend_index];
                backend.connections += 1;
                backend
            })
            .collect()
    }

    fn handle_client(&mut self, mut client: TcpStream) {
        let client_addr = client.peer_addr().unwrap().to_string();
        let backends = self.get_backends(&client_addr);
        
        let mut server_streams: Vec<TcpStream> = Vec::new();
        for backend in &backends {
            match TcpStream::connect(&backend.addr) {
                Ok(server) => server_streams.push(server),
                Err(e) => eprintln!("Failed to connect to backend {}: {}", backend.addr, e),
            }
        }

        if server_streams.is_empty() {
            eprintln!("Failed to connect to any backend");
            return;
        }

        let primary_server = server_streams.remove(0);
        let mut primary_server_clone = primary_server.try_clone().unwrap();

        // Forward client data to all backends
        let client_clone = client.try_clone().unwrap();
        thread::spawn(move || {
            let mut buf = [0; 4096];
            loop {
                match client_clone.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        for server in &mut server_streams {
                            if let Err(e) = server.write_all(&buf[..n]) {
                                eprintln!("Error writing to backend: {}", e);
                            }
                        }
                        if let Err(e) = primary_server_clone.write_all(&buf[..n]) {
                            eprintln!("Error writing to primary backend: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading from client: {}", e);
                        break;
                    }
                }
            }
        });

        // Forward primary backend responses to client
        std::io::copy(&mut primary_server, &mut client).unwrap();

        for backend in backends {
            backend.connections -= 1;
        }
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
            backend_groups: self.backend_groups.clone(),
            sessions: Arc::clone(&self.sessions),
        }
    }
}

fn main() {
    let backend_groups: Vec<Vec<String>> = env::var("BACKEND_GROUPS")
        .expect("BACKEND_GROUPS environment variable not set")
        .split(';')
        .map(|group| group.split(',').map(|s| s.trim().to_string()).collect())
        .collect();

    if backend_groups.is_empty() {
        eprintln!("No backend groups provided");
        std::process::exit(1);
    }

    let primary_group: usize = env::var("PRIMARY_GROUP")
        .expect("PRIMARY_GROUP environment variable not set")
        .parse()
        .expect("PRIMARY_GROUP must be a valid number");

    if primary_group >= backend_groups.len() {
        eprintln!("Invalid PRIMARY_GROUP index");
        std::process::exit(1);
    }

    println!("Backend groups: {:?}", backend_groups);
    println!("Primary group index: {}", primary_group);

    let mut proxy = Proxy::new(backend_groups, primary_group);
    proxy.run("127.0.0.1:8080");
}
