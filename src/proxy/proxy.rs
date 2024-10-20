use futures::future;
use std::borrow::BorrowMut;
use std::env;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use stretto::AsyncCache;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, Interest};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, UdpSocket};
use tokio::spawn;
use tokio::sync::{mpsc, Mutex, RwLock};
#[derive(Clone)]
pub struct Backend {
    addr: String,
    connections: Arc<AtomicUsize>,
}

enum SocketType {
    Tcp(TcpSocket),
    Udp(UdpSocket),
}

struct Packet {
    data: Vec<u8>,
    addr: SocketAddr,
}

pub struct Proxy {
    backend_groups: Vec<Vec<Backend>>,
    primary_group: usize,
    bind_addr: String,
    socket: SocketType,
    socket_type: String,
}

impl Proxy {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        dotenv::dotenv().ok();

        let backend_groups_str = env::var("BACKEND_GROUPS").expect("BACKEND_GROUPS must be set");
        let primary_group: usize = env::var("PRIMARY_GROUP")
            .expect("PRIMARY_GROUP must be set")
            .parse()?;
        let bind_addr = env::var("BIND_ADDR").expect("BIND_ADDR must be set");
        let backend_groups: Vec<Vec<Backend>> = backend_groups_str
            .split(';')
            .map(|group| {
                group
                    .split(',')
                    .map(|addr| Backend {
                        addr: addr.trim().to_string(),
                        connections: Arc::new(AtomicUsize::new(0)),
                    })
                    .collect()
            })
            .collect();
        let socket_type = env::var("SOCKET_TYPE").unwrap_or("TCP".to_string());
        let socket = match socket_type.as_str() {
            "TCP" => SocketType::Tcp(TcpSocket::new_v4().expect("Failed to bind TCP socket")),
            "UDP" => SocketType::Udp(UdpSocket::bind(&bind_addr).await?),
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid socket type",
                )))
            }
        };

        Ok(Proxy {
            backend_groups,
            primary_group,
            bind_addr,
            socket,
            socket_type,
        })
    }

    async fn start_tcp_backend_worker(
        self: Arc<Self>,
        client_address: SocketAddr,
        inbound_receiver: Arc<Mutex<mpsc::Receiver<Packet>>>,
        outbound_writer: Arc<RwLock<OwnedWriteHalf>>,
    ) -> Result<(), std::io::Error> {
        let streams_res = future::join_all(self.backend_groups.iter().map(|backends| {
            let backend =
                Self::select_least_connected_backend(backends).expect("No backend available");
            let socket: TcpSocket =
                TcpSocket::new_v4().expect("Failed to bind TCP socket for client");
            let addrstr = backend.addr.as_str();
            let stream = socket.connect(
                backend
                    .addr
                    .to_socket_addrs()
                    .expect(&format!("Failed to lookup backend address {}", addrstr))
                    .next()
                    .expect("DNS returned no addresses"),
            );
            stream
        }))
        .await;
        let streams: Vec<(OwnedReadHalf, OwnedWriteHalf)> = streams_res
            .into_iter()
            .map(|stream| {
                let s = stream.expect("Failed to connect to backend");
                s.into_split()
            })
            .collect();
        let mut readers: Vec<OwnedReadHalf> = Vec::new();
        let mut writers: Vec<OwnedWriteHalf> = Vec::new();
        streams.into_iter().for_each(|stream| {
            readers.push(stream.0);
            writers.push(stream.1);
        });

        spawn(async move {
            let mut inbound_receiver = inbound_receiver.lock().await;
            let writers = &mut writers;
            while let Some(packet) = inbound_receiver.recv().await {
                future::join_all(writers.into_iter().map(|stream| {
                    println!(
                        "Writing to stream {} data {:?}",
                        stream.peer_addr().unwrap().to_string(),
                        packet.data
                    );
                    stream.write_all(&packet.data)
                }))
                .await;
                println!("Done writing to all backends");
            }
        });

        readers.into_iter().for_each(|stream| {
            let primary_group = self.backend_groups[self.primary_group].clone();
            let local_addr = stream.peer_addr().unwrap();
            let is_primary = primary_group
                .iter()
                .any(|backend| backend.addr.to_socket_addrs().unwrap().next().unwrap() == local_addr);
            let outbound_writer = outbound_writer.clone();
            spawn(async move {
                loop {
                    let mut buf: Vec<u8> = Vec::new();
                    stream.readable().await.expect("Failed to check if reader is readable");
                    match stream.try_read_buf(&mut buf) {
                        Ok(0) => {
                            continue;
                        },
                        Ok(bytes_read) => println!(
                            "Read from out stream {:?} bytes read: {}",
                            buf, bytes_read
                        ),
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            continue;
                        },
                        Err(e) => {
                            println!("Error reading from out stream: {}", e);
                            continue;
                        }
                    };
                    
                    let packet = Packet {
                        data: buf,
                        addr: client_address,
                    };
                    if is_primary {
                        println!("Writing to outbound");
                        let mut d = packet.data.clone();
                        let v = d.as_mut_slice();
                        outbound_writer
                            .write()
                            .await
                            .write_all(v)
                            .await
                            .expect("Failed to write to outbound");
                    }
                }
            });
        });
        Ok(())
    }

    async fn start_udp_backend_worker(
        self,
        client_address: SocketAddr,
        inbound_receiver: Arc<Mutex<mpsc::Receiver<Packet>>>,
        outbound_sender: mpsc::Sender<Packet>,
    ) -> Result<(), std::io::Error> {
        let streams_res = future::join_all(self.backend_groups.iter().map(|backends| {
            let backend =
                Self::select_least_connected_backend(backends).expect("No backend available");
            let socket = UdpSocket::bind(
                SocketAddr::from_str(&backend.addr).expect("Failed to parse backend address"),
            );
            socket
        }))
        .await;
        let streams: Vec<Arc<UdpSocket>> = streams_res
            .into_iter()
            .map(|stream| {
                let s = stream.expect("Failed to connect to backend");
                Arc::new(s)
            })
            .collect();
        let writers = streams.clone();
        spawn(async move {
            let mut inbound_receiver = inbound_receiver.lock().await;
            future::join_all(writers.iter().map(|sock| sock.connect(client_address))).await;
            while let Some(packet) = inbound_receiver.recv().await {
                future::join_all(
                    writers
                        .iter()
                        .map(|stream| stream.send(packet.data.as_slice())),
                )
                .await;
            }
        });

        streams.into_iter().for_each(|stream| {
            let sender = outbound_sender.clone();
            let primary_group = self.backend_groups[self.primary_group].clone();
            let local_addr = stream.peer_addr().unwrap().to_string();
            let is_primary = primary_group
                .iter()
                .any(|backend| backend.addr == local_addr);
            spawn(async move {
                loop {
                    let mut buf: [u8; 2048] = [0; 2048];
                    stream
                        .recv(&mut buf)
                        .await
                        .expect("Failed to read from backend");
                    let packet = Packet {
                        data: buf.to_vec(),
                        addr: client_address,
                    };
                    if is_primary {
                        sender
                            .send(packet)
                            .await
                            .expect("Failed to send packet to outbound");
                    }
                }
            });
        });
        Ok(())
    }
    pub async fn start(self) -> Result<(), std::io::Error> {
        let cache: AsyncCache<String, mpsc::Sender<Packet>> =
            AsyncCache::new(1000000, 1000000, spawn).expect("Failed to create cache");
        let proxy = Arc::new(self);
        let _udp_proxy_handle = proxy.clone();
        let addr = proxy.bind_addr.clone();
        println!("Starting {}", addr.to_string());
        match proxy.socket_type.as_str() {
            "TCP" => {
                let socket = TcpSocket::new_v4().expect("Failed to create TCP socket");
                let _ =
                    socket.bind(SocketAddr::from_str(&addr).expect("Failed to parse bind address"));
                let handle = spawn(async move {
                    let listener = socket.listen(5).expect("Failed to listen on TCP socket");
                    loop {
                        println!("Accepting TCP connection");
                        let (stream, addr) = listener
                            .accept()
                            .await
                            .expect("Failed to accept TCP connection");
                        let (reader, writer) = stream.into_split();
                        let cache = cache.clone();
                        let proxy = proxy.clone();
                        let writer = Arc::new(RwLock::new(writer));

                        spawn(async move {
                            loop {
                                let sender = cache.get(&addr.to_string()).await;
                                let mut data: Vec<u8> = Vec::new();
                                reader.readable().await.expect("Failed to check if reader is readable");
                                match reader.try_read_buf(&mut data) {
                                    Ok(0) => {
                                        continue;
                                    },
                                    Ok(bytes_read) => println!(
                                        "Read from in stream {:?} bytes read: {}",
                                        data, bytes_read
                                    ),
                                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                        continue;
                                    },
                                    Err(e) => {
                                        println!("Error reading from in stream: {}", e);
                                        continue;
                                    }
                                };
                                let packet = Packet { addr, data };

                                match sender {
                                    Some(sender) => {
                                        println!("Sending to worker");
                                        sender
                                            .value()
                                            .send(packet)
                                            .await
                                            .expect("Failed to send stream to worker");
                                    }
                                    None => {
                                        println!("No sender found for {}", addr.to_string());
                                        let tcp_proxy_handle = proxy.clone();
                                        let (inbound_sender, inbound_receiver) =
                                            mpsc::channel::<Packet>(100);
                                        let recv = Arc::new(Mutex::new(inbound_receiver));
                                        let sender = inbound_sender.clone();
                                        cache.insert(addr.to_string(), inbound_sender, 1).await;
                                        let writer = writer.clone();
                                        let worker = tcp_proxy_handle
                                            .start_tcp_backend_worker(addr, recv, writer);
                                        spawn(worker);
                                        sender
                                            .send(packet)
                                            .await
                                            .expect("Failed to send packet to worker");
                                    }
                                }
                            }
                        });
                    };
                });
                handle.await.expect("Failed to start TCP worker");
            }
            "UDP" => {
                todo!()
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid socket type",
                ))
            }
        };
        Ok(())
    }

    fn select_least_connected_backend(backends: &[Backend]) -> Option<&Backend> {
        backends.iter().min_by_key(|backend| {
            backend
                .connections
                .load(std::sync::atomic::Ordering::Relaxed)
        })
    }
}
