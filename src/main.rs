use clap::Parser;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{TlsConnector, rustls};

struct ProbeContext {
    stream: TcpStream,
    banner: Vec<u8>,
}
fn has_220_greeting(ctx: &ProbeContext) -> bool {
    ctx.banner.starts_with(b"220 ")
}
// fn ftp_probe(ctx: &ProbeContext) -> bool {
//     ctx.banner.starts_with(b"220 ")
// }
fn smtp_probe(ctx: &ProbeContext) -> bool {
    ctx.banner.starts_with(b"220 ")
}
enum Service {
    Http(Option<HttpInfo>),
    Https(Option<HttpInfo>),
    Tls,
    Ssh,
    Ftp,
    Unknown,
}
enum PortStatus {
    Open,
    Closed,
    TimedOut,
}

struct HttpInfo {
    protocol: String,
    status_code: u16,
    reason: String,
    server: Option<String>,
}
impl HttpInfo {
    fn new(protocol: String, status_code: u16, reason: String, server: Option<String>) -> Self {
        HttpInfo {
            protocol,
            status_code,
            reason,
            server,
        }
    }
}
impl ProbeContext {
    async fn new(mut stream: TcpStream) -> Self {
        let banner = read_banner(&mut stream).await;

        ProbeContext { stream, banner }
    }
}
async fn read_banner(stream: &mut TcpStream) -> Vec<u8> {
    let mut buffer = [0u8; 1024];

    match timeout(Duration::from_millis(500), stream.read(&mut buffer)).await {
        Ok(Ok(size)) => buffer[..size].to_vec(),
        _ => Vec::new(),
    }
}
enum ScanError {
    Semaphore(tokio::sync::AcquireError),
    Task(tokio::task::JoinError),
}
struct ScanResult {
    port: u16,
    status: PortStatus,
    ip: Ipv4Addr,
    service: Service,
}

impl ScanResult {
    fn new(port: u16, status: PortStatus, ip: Ipv4Addr, service: Service) -> Self {
        ScanResult {
            port: port,
            status: status,
            ip: ip,
            service: service,
        }
    }
}
#[derive(Parser)]
struct Cli {
    ipv4: Ipv4Addr,
    #[arg(short, long)]
    start: u16,
    #[arg(short, long)]
    end: u16,
    #[arg(short, long)]
    concurrency: usize,
}

async fn tls_service_probe(ip: Ipv4Addr, port: u16, connector: &TlsConnector) -> Option<Service> {
    // Your implementation
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let st = match timeout(
        Duration::from_millis(500),
        TcpStream::connect(socket_address),
    )
    .await
    {
        Ok(Ok(stream)) => Some(stream),
        Ok(Err(_)) => None,
        Err(_) => None,
    };
    if let Some(st) = st {
        let tls_scan = tls_probe(st, ip, connector).await;
        match tls_scan {
            Some(stream) => match https_probe(stream).await {
                Some(f) => {
                    return Some(Service::Https(Some(f)));
                }
                None => {
                    return Some(Service::Tls);
                }
            },
            None => {
                // return (PortStatus::Closed,Service::Unknown);
                return None;
            }
        }
    }
    None
}

fn create_tls_connector() -> TlsConnector {
    let mut root_cert_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let cert_pem = std::fs::read("tls-test/ca.pem").expect("Failed to read test certificate");

    let mut reader = std::io::BufReader::new(&cert_pem[..]);

    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse test certificate");

    for cert in certs {
        root_cert_store
            .add(cert)
            .expect("Failed to add test certificate");
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    TlsConnector::from(Arc::new(config))
}
async fn read_ftp_response(stream: &mut TcpStream) -> Option<String> {
    let mut buffer = [0u8; 4096];
    let mut response = Vec::new();
    let mut response_code = None;
    let readd = timeout(Duration::from_millis(500), async {
        loop {
            let n = match stream.read(&mut buffer).await {
                Ok(n) => n,
                Err(_) => return None,
            };
            if n == 0 {
                return None;
            }
            response.extend_from_slice(&buffer[0..n]);
            if !response.windows(2).any(|w| w == b"\r\n") {
                continue;
            }
            if !response.ends_with(b"\r\n") {
                continue;
            }
            let converted = String::from_utf8_lossy(&response);
            let Some(first_line) = converted.lines().next() else {
                continue;
            };
            
            if first_line.len() < 4 {
                continue;
            }
            let code = first_line.get(0..3)?;
            if response_code.is_none() {
                response_code = Some(code.to_string());
            }
            let last_line = converted.lines().last()?;
            let code = response_code.as_ref()?;
            let complete = last_line.starts_with(&format!("{} ", code));
            if complete {
                return Some(converted.to_string());
            }
        }
    })
    .await;
    match readd {
        Ok(Some(f)) => {
            return Some(f);
        }
        Ok(None) => {
            return None;
        }
        Err(_) => {
            return None;
        }
    }
}
async fn detect_service(
    ip: Ipv4Addr,
    port: u16,
    stream: TcpStream,
    connector: &TlsConnector,
) -> Service {
    let mut ctx = ProbeContext::new(stream).await;
    match tls_service_probe(ip, port, connector).await {
        Some(f) => {
            return f;
        }
        None => {}
    }
    if ssh_probe(&ctx) {
        return Service::Ssh;
    }
    if has_220_greeting(&ctx) && ftp_probe(&mut ctx).await {
        return Service::Ftp;
    }

    match http_probe(&mut ctx.stream).await {
        Some(info) => return Service::Http(Some(info)),
        None => return Service::Unknown,
    }
}

async fn ftp_probe(ctx: &mut ProbeContext) -> bool {
    // 1. Send FEAT\r\n through ctx.stream
    // 2. Read the server's response
    // 3. Check whether it starts with the expected FTP response
    let mut buffer = [0u8; 4096];
    let req = b"FEAT\r\n";
    let f = ctx.stream.write_all(req).await;
    match f {
        Ok(_) => {
            let res = read_ftp_response(&mut ctx.stream).await;
            match res {
                Some(e) => {
                    let is_ftp = e.starts_with("211-") || e.starts_with("211 ");
                    return is_ftp;
                }
                None => {
                    return false;
                }
            }
        }
        Err(_) => {
            return false;
        }
    }
}

fn ssh_probe(ctx: &ProbeContext) -> bool {
    ctx.banner.starts_with(b"SSH-")
}
async fn tls_probe(
    stream: TcpStream,
    ip: Ipv4Addr,
    connector: &TlsConnector,
) -> Option<tokio_rustls::client::TlsStream<TcpStream>> {
    // let addr = SocketAddr::new(ip.into(), port);
    // let stream = match timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
    //     Ok(Ok(stream)) => stream,
    //     Ok(Err(_)) => return false,
    //     Err(_) => return false,
    // };
    let server_name = match ServerName::try_from(ip.to_string()) {
        Ok(name) => name,
        Err(_) => return None,
    };
    match timeout(
        Duration::from_secs(2),
        connector.connect(server_name, stream),
    )
    .await
    {
        Ok(e) => match e {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("TLS handshake failed : {}", e);
                None
            }
        },
        Err(_) => {
            return None;
        }
    }
}
#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let start_port = args.start;
    let end_port = args.end;
    let concurr = args.concurrency;
    if start_port > end_port {
        println!("The start_port should be less than end_port");
        return;
    }
    if concurr == 0 {
        println!("concurrency cannot be zero. Use 1 for restricted usage of system resources");
        return;
    }
    // for i in start_port..end_port {
    //     println!("{}",i);
    // }
    // let _port_to_be_scanned = 3000;
    let start = Instant::now();
    println!("{}", args.ipv4);
    let ip = args.ipv4;
    let connector = Arc::new(create_tls_connector());
    let ports = run_scan(ip, start_port, end_port, concurr, connector).await;
    for scan in ports {
        match scan {
            Ok(s) => match s.status {
                PortStatus::Open => match s.service {
                    Service::Https(e) => match e {
                        Some(e) => {
                            println!("Protocol : {}", e.protocol);
                            println!("Status : {}", e.status_code);
                            println!("reason : {}", e.reason);
                            match e.server {
                                Some(val) => {
                                    println!("Server : {}", val);
                                }
                                None => {}
                            }
                            println!("TLS OPEN ON PORT {}", s.port);
                        }
                        None => {
                            println!("No http info")
                        }
                    },
                    Service::Http(e) => match e {
                        Some(e) => {
                            println!("Protocol : {}", e.protocol);
                            println!("Status : {}", e.status_code);
                            println!("reason : {}", e.reason);
                            match e.server {
                                Some(val) => {
                                    println!("Server : {}", val);
                                }
                                None => {}
                            }
                        }
                        None => {
                            println!("No http info")
                        }
                    },
                    Service::Ssh => {
                        println!("SSH open");
                    }
                    Service::Tls => {
                        println!("TLS open detected on port {}", s.port);
                    }
                    Service::Ftp => {
                        println!("FTP detected on the port {}", s.port);
                    }
                    _ => {}
                },
                PortStatus::TimedOut => {
                    println!("[TimedOut] port {}:{}", s.ip, s.port);
                }
                PortStatus::Closed => {
                    continue;
                }
            },
            Err(s) => match s {
                ScanError::Semaphore(_) => {}
                ScanError::Task(_) => {}
            },
        }
    }
    let end = Instant::now();
    println!("The time is {:?}", end - start);
}

async fn run_scan(
    ip: Ipv4Addr,
    start_port: u16,
    end_port: u16,
    concurrency: usize,
    connector: Arc<TlsConnector>,
) -> Vec<Result<ScanResult, ScanError>> {
    let mut arr = Vec::new();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::new();
    for port_number in start_port..=end_port {
        let connector = Arc::clone(&connector);
        let semaphore = Arc::clone(&semaphore);
        let res = tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await;
            match permit {
                Ok(_) => {
                    let scan = port_scan(ip, port_number, &connector).await;
                    Ok(scan)
                }
                Err(e) => Err(e),
            }
        });
        handles.push((port_number, res));
        // match res {
        //     PortStatus::Closed => println!("The port {} is closed",port_number),
        //     PortStatus::Open => println!("The port {} is open",port_number),
        //     PortStatus::TimedOut => println!("The port {} timed out",port_number)
        // }
    }
    for (port, h) in handles {
        let res = h.await;

        match res {
            Ok(status) => match status {
                Ok((status, st)) => match status {
                    PortStatus::Open => {
                        arr.push(Ok(ScanResult::new(port, status, ip, st)));
                    }
                    PortStatus::Closed => arr.push(Ok(ScanResult {
                        port,
                        status,
                        ip,
                        service: st,
                    })),
                    PortStatus::TimedOut => {
                        arr.push(Ok(ScanResult::new(port, status, ip, st)));
                    }
                },
                Err(e) => {
                    arr.push(Err(ScanError::Semaphore(e)));
                }
            },
            Err(e) => {
                arr.push(Err(ScanError::Task(e)));
            }
        }
    }
    arr
}

async fn http_probe(stream: &mut TcpStream) -> Option<HttpInfo> {
    let mut buffer = [0u8; 4096];
    let request = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    let f = stream.write_all(request).await;
    match f {
        Ok(_) => {
            let res = timeout(Duration::from_millis(1000), stream.read(&mut buffer[..])).await;
            match res {
                Ok(Ok(n)) => {
                    let converted_buffer = String::from_utf8_lossy(&buffer[0..n]);
                    let first_line = converted_buffer.lines().next();
                    match first_line {
                        Some(line) => {
                            let mut iter = line.split_whitespace();
                            let protocol = if let Some(val) = iter.next() {
                                val
                            } else {
                                "Empty"
                            };
                            let status: u16 = if let Some(val) = iter.next() {
                                match val.parse::<u16>() {
                                    Ok(v) => v,
                                    Err(_) => 0,
                                }
                            } else {
                                0
                            };

                            let response = iter.collect::<Vec<&str>>().join(" ");
                            // println!(
                            //     "The line is {} {} {} {}",
                            //     line, protocol, status, response
                            // );
                            let mut temp = None;
                            for line in converted_buffer.lines() {
                                if line.starts_with("Server:") {
                                    // println!("{}",&line[7..]);
                                    temp = Some(line[7..].to_string());
                                    // println!("{}",line.split_once(':'))
                                }
                            }
                            let hi = HttpInfo::new(protocol.to_string(), status, response, temp);
                            if protocol.starts_with("HTTP/1.0") || protocol.starts_with("HTTP/1.1")
                            {
                                return Some(hi);
                            } else {
                                return None;
                            }
                        }
                        None => {
                            println!("No line to print");
                        }
                    }
                    None
                }
                Ok(Err(_)) => None,
                Err(_) => None,
            }
        }
        Err(_) => None,
    }
}
async fn https_probe(mut stream: tokio_rustls::client::TlsStream<TcpStream>) -> Option<HttpInfo> {
    let mut buffer = [0u8; 4096];
    let request = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    let f = stream.write_all(request).await;
    match f {
        Ok(_) => {
            let res = timeout(Duration::from_millis(1000), stream.read(&mut buffer[..])).await;
            match res {
                Ok(Ok(n)) => {
                    let converted_buffer = String::from_utf8_lossy(&buffer[0..n]);
                    let first_line = converted_buffer.lines().next();
                    match first_line {
                        Some(line) => {
                            let mut iter = line.split_whitespace();
                            let protocol = if let Some(val) = iter.next() {
                                val
                            } else {
                                "Empty"
                            };
                            let status: u16 = if let Some(val) = iter.next() {
                                match val.parse::<u16>() {
                                    Ok(v) => v,
                                    Err(_) => 0,
                                }
                            } else {
                                0
                            };

                            let response = iter.collect::<Vec<&str>>().join(" ");
                            // println!(
                            //     "The line is {} {} {} {}",
                            //     line, protocol, status, response
                            // );
                            let mut temp = None;
                            for line in converted_buffer.lines() {
                                if line.starts_with("Server:") {
                                    // println!("{}",&line[7..]);
                                    temp = Some(line[7..].to_string());
                                    // println!("{}",line.split_once(':'))
                                }
                            }
                            let hi = HttpInfo::new(protocol.to_string(), status, response, temp);
                            if protocol.starts_with("HTTP/1.0") || protocol.starts_with("HTTP/1.1")
                            {
                                return Some(hi);
                            } else {
                                return None;
                            }
                        }
                        None => {
                            println!("No line to print");
                        }
                    }
                    None
                }
                Ok(Err(_)) => None,
                Err(_) => None,
            }
        }
        Err(_) => None,
    }
}

async fn port_scan(ip: Ipv4Addr, port: u16, connector: &TlsConnector) -> (PortStatus, Service) {
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let conn = TcpStream::connect(socket_address);
    let timer = timeout(Duration::from_millis(500), conn).await;
    match timer {
        Ok(Ok(mut stream)) => {
            let service = detect_service(ip, port, stream, connector).await;

            (PortStatus::Open, service)
        }
        Ok(Err(_)) => {
            return (PortStatus::Closed, Service::Unknown);
        }
        Err(_) => {
            // println!("The port is not open, {}",val);
            return (PortStatus::TimedOut, Service::Unknown);
        }
    }
}
