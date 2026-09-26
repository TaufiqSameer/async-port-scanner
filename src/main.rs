use clap::Parser;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
enum PortStatus {
    Open,
    Closed,
    TimedOut,
}
enum ScanError {
    Semaphore(tokio::sync::AcquireError),
    Task(tokio::task::JoinError),
}
struct ScanResult {
    port: u16,
    status: PortStatus,
    ip: Ipv4Addr,
    banner: Option<String>,
}

impl ScanResult {
    fn new(port: u16, status: PortStatus, ip: Ipv4Addr, banner: Option<String>) -> Self {
        ScanResult {
            port: port,
            status: status,
            ip: ip,
            banner: banner,
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
    let ports = run_scan(ip, start_port, end_port, concurr).await;
    for scan in ports {
        match scan {
            Ok(s) => match s.status {
                PortStatus::Open => match s.banner {
                    Some(st) => {
                        println!("Banner : {}", st);
                        println!("[Open] {}:{}", s.ip, s.port);
                    }
                    None => {
                        println!("[Open] {}:{}", s.ip, s.port);
                    }
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
) -> Vec<Result<ScanResult, ScanError>> {
    let mut arr = Vec::new();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::new();
    for port_number in start_port..=end_port {
        let semaphore = Arc::clone(&semaphore);
        let res = tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await;
            match permit {
                Ok(_) => {
                    let scan = port_scan(ip, port_number).await;
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
                        banner: st,
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

async fn port_scan(ip: Ipv4Addr, port: u16) -> (PortStatus, Option<String>) {
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let conn = TcpStream::connect(socket_address);
    let timer = timeout(Duration::from_millis(500), conn).await;
    match timer {
        Ok(Ok(mut stream)) => {
            let mut buffer = [0u8; 4096];
            let request = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
            let f = stream.write_all(request).await;
            match f {
                Ok(_) => {
                    let res =
                        timeout(Duration::from_millis(1000), stream.read(&mut buffer[..])).await;
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
                                    println!(
                                        "The line is {} {} {} {}",
                                        line, protocol, status, response
                                    );
                                }
                                None => {
                                    println!("No line to print");
                                }
                            }
                            return (PortStatus::Open, Some(converted_buffer.to_string()));
                        }
                        Ok(Err(_)) => {
                            return (PortStatus::Open, None);
                        }
                        Err(_) => {
                            return (PortStatus::Open, None);
                        }
                    }
                }
                Err(_) => {
                    return (PortStatus::Open, None);
                }
            }
        }
        Ok(Err(_)) => {
            return (PortStatus::Closed, None);
        }
        Err(_) => {
            // println!("The port is not open, {}",val);
            return (PortStatus::TimedOut, None);
        }
    }
}
