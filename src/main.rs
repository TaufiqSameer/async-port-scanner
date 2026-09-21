use clap::Parser;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
enum PortStatus {
    Open,
    Closed,
    TimedOut,
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
    let semaphore = Arc::new(Semaphore::new(concurr));
    let mut handles = Vec::new();
    for port_number in start_port..=end_port {
        println!("Scanning port {}", port_number);
        let semaphore = Arc::clone(&semaphore);
        let res = tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await;
            match permit {
                Ok(_) => {
                    let scan = port_scan(ip, port_number).await;
                    Ok(scan)
                }
                Err(e) => {
                    println!("acquire error {}", e);
                    Err(e)
                }
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
                Ok(status) => match status {
                    PortStatus::Open => {
                        println!("{} port is open", port);
                    }
                    PortStatus::Closed => {
                        println!("The port is closed {}", port);
                    }
                    PortStatus::TimedOut => {
                        println!("The port is timedout {}", port);
                    }
                },
                Err(e) => {
                    println!("Acuisiotn err {}", e);
                }
            },
            Err(e) => {
                println!("Error {}", e);
            }
        }
    }
    let end = Instant::now();
    println!("The time is {:?}", end - start);
}

async fn port_scan(ip: Ipv4Addr, port: u16) -> PortStatus {
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let conn = TcpStream::connect(socket_address);
    let timer = timeout(Duration::from_millis(500), conn).await;
    match timer {
        Ok(Ok(_)) => {
            // println!("{:?} is open",val);
            return PortStatus::Open;
        }
        Ok(Err(_)) => {
            return PortStatus::Closed;
        }
        Err(_) => {
            // println!("The port is not open, {}",val);
            return PortStatus::TimedOut;
        }
    }
}
