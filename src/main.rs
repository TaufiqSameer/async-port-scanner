use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::net::{TcpStream};
use tokio::time::{timeout};
use tokio::sync::{Semaphore};
use std::sync::{Arc};

enum PortStatus{
    Open,
    Closed,
    TimedOut,
    SemaphoretimedOut
}

#[tokio::main]
async fn main() {
    let start_port = 1;
    let end_port = 3001;
    // for i in start_port..end_port {
    //     println!("{}",i);
    // }
    let _port_to_be_scanned = 3000;
    let start = Instant::now();  
    let ip = Ipv4Addr::new(127, 0, 0, 1);
    let semaphore = Arc::new(Semaphore::new(50));
    let mut handles = Vec::new();
    for port_number in start_port..=end_port {
        println!("Scanning port {}", port_number);
        let semaphore = Arc::clone(&semaphore);
        let res = tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await;
            match permit {
                Ok(_) => {
                    let scan = port_scan(ip, port_number).await;
                    scan
                }
                Err(_) => {
                    PortStatus::SemaphoretimedOut
                }
            }
     
        });
        handles.push((port_number,res));
        // match res {
        //     PortStatus::Closed => println!("The port {} is closed",port_number),
        //     PortStatus::Open => println!("The port {} is open",port_number),
        //     PortStatus::TimedOut => println!("The port {} timed out",port_number)
        // }
    }
    for (port,h) in handles{
        let res = h.await;

        match res {
            Ok(status ) => {
                match status {
                    PortStatus::Open => {
                        println!("The port is open {}",port);
                    }
                    PortStatus::Closed => {
                        println!("The port is closed {}",port);
                    }
                    PortStatus::TimedOut => {
                        println!("The port is timedout {}",port);
                    }
                    PortStatus::SemaphoretimedOut => {
                        println!("This should not happen here");
                    }
                }
            },
            Err(e) => {
                println!("Error {}",e);
            }
        }
    }
    let end = Instant::now();
    println!("The time is {:?}",end-start);
}

async fn port_scan(ip: Ipv4Addr, port: u16) -> PortStatus {
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let conn = TcpStream::connect(socket_address);
    let timer = timeout(Duration::from_millis(500),conn).await;
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
