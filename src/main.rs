use std::net::{Ipv4Addr, SocketAddr};
use std::time::Instant;
use tokio::net::{TcpSocket, TcpStream};

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
    for port_number in start_port..=end_port {
        println!("Scanning port {}", port_number);
        let res = port_scan(ip, port_number).await;
        if (res) {
            println!("{} is open", port_number);
        }
    }
    let end = Instant::now();
    println!("The time is {:?}",end-start);
}

async fn port_scan(ip: Ipv4Addr, port: u16) -> bool {
    let socket_address = SocketAddr::new(std::net::IpAddr::V4(ip), port);
    let conn = TcpStream::connect(socket_address).await;
    match conn {
        Ok(_) => {
            // println!("{:?} is open",val);
            return true;
        }
        Err(_) => {
            // println!("The port is not open, {}",val);
            return false;
        }
    }
}
