use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

#[derive(Debug, Clone, Copy)]
pub struct SocketAddrPortRange {
    ip_addr: IpAddr,
    port_from: u16,
    port_to: u16,
}

pub struct SocketAddrPortRangeIterator {
    socket_addr_port_range: SocketAddrPortRange,
    current_port: u16,
}

// Implement `Iterator` for `SocketAddrPortRange`.
impl Iterator for SocketAddrPortRangeIterator {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_port += 1;
        if self.current_port < self.socket_addr_port_range.port_to {
            Some(SocketAddr::new(
                self.socket_addr_port_range.ip_addr,
                self.current_port,
            ))
        } else {
            None
        }
    }
}

impl ToSocketAddrs for SocketAddrPortRange {
    type Iter = SocketAddrPortRangeIterator;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        Ok(SocketAddrPortRangeIterator {
            socket_addr_port_range: *self,
            current_port: self.port_from,
        })
    }
}
