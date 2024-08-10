use blake3::Hash;
use std::array::TryFromSliceError;
use std::io::{Read, Write};
use std::net::SocketAddr;
use utp::{UtpSocket, UtpStream};

use crate::metadata::CHUNK_SIZE;

#[derive(Debug)]
pub struct Peer {
    pub id: String,
    // uuid assigned from server
    //pub uuid: Uuid,
    pub addr: SocketAddr,
}

impl Peer {
    pub fn new(id: String, addr: SocketAddr) -> Self {
        Peer { id, addr }
    }

    pub async fn connect(&self) -> Result<UtpSocket, Box<dyn std::error::Error>> {
        let socket = UtpSocket::connect(self.addr)?;
        Ok(socket)
    }
}

/// Peer message structure

/// This is a very simplified BIttorent message.
/// Because we have a shared common server we can assume some share knowledge among peers, in particular:
///     - No need for a bitfield message, it is provided by the server
///     - No choke/unchoke, no interested/not interested
///     - No cancel
///     - No need for sharing lenghts for pieces, they are provided by the server
#[derive(Debug, PartialEq)]
pub enum PeerMessage {
    Have(Hash),
    Request(Hash),
    Piece { hash: Hash, block: Vec<u8> },
    Choke,
    Unchoke,
}

/*
#[inline(always)]
fn get_u64(bytes: &[u8], starting: usize) -> Option<u64> {
    match bytes[starting..starting + 8].array_chunks::<8>().next() {
        Some(b) => Some(u64::from_be_bytes(*b)),
        _ => None,
    }
}
*/
#[inline(always)]
fn get_index(bytes: &[u8]) -> u64 {
    u64::from_be_bytes([
        bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
    ])
}

#[inline(always)]
fn get_hash(bytes: &[u8]) -> Result<Hash, TryFromSliceError> {
    Ok(Hash::from_bytes(
        bytes[1..33]
            .iter()
            .map(|x| u8::from_be(*x))
            .collect::<Vec<u8>>()
            .as_slice()
            .try_into()?,
    ))
}

impl PeerMessage {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            0 => {
                if bytes.len() < 33 {
                    return None;
                }
                get_hash(bytes).and_then(|x| Ok(Self::Have(x))).ok()
            }
            1 => {
                if bytes.len() < 33 {
                    return None;
                }
                get_hash(bytes).and_then(|x| Ok(Self::Request(x))).ok()
            }
            2 => {
                if bytes.len() < 33 {
                    return None;
                }
                if let Ok(hash) = get_hash(bytes) {
                    let block = bytes[33..].to_vec();
                    Some(Self::Piece { hash, block })
                } else {
                    None
                }
            }
            3 => Some(Self::Choke),
            4 => Some(Self::Unchoke),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Have(hash) => {
                let mut msg = vec![0];
                msg.extend(hash.as_bytes().map(|x| x.to_be()));
                msg
            }
            Self::Request(hash) => {
                let mut msg = vec![1];
                msg.extend(hash.as_bytes().map(|x| x.to_be()));
                msg
            }
            Self::Piece { hash, block } => {
                let mut msg = vec![2];
                msg.extend(hash.as_bytes().map(|x| x.to_be()));
                msg.extend(block);
                msg
            }
            Self::Choke => vec![3],
            Self::Unchoke => vec![4],
        }
    }
}

pub async fn send_message(
    stream: &mut UtpStream,
    message: PeerMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let msg_bytes = message.to_bytes();
    let len = msg_bytes.len() as u32;
    let mut data = len.to_be_bytes().to_vec();
    data.extend(msg_bytes);

    stream.write(&data)?;
    Ok(())
}

pub async fn receive_message(
    stream: &mut UtpStream,
) -> Result<Option<PeerMessage>, Box<dyn std::error::Error>> {
    let mut buf = vec![0u8; 4 + 9 + CHUNK_SIZE];
    let len = stream.read(&mut buf)?;

    if len < 4 {
        return Ok(None);
    }

    let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    if msg_len + 4 > len {
        return Ok(None);
    }

    let msg_bytes = &buf[4..4 + msg_len];
    Ok(PeerMessage::from_bytes(msg_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::task;
    use tokio::time::{sleep, Duration};
    use utp::UtpListener;

    #[test]
    fn test_peer_creation() {
        let peer = Peer::new(String::from("peer_id"), "127.0.0.1:8080".parse().unwrap());
        assert_eq!(peer.id, "peer_id");
        assert_eq!(peer.addr, "127.0.0.1:8080".parse().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_peer_connection() {

        // Start a UtpListener to accept the connection
        task::spawn(async move {
            let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
            let listener = UtpListener::bind(addr).unwrap();

            for connection in listener.incoming() {
                // Spawn a new handler for each new connection
                if let Ok((mut socket, _src)) = connection {
                    let mut buf = [0; 12];
                    socket.recv_from(&mut buf).unwrap();
                    println!("RECV: {:?}", buf);
                    tokio::time::sleep(Duration::from_millis(150)).await; // Ensure the listener is ready
                    socket.flush();
                    socket.send_to(&buf).unwrap();
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await; // Ensure the listener is ready

        let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let peer = Peer::new(String::from("peer_id"), addr);
        let mut socket = peer.connect().await.unwrap();

        let message = b"Hello Peer!!";
        println!("SENT: {:?}", message);
        socket.send_to(message).unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await; // Ensure the listener is ready

        let mut buf = [0; 12];
        socket.recv_from(&mut buf).unwrap();
        println!("RECV2: {:?}", buf);
        assert_eq!(&buf, message);
    }

    #[test]
    fn test_peer_message_conversion() {
        let some_hash = blake3::hash(&[42]);
        let message = PeerMessage::Have(some_hash);
        let bytes = message.to_bytes();
        assert_eq!(
            PeerMessage::from_bytes(&bytes),
            Some(PeerMessage::Have(some_hash))
        );

        let message = PeerMessage::Request(some_hash);
        let bytes = message.to_bytes();
        assert_eq!(
            PeerMessage::from_bytes(&bytes),
            Some(PeerMessage::Request(some_hash))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_receive_message() {
        let addr = "127.0.0.1:1234";
        task::spawn(async move {
            let mut stream = UtpStream::bind(addr).expect("Error binding stream");

            let msg = receive_message(&mut stream).await.unwrap().unwrap();
            assert_eq!(msg, PeerMessage::Have(blake3::hash(&[42])));
            send_message(&mut stream, PeerMessage::Unchoke)
                .await
                .unwrap();
        });

        sleep(Duration::from_millis(100)).await; // Ensure the listener is ready

        let mut stream = UtpStream::connect(addr).expect("Error binding stream");

        send_message(&mut stream, PeerMessage::Have(blake3::hash(&[42])))
            .await
            .unwrap();
        sleep(Duration::from_millis(100)).await; // Ensure the listener is ready
        let msg = receive_message(&mut stream).await.unwrap().unwrap();
        assert_eq!(msg, PeerMessage::Unchoke);
    }
}
