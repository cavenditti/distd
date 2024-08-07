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
    Have(u64),
    Request(u64),
    Piece { index: u64, block: Vec<u8> },
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
    Ok(Hash::from_bytes(bytes[..33].try_into()?))
}

impl PeerMessage {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            0 => {
                if bytes.len() < 9 {
                    return None;
                }
                let index = get_index(bytes);
                Some(Self::Have(index))
            }
            1 => {
                if bytes.len() < 9 {
                    return None;
                }
                let index = get_index(bytes);
                Some(Self::Request(index))
            }
            2 => {
                if bytes.len() < 9 {
                    return None;
                }
                let index = get_index(bytes);
                let block = bytes[5..].to_vec();
                Some(Self::Piece { index, block })
            }
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Have(index) => {
                let mut msg = vec![4];
                msg.extend(&index.to_be_bytes());
                msg
            }
            Self::Request(index) => {
                let mut msg = vec![1];
                msg.extend(&index.to_be_bytes());
                msg
            }
            Self::Piece { index, block } => {
                let mut msg = vec![2];
                msg.extend(&index.to_be_bytes());
                msg.extend(block);
                msg
            }
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

