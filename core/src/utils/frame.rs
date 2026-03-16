use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::transport::{TransportOp, TransportOpError};

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame I/O error")]
    IoError(#[from] std::io::Error),

    #[error("frame payload exceeds u32 length prefix")]
    FrameTooLarge,

    #[error("malformed frame")]
    MalformedFrame,

    #[error("protobuf decode error")]
    Decode(#[from] prost::DecodeError),

    #[error("protobuf encode error")]
    Encode(#[from] prost::EncodeError),

    #[error("transport op error")]
    TransportOp(#[from] TransportOpError),
}

pub fn encode_length_delimited<M>(message: &M) -> Result<Vec<u8>, FrameError>
where
    M: Message,
{
    let payload_len = message.encoded_len();
    let frame_len = u32::try_from(payload_len).map_err(|_| FrameError::FrameTooLarge)?;

    let mut out = Vec::with_capacity(4 + payload_len);
    out.extend_from_slice(&frame_len.to_be_bytes());
    message.encode(&mut out)?;
    Ok(out)
}

pub fn decode_length_delimited<M>(frame: &[u8]) -> Result<M, FrameError>
where
    M: Message + Default,
{
    let Some((message, consumed)) = try_decode_length_delimited(frame)? else {
        return Err(FrameError::MalformedFrame);
    };

    if consumed != frame.len() {
        return Err(FrameError::MalformedFrame);
    }

    Ok(message)
}

pub fn try_decode_length_delimited<M>(buffer: &[u8]) -> Result<Option<(M, usize)>, FrameError>
where
    M: Message + Default,
{
    if buffer.len() < 4 {
        return Ok(None);
    }

    let frame_len = u32::from_be_bytes(
        buffer[..4]
            .try_into()
            .map_err(|_| FrameError::MalformedFrame)?,
    ) as usize;
    let total_len = 4 + frame_len;
    if buffer.len() < total_len {
        return Ok(None);
    }

    let message = M::decode(&buffer[4..total_len])?;
    Ok(Some((message, total_len)))
}

pub async fn write_length_delimited_async<W, M>(
    writer: &mut W,
    message: &M,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let frame = encode_length_delimited(message)?;
    writer.write_all(&frame).await?;
    Ok(())
}

pub async fn read_length_delimited_async<R, M>(reader: &mut R) -> Result<M, FrameError>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok(M::decode(payload.as_slice())?)
}

pub async fn write_transport_op_async<W>(writer: &mut W, op: TransportOp) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_u8(op.into()).await?;
    Ok(())
}

pub async fn read_transport_op_async<R>(reader: &mut R) -> Result<TransportOp, FrameError>
where
    R: AsyncRead + Unpin,
{
    Ok(TransportOp::try_from(reader.read_u8().await?)?)
}

pub async fn write_optional_uuid_async<W>(
    writer: &mut W,
    uuid: Option<Uuid>,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    match uuid {
        Some(uuid) => {
            writer.write_u8(1).await?;
            writer.write_all(uuid.as_bytes()).await?;
        }
        None => {
            writer.write_u8(0).await?;
        }
    }
    Ok(())
}

pub async fn read_optional_uuid_async<R>(reader: &mut R) -> Result<Option<Uuid>, FrameError>
where
    R: AsyncRead + Unpin,
{
    match reader.read_u8().await? {
        0 => Ok(None),
        1 => {
            let mut raw = [0u8; 16];
            reader.read_exact(&mut raw).await?;
            Ok(Some(Uuid::from_bytes(raw)))
        }
        _ => Err(FrameError::MalformedFrame),
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::{sync_message::Msg, ManifestRequest, SyncMessage};

    use super::{decode_length_delimited, encode_length_delimited, try_decode_length_delimited};

    fn message(artifact_id: &str) -> SyncMessage {
        SyncMessage {
            msg: Some(Msg::ManifestRequest(ManifestRequest {
                artifact_id: artifact_id.to_owned(),
                version: Some(7),
            })),
        }
    }

    #[test]
    fn roundtrips_single_frame() {
        let encoded = encode_length_delimited(&message("artifact-a")).unwrap();
        let decoded = decode_length_delimited::<SyncMessage>(&encoded).unwrap();

        assert_eq!(decoded, message("artifact-a"));
    }

    #[test]
    fn decodes_multiple_frames_from_buffer() {
        let first = encode_length_delimited(&message("artifact-a")).unwrap();
        let second = encode_length_delimited(&message("artifact-b")).unwrap();
        let mut buffer = first.clone();
        buffer.extend_from_slice(&second);

        let (decoded_first, consumed) = try_decode_length_delimited::<SyncMessage>(&buffer)
            .unwrap()
            .unwrap();
        let (decoded_second, consumed_second) =
            try_decode_length_delimited::<SyncMessage>(&buffer[consumed..])
                .unwrap()
                .unwrap();

        assert_eq!(decoded_first, message("artifact-a"));
        assert_eq!(decoded_second, message("artifact-b"));
        assert_eq!(consumed + consumed_second, buffer.len());
    }

    #[test]
    fn waits_for_complete_frame() {
        let encoded = encode_length_delimited(&message("artifact-a")).unwrap();

        assert!(try_decode_length_delimited::<SyncMessage>(&encoded[..3])
            .unwrap()
            .is_none());
        assert!(
            try_decode_length_delimited::<SyncMessage>(&encoded[..encoded.len() - 1])
                .unwrap()
                .is_none()
        );
    }
}
