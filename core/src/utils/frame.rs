use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::transport::{
    decode_wire_preface, decode_wire_rejection, encode_wire_preface, encode_wire_rejection,
    TransportOp, TransportOpError, WireCompatibilityError, WireRejection, WireVersionDeclaration,
    WIRE_PREFACE_BYTES, WIRE_REJECTION_BYTES, WIRE_REJECTION_DOMAIN,
};

/// Hard protocol ceiling. It bounds allocations before any protobuf message is
/// decoded and is intentionally above the 16 MiB sync batch target.
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame I/O error")]
    IoError(#[from] std::io::Error),

    #[error("frame payload exceeds allowed limit")]
    FrameTooLarge,

    #[error("configured frame payload limit exceeds protocol ceiling")]
    InvalidPayloadLimit,

    #[error("malformed frame")]
    MalformedFrame,

    #[error("protobuf decode error")]
    Decode(#[from] prost::DecodeError),

    #[error("protobuf encode error")]
    Encode(#[from] prost::EncodeError),

    #[error("transport op error")]
    TransportOp(#[from] TransportOpError),

    #[error("wire compatibility error")]
    WireCompatibility(#[from] WireCompatibilityError),
}

pub fn encode_length_delimited<M>(message: &M) -> Result<Vec<u8>, FrameError>
where
    M: Message,
{
    let payload_len = message.encoded_len();
    if payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(FrameError::FrameTooLarge);
    }
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
    if frame_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(FrameError::FrameTooLarge);
    }
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
    let payload_len = message.encoded_len();
    if payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(FrameError::FrameTooLarge);
    }
    let frame_len = u32::try_from(payload_len).map_err(|_| FrameError::FrameTooLarge)?;
    writer.write_all(&frame_len.to_be_bytes()).await?;
    let mut buf = Vec::with_capacity(payload_len);
    message.encode(&mut buf)?;
    writer.write_all(&buf).await?;
    Ok(())
}

pub async fn read_length_delimited_async<R, M>(reader: &mut R) -> Result<M, FrameError>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    read_length_delimited_async_with_limit(reader, MAX_FRAME_PAYLOAD_BYTES).await
}

/// Read the first response frame while recognizing a bounded wire-version rejection.
pub async fn read_length_delimited_or_wire_rejection_async<R, M>(
    reader: &mut R,
) -> Result<M, FrameError>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let frame_len = read_response_frame_length_async(reader, MAX_FRAME_PAYLOAD_BYTES).await?;
    let mut payload = vec![0u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok(M::decode(payload.as_slice())?)
}

/// Read a response frame length or return a typed fixed-size compatibility rejection.
pub async fn read_response_frame_length_async<R>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<usize, FrameError>
where
    R: AsyncRead + Unpin,
{
    if max_payload_bytes > MAX_FRAME_PAYLOAD_BYTES {
        return Err(FrameError::InvalidPayloadLimit);
    }

    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).await?;
    if prefix == WIRE_REJECTION_DOMAIN {
        let mut encoded = [0u8; WIRE_REJECTION_BYTES];
        encoded[..4].copy_from_slice(&prefix);
        reader.read_exact(&mut encoded[4..]).await?;
        let rejection = decode_wire_rejection(&encoded)?;
        return Err(WireCompatibilityError::Rejected(rejection.status).into());
    }

    let frame_len = u32::from_be_bytes(prefix) as usize;
    if frame_len > max_payload_bytes {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(frame_len)
}

/// Read and decode one frame while enforcing a caller-specific payload limit.
///
/// The caller limit cannot exceed [`MAX_FRAME_PAYLOAD_BYTES`]. The declared frame length is
/// checked against the caller limit before allocating or reading the payload.
pub async fn read_length_delimited_async_with_limit<R, M>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<M, FrameError>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    if max_payload_bytes > MAX_FRAME_PAYLOAD_BYTES {
        return Err(FrameError::InvalidPayloadLimit);
    }

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    if frame_len > max_payload_bytes {
        return Err(FrameError::FrameTooLarge);
    }
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

pub async fn write_current_wire_preface_async<W>(writer: &mut W) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(&encode_wire_preface(WireVersionDeclaration::CURRENT))
        .await?;
    Ok(())
}

pub async fn read_wire_preface_async<R>(
    reader: &mut R,
) -> Result<WireVersionDeclaration, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut encoded = [0u8; WIRE_PREFACE_BYTES];
    reader.read_exact(&mut encoded).await?;
    Ok(decode_wire_preface(&encoded)?)
}

pub async fn write_wire_rejection_async<W>(
    writer: &mut W,
    rejection: WireRejection,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&encode_wire_rejection(rejection)).await?;
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
    use crate::transport::{WireCompatibilityError, WireRejection, WireRejectionStatus};

    use super::{
        decode_length_delimited, encode_length_delimited, read_length_delimited_async,
        read_length_delimited_async_with_limit, read_length_delimited_or_wire_rejection_async,
        read_wire_preface_async, try_decode_length_delimited, write_current_wire_preface_async,
        write_wire_rejection_async, FrameError, MAX_FRAME_PAYLOAD_BYTES,
    };

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

    #[tokio::test]
    async fn current_wire_preface_round_trips_without_allocation() {
        let (mut writer, mut reader) = tokio::io::duplex(12);
        write_current_wire_preface_async(&mut writer).await.unwrap();
        assert_eq!(
            read_wire_preface_async(&mut reader).await.unwrap(),
            crate::transport::WireVersionDeclaration::CURRENT
        );
    }

    #[tokio::test]
    async fn response_reader_recognizes_every_bounded_wire_rejection() {
        for status in [
            WireRejectionStatus::UnsupportedPrefaceVersion,
            WireRejectionStatus::UnsupportedOperationVersion,
            WireRejectionStatus::UnsupportedProtobufVersion,
            WireRejectionStatus::UnsupportedVersionPair,
            WireRejectionStatus::MalformedPreface,
        ] {
            let (mut writer, mut reader) = tokio::io::duplex(12);
            write_wire_rejection_async(&mut writer, WireRejection::current(status))
                .await
                .unwrap();
            let error =
                read_length_delimited_or_wire_rejection_async::<_, SyncMessage>(&mut reader)
                    .await
                    .unwrap_err();
            assert!(matches!(
                error,
                FrameError::WireCompatibility(WireCompatibilityError::Rejected(actual))
                    if actual == status
            ));
        }
    }

    #[tokio::test]
    async fn rejects_oversized_frame_before_reading_payload() {
        use tokio::io::AsyncWriteExt;

        let (mut writer, mut reader) = tokio::io::duplex(8);
        tokio::spawn(async move {
            writer
                .write_all(&((MAX_FRAME_PAYLOAD_BYTES as u32) + 1).to_be_bytes())
                .await
                .unwrap();
        });

        let error = read_length_delimited_async::<_, SyncMessage>(&mut reader)
            .await
            .unwrap_err();
        assert!(matches!(error, FrameError::FrameTooLarge));
    }

    #[tokio::test]
    async fn caller_limit_exact_boundary_decodes_frame() {
        use tokio::io::AsyncWriteExt;

        let expected = message("caller-limit-boundary");
        let encoded = encode_length_delimited(&expected).unwrap();
        let payload_len = encoded.len() - 4;
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());
        tokio::spawn(async move {
            writer.write_all(&encoded).await.unwrap();
        });

        let decoded =
            read_length_delimited_async_with_limit::<_, SyncMessage>(&mut reader, payload_len)
                .await
                .unwrap();
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn caller_limit_rejects_declarations_from_prefix_without_reading_payload() {
        use std::time::Duration;

        use tokio::io::AsyncWriteExt;
        use tokio::time::timeout;

        async fn assert_prefix_rejected(caller_max: usize, declared_len: usize) {
            let (mut writer, mut reader) = tokio::io::duplex(8);
            writer
                .write_all(&(declared_len as u32).to_be_bytes())
                .await
                .unwrap();

            let error = timeout(
                Duration::from_millis(100),
                read_length_delimited_async_with_limit::<_, SyncMessage>(&mut reader, caller_max),
            )
            .await
            .expect("prefix must be rejected without waiting for payload")
            .unwrap_err();
            assert!(matches!(error, FrameError::FrameTooLarge));
        }

        assert_prefix_rejected(64, 65).await;
        assert_prefix_rejected(MAX_FRAME_PAYLOAD_BYTES - 1, MAX_FRAME_PAYLOAD_BYTES).await;
    }

    #[tokio::test]
    async fn rejects_caller_limit_above_protocol_ceiling_without_reading() {
        use std::time::Duration;

        use tokio::time::timeout;

        let (_writer, mut reader) = tokio::io::duplex(1);
        let error = timeout(
            Duration::from_millis(100),
            read_length_delimited_async_with_limit::<_, SyncMessage>(
                &mut reader,
                MAX_FRAME_PAYLOAD_BYTES + 1,
            ),
        )
        .await
        .expect("invalid configured limit must fail before reading")
        .unwrap_err();
        assert!(matches!(error, FrameError::InvalidPayloadLimit));
        assert_eq!(
            error.to_string(),
            "configured frame payload limit exceeds protocol ceiling"
        );
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
