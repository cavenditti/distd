use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransportOp {
    Register = 1,
    Fetch = 2,
    Sync = 3,
}

#[derive(Debug, Error)]
pub enum TransportOpError {
    #[error("unknown transport op {0}")]
    Unknown(u8),
}

impl From<TransportOp> for u8 {
    fn from(value: TransportOp) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for TransportOp {
    type Error = TransportOpError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Register),
            2 => Ok(Self::Fetch),
            3 => Ok(Self::Sync),
            other => Err(TransportOpError::Unknown(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransportOp;

    #[test]
    fn op_roundtrip() {
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Register)).unwrap(), TransportOp::Register);
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Fetch)).unwrap(), TransportOp::Fetch);
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Sync)).unwrap(), TransportOp::Sync);
    }
}