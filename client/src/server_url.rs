use std::{fmt, net::IpAddr, str::FromStr};

use thiserror::Error;

pub const MAX_CANONICAL_SERVER_URL_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalServerUrl(String);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid server URL: {0}")]
pub struct ServerUrlError(&'static str);

impl CanonicalServerUrl {
    pub fn parse(raw: &str) -> Result<Self, ServerUrlError> {
        canonicalize(raw).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn dial_authority(&self) -> &str {
        self.0.strip_prefix("quic://").expect("canonical URL")
    }

    pub fn tls_server_name(&self) -> &str {
        let authority = self.dial_authority();
        if let Some(rest) = authority.strip_prefix('[') {
            rest.split_once("]:").expect("canonical IPv6 authority").0
        } else {
            authority.rsplit_once(':').expect("canonical authority").0
        }
    }
}

impl fmt::Display for CanonicalServerUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CanonicalServerUrl {
    type Err = ServerUrlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

fn canonicalize(raw: &str) -> Result<String, ServerUrlError> {
    if !raw.is_ascii() {
        return Err(ServerUrlError("non-ASCII URL"));
    }
    let authority = raw
        .strip_prefix("quic://")
        .or_else(|| raw.strip_prefix("udp://"))
        .ok_or(ServerUrlError("scheme must be quic or udp"))?;
    if authority.is_empty() || authority.contains(['@', '?', '#', '%']) {
        return Err(ServerUrlError("invalid authority"));
    }
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.contains('/') {
        return Err(ServerUrlError("path is not allowed"));
    }

    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, port) = rest
            .split_once("]:")
            .ok_or(ServerUrlError("IPv6 authority requires brackets and port"))?;
        if host.contains('%') {
            return Err(ServerUrlError("IPv6 zone identifiers are not allowed"));
        }
        let ip = host
            .parse::<IpAddr>()
            .map_err(|_| ServerUrlError("invalid IPv6 address"))?;
        if !ip.is_ipv6() {
            return Err(ServerUrlError("brackets require IPv6"));
        }
        (format!("[{ip}]"), port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or(ServerUrlError("explicit port is required"))?;
        if host.is_empty() || host.contains(':') {
            return Err(ServerUrlError("invalid host"));
        }
        let host = match host.parse::<IpAddr>() {
            Ok(ip) if ip.is_ipv4() => ip.to_string(),
            Ok(_) => return Err(ServerUrlError("IPv6 requires brackets")),
            Err(_) => {
                if host
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte == b'.')
                {
                    return Err(ServerUrlError("invalid IPv4 address"));
                }
                let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
                if host.is_empty()
                    || host.split('.').any(|label| {
                        label.is_empty()
                            || label.len() > 63
                            || label.starts_with('-')
                            || label.ends_with('-')
                            || !label
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    })
                {
                    return Err(ServerUrlError("invalid DNS host"));
                }
                host
            }
        };
        (host, port)
    };
    let port = port
        .parse::<u16>()
        .map_err(|_| ServerUrlError("invalid port"))?;
    if port == 0 {
        return Err(ServerUrlError("port must be nonzero"));
    }
    let canonical = format!("quic://{host}:{port}");
    if canonical.len() > MAX_CANONICAL_SERVER_URL_BYTES {
        return Err(ServerUrlError("canonical URL exceeds length limit"));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_aliases_hosts_and_ips() {
        assert_eq!(
            CanonicalServerUrl::parse("udp://EXAMPLE.COM.:00443/")
                .unwrap()
                .as_str(),
            "quic://example.com:443"
        );
        assert_eq!(
            CanonicalServerUrl::parse("quic://127.000.000.001:9"),
            Err(ServerUrlError("invalid IPv4 address"))
        );
        assert_eq!(
            CanonicalServerUrl::parse("quic://[2001:0db8::1]:50051")
                .unwrap()
                .as_str(),
            "quic://[2001:db8::1]:50051"
        );
    }

    #[test]
    fn rejects_ambiguous_or_extended_urls() {
        for raw in [
            "https://example.com:1",
            "quic://example.com",
            "quic://example.com:0",
            "quic://user@example.com:1",
            "quic://example.com:1/path",
            "quic://example.com:1?x",
            "quic://example.com:1#x",
            "quic://[fe80::1%1]:1",
            "quic://example.com:1//",
        ] {
            assert!(CanonicalServerUrl::parse(raw).is_err(), "{raw}");
        }
    }
}
