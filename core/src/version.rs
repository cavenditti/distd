use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::LazyLock};

/// Version struct used in client-server and peer-peer version checking
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
    patch: u16,
}

pub static VERSION: LazyLock<Version> = LazyLock::new(|| Version {
    major: env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
    minor: env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
    patch: env!("CARGO_PKG_VERSION_PATCH").parse().unwrap(),
});

#[derive(Debug, PartialEq, Eq)]
pub struct ParseVersionError;

impl std::fmt::Display for ParseVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad("Cannot parse version string")
    }
}

impl From<(u16, u16, u16)> for Version {
    fn from(value: (u16, u16, u16)) -> Self {
        let (x, y, z) = value;
        Version {
            major: x,
            minor: y,
            patch: z,
        }
    }
}

impl std::str::FromStr for Version {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Result<Vec<u16>, ParseVersionError> = s
            .split('.')
            .map(|frag| u16::from_str(frag).map_err(|_| ParseVersionError))
            .collect();
        let v = v?;
        if v.len() != 3 {
            Err(ParseVersionError)
        } else {
            Ok(Version {
                major: v[0],
                minor: v[1],
                patch: v[2],
            })
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        *VERSION
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}.{}.{}", self.major, self.minor, self.patch))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_from_str() {
        let v = Version::from_str("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn test_from_str_invalid() {
        assert!(Version::from_str("1.2invalid.3").is_err())
    }
}
