use std::sync::LazyLock;

/// Version struct used in client-server and peer-peer version checking
#[derive(Debug, Clone, Copy)]
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
