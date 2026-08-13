#[cfg(not(unix))]
compile_error!("quic-fetch-acceptance currently supports Unix platforms only");

pub mod audit;
pub mod compose;
pub mod environment;
pub mod evidence;
pub mod execute;
pub mod model;
pub mod prepare;
pub mod process;
pub mod provenance;

#[cfg(test)]
mod acceptance_tests;
