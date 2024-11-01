use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::settings::cache_dir;

#[inline]
#[must_use] pub fn state_path() -> PathBuf {
    cache_dir().join("state.json")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientPersistentState {
    /// Client Uuid assigned to client from server, as a string
    pub client_uuid: Option<String>,
}

impl ClientPersistentState {
    #[must_use] pub fn commit(&self) -> Option<()> {
        std::fs::File::create(state_path())
            .map(std::io::BufWriter::new)
            .ok()
            .and_then(|file| serde_json::to_writer(file, self).ok())
    }
}

impl Default for ClientPersistentState {
    fn default() -> Self {
        std::fs::File::open(state_path())
            .map(std::io::BufReader::new)
            .ok()
            .and_then(|file| serde_json::from_reader(file).ok())
            .unwrap_or(ClientPersistentState { client_uuid: None })
    }
}

#[derive(Debug, Clone)]
pub struct ClientPid {
    /// The path to the lock file.
    /// If the path exists a client is running.
    /// The file contains only the pid of the client process
    pub pid_path: PathBuf,
}

impl ClientPid {
    fn pidfile_cleanup(pid_path: &Path) {
        std::fs::remove_file(pid_path).unwrap();
    }
}

impl Default for ClientPid {
    fn default() -> Self {
        let pid_path = cache_dir().join("pid");
        tracing::debug!("Pidfile: {}", pid_path.to_string_lossy());

        // FIXME linux-specific and fragile
        if pid_path.exists() {
            let mut buf = String::new();
            std::fs::File::open(&pid_path)
                .unwrap()
                .read_to_string(&mut buf)
                .unwrap();
            if PathBuf::from_str("/proc").unwrap().join(&buf).exists() {
                println!("Client already running, exiting.");
                exit(1);
            }
        }

        std::fs::File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&pid_path)
            .unwrap()
            .write_all(format!("{}", std::process::id()).as_ref())
            .unwrap();

        Self { pid_path }
    }
}
impl Drop for ClientPid {
    fn drop(&mut self) {
        Self::pidfile_cleanup(&self.pid_path);
    }
}
#[derive(Debug, Default, Clone)]
pub struct ClientState {
    /// Pid-lock of the client process
    pub pid: ClientPid,

    /// Persistent state of the client
    pub persistent: ClientPersistentState,
}
