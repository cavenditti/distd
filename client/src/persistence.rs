use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
};

use crate::settings::cache_dir;

#[derive(Debug, Clone)]
pub struct ClientState {
    /// The path to the lock file.
    /// If the path exists a client is running.
    /// The file contains only the pid of the client process
    pub pid_path: PathBuf,
}

impl ClientState {
    fn pidfile_cleanup(pid_path: &Path) {
        std::fs::remove_file(pid_path).unwrap()
    }
}

impl Default for ClientState {
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
impl Drop for ClientState {
    fn drop(&mut self) {
        Self::pidfile_cleanup(&self.pid_path);
    }
}
