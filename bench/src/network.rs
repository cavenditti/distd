use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NetworkProfile {
    pub delay_ms: u64,
    pub jitter_ms: u64,
    pub bandwidth_mbps: Option<f64>,
    pub loss_percent: f64,
}

impl NetworkProfile {
    pub fn new(
        delay_ms: u64,
        jitter_ms: u64,
        bandwidth_mbps: Option<f64>,
        loss_percent: f64,
    ) -> Result<Option<Self>, String> {
        if let Some(rate) = bandwidth_mbps {
            if rate <= 0.0 {
                return Err("--net-bandwidth-mbps must be greater than 0".to_string());
            }
        }
        if !(0.0..=100.0).contains(&loss_percent) {
            return Err("--net-loss-percent must be between 0 and 100".to_string());
        }

        let profile = Self {
            delay_ms,
            jitter_ms,
            bandwidth_mbps,
            loss_percent,
        };

        if profile.is_active() {
            Ok(Some(profile))
        } else {
            Ok(None)
        }
    }

    pub fn is_active(&self) -> bool {
        self.delay_ms > 0
            || self.jitter_ms > 0
            || self.bandwidth_mbps.is_some()
            || self.loss_percent > 0.0
    }

    pub fn label(&self) -> String {
        let mut parts = Vec::new();
        if self.delay_ms > 0 {
            parts.push(format!("delay={}ms", self.delay_ms));
        }
        if self.jitter_ms > 0 {
            parts.push(format!("jitter={}ms", self.jitter_ms));
        }
        if let Some(rate) = self.bandwidth_mbps {
            parts.push(format!("bw={rate:.1}Mbps"));
        }
        if self.loss_percent > 0.0 {
            parts.push(format!("loss={:.2}%", self.loss_percent));
        }

        if parts.is_empty() {
            "default".to_string()
        } else {
            parts.join(",")
        }
    }

    fn sample_delay<R: Rng>(&self, rng: &mut R) -> Duration {
        if self.delay_ms == 0 && self.jitter_ms == 0 {
            return Duration::ZERO;
        }

        let jitter = if self.jitter_ms == 0 {
            0_i64
        } else {
            rng.gen_range(-(self.jitter_ms as i64)..=(self.jitter_ms as i64))
        };
        let millis = (self.delay_ms as i64 + jitter).max(0) as u64;
        Duration::from_millis(millis)
    }

    fn bandwidth_bytes_per_sec(&self) -> Option<f64> {
        self.bandwidth_mbps.map(|mbps| mbps * 1024.0 * 1024.0 / 8.0)
    }

    fn should_drop<R: Rng>(&self, rng: &mut R) -> bool {
        self.loss_percent > 0.0 && rng.gen_bool((self.loss_percent / 100.0).clamp(0.0, 1.0))
    }
}

struct RateLimiter {
    bytes_per_sec: Option<f64>,
    next_slot: Instant,
}

impl RateLimiter {
    fn new(bytes_per_sec: Option<f64>) -> Self {
        Self {
            bytes_per_sec,
            next_slot: Instant::now(),
        }
    }

    fn wait(&mut self, bytes: usize) {
        let Some(rate) = self.bytes_per_sec else {
            return;
        };

        let now = Instant::now();
        if self.next_slot > now {
            thread::sleep(self.next_slot - now);
        }

        let scheduled_from = self.next_slot.max(now);
        let transmit = Duration::from_secs_f64(bytes as f64 / rate);
        self.next_slot = scheduled_from + transmit;
    }
}

pub struct UdpShaperProxy {
    listen_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl UdpShaperProxy {
    pub fn start(target_addr: SocketAddr, profile: NetworkProfile) -> Result<Self, String> {
        let socket = UdpSocket::bind(("127.0.0.1", 0))
            .map_err(|err| format!("bind udp proxy: {err}"))?;
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|err| format!("set udp proxy timeout: {err}"))?;
        let listen_addr = socket
            .local_addr()
            .map_err(|err| format!("read udp proxy address: {err}"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let worker_socket = socket
            .try_clone()
            .map_err(|err| format!("clone udp proxy socket: {err}"))?;
        let (client_tx, client_rx) = mpsc::channel::<UdpPacket>();
        let (server_tx, server_rx) = mpsc::channel::<UdpPacket>();
        let client_worker = spawn_udp_worker(worker_socket, Arc::clone(&stop_flag), client_rx, profile.clone());
        let server_worker = spawn_udp_worker(socket.try_clone().map_err(|err| format!("clone udp proxy socket: {err}"))?, Arc::clone(&stop_flag), server_rx, profile.clone());

        let thread = thread::spawn(move || {
            let mut buffer = vec![0_u8; 65_536];
            let mut latest_client: Option<SocketAddr> = None;

            while !stop_flag.load(Ordering::Relaxed) {
                match socket.recv_from(&mut buffer) {
                    Ok((size, source)) => {
                        if source == target_addr {
                            if let Some(client_addr) = latest_client {
                                if client_tx
                                    .send(UdpPacket {
                                        payload: buffer[..size].to_vec(),
                                        destination: client_addr,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        } else {
                            latest_client = Some(source);
                            if server_tx
                                .send(UdpPacket {
                                    payload: buffer[..size].to_vec(),
                                    destination: target_addr,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    Err(err)
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(err) => {
                        if !stop_flag.load(Ordering::Relaxed) {
                            tracing::warn!("udp proxy receive failed: {err}");
                        }
                    }
                }
            }

            drop(client_tx);
            drop(server_tx);
            let _ = client_worker.join();
            let _ = server_worker.join();
        });

        Ok(Self {
            listen_addr,
            stop,
            thread: Some(thread),
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl Drop for UdpShaperProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(socket) = UdpSocket::bind(("127.0.0.1", 0)) {
            let _ = socket.send_to(&[0], self.listen_addr);
        }
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug)]
struct UdpPacket {
    payload: Vec<u8>,
    destination: SocketAddr,
}

fn spawn_udp_worker(
    socket: UdpSocket,
    stop: Arc<AtomicBool>,
    receiver: mpsc::Receiver<UdpPacket>,
    profile: NetworkProfile,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut limiter = RateLimiter::new(profile.bandwidth_bytes_per_sec());
        let mut rng = rand::thread_rng();
        while !stop.load(Ordering::Relaxed) {
            let packet = match receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(packet) => packet,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };

            if profile.should_drop(&mut rng) {
                continue;
            }

            let delay = profile.sample_delay(&mut rng);
            if !delay.is_zero() {
                thread::sleep(delay);
            }
            limiter.wait(packet.payload.len());

            if let Err(err) = socket.send_to(&packet.payload, packet.destination) {
                tracing::debug!("udp proxy send failed: {err}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{NetworkProfile, UdpShaperProxy};
    use std::net::UdpSocket;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn network_profile_label_is_stable() {
        let profile = NetworkProfile::new(40, 5, Some(12.5), 0.5)
            .expect("profile should parse")
            .expect("profile should be active");

        assert_eq!(profile.label(), "delay=40ms,jitter=5ms,bw=12.5Mbps,loss=0.50%");
    }

    #[test]
    fn udp_proxy_forwards_datagrams() {
        let server = UdpSocket::bind(("127.0.0.1", 0)).expect("bind udp echo server");
        server
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set timeout");
        let target = server.local_addr().expect("server addr");
        let server_thread = thread::spawn(move || {
            let mut buffer = [0_u8; 64];
            let (size, source) = server.recv_from(&mut buffer).expect("receive datagram");
            server
                .send_to(&buffer[..size], source)
                .expect("echo datagram");
        });

        let profile = NetworkProfile {
            delay_ms: 0,
            jitter_ms: 0,
            bandwidth_mbps: None,
            loss_percent: 0.0,
        };
        let proxy = UdpShaperProxy::start(target, profile).expect("start udp proxy");

        let client = UdpSocket::bind(("127.0.0.1", 0)).expect("bind udp client");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set timeout");
        client
            .send_to(b"ping", proxy.listen_addr())
            .expect("send through proxy");

        let mut buffer = [0_u8; 16];
        let (size, _) = client.recv_from(&mut buffer).expect("receive echo");
        assert_eq!(&buffer[..size], b"ping");

        drop(proxy);
        server_thread.join().expect("join udp server");
    }
}