//! Minimal QUIC throughput test: server writes 256 MiB of zeros, client reads it.
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let size: u64 = 256 * 1024 * 1024;

    // Generate self-signed cert
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().clone());
    let key_der = rustls::pki_types::PrivateKeyDer::from(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()),
    );

    let use_bbr = std::env::var("QUIC_CC").unwrap_or_default() == "bbr";

    fn make_transport(use_bbr: bool) -> quinn::TransportConfig {
        let mut transport = quinn::TransportConfig::default();
        transport.receive_window(quinn::VarInt::from_u32(104_857_600));
        transport.stream_receive_window(quinn::VarInt::from_u32(104_857_600));
        transport.send_window(104_857_600);
        transport.initial_rtt(std::time::Duration::from_millis(1));
        transport.initial_mtu(9000);
        transport.min_mtu(9000);
        if use_bbr {
            let mut bbr = quinn::congestion::BbrConfig::default();
            bbr.initial_window(10_485_760);
            transport.congestion_controller_factory(Arc::new(bbr));
        } else {
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(10_485_760);
            transport.congestion_controller_factory(Arc::new(cubic));
        }
        transport
    }

    // Server config
    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert_der], key_der).unwrap();
    server_config.transport_config(Arc::new(make_transport(use_bbr)));

    let server_endpoint =
        quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();

    // Client config
    let rustls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config).unwrap(),
    ));
    client_config.transport_config(Arc::new(make_transport(use_bbr)));

    let mut client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_config);

    // Server task
    let server = tokio::spawn(async move {
        let conn = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, _recv) = conn.accept_bi().await.unwrap();
        let buf = vec![0u8; 1024 * 1024]; // 1 MiB chunks
        let mut remaining = size;
        while remaining > 0 {
            let n = remaining.min(buf.len() as u64) as usize;
            send.write_all(&buf[..n]).await.unwrap();
            remaining -= n as u64;
        }
        send.finish().unwrap();
        send.stopped().await.ok();
    });

    // Client
    let conn = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (_, mut recv) = conn.open_bi().await.unwrap();

    let t0 = Instant::now();
    let mut total = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        match recv.read(&mut buf).await.unwrap() {
            Some(n) => total += n as u64,
            None => break,
        }
    }
    let elapsed = t0.elapsed();
    let mib_per_sec = (total as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    eprintln!(
        "Received {} MiB in {:.3}s = {:.1} MiB/s",
        total / (1024 * 1024),
        elapsed.as_secs_f64(),
        mib_per_sec,
    );

    server.await.unwrap();
}

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}
