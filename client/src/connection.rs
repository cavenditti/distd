use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

pub async fn make_connection(url: hyper::Uri) -> Result<SendRequest<Empty<Bytes>>, anyhow::Error> {
    let host = url.host().expect("uri has no host");
    let port = url.port_u16().unwrap_or(80);
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(anyhow::Error::msg)?;
    let io = TokioIo::new(stream);

    let (sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(anyhow::Error::msg)?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });
    Ok(sender)
}
