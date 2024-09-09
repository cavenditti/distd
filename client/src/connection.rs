use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::error::{InvalidParameter, ServerConnection};

/// Make a connection to a given URL
pub async fn make(url: hyper::Uri) -> Result<SendRequest<Empty<Bytes>>, ServerConnection> {
    let host = url.host().ok_or(InvalidParameter::Generic {
        expected: String::from("URL"),
        got: url.to_string(),
    })?;
    let port = url.port_u16().unwrap_or(80);
    let addr = format!("{host}:{port}");
    let stream = TcpStream::connect(addr).await?;
    let io = TokioIo::new(stream);

    let (sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {err:?}");
        }
    });
    Ok(sender)
}
