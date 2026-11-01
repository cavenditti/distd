#[tokio::main]
pub async fn main() -> Result<(), distd_client::ClientError> {
    distd_client::client::cli::main().await
}
