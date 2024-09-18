use distd_core::tonic;
use distd_core::tonic::{metadata::MetadataValue, service::Interceptor, Status};

#[derive(Debug)]
pub struct DistdGrpcClient {
    pub uuid: MetadataValue<distd_core::tonic::metadata::Binary>
}

impl Interceptor for DistdGrpcClient {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        tracing::trace!("request: {request:?}");
        request.metadata_mut().insert_bin("x-uuid-bin", self.uuid.clone());
        Ok(request)
    }
}
