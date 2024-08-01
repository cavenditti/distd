use std::cmp::Ordering;
use std::net::SocketAddr;
use std::time::SystemTime;

use serde::{ser::SerializeStruct, Serialize, Serializer};
use uuid::Uuid;

use distd_core::unique_name::UniqueName;
use distd_core::version::Version;

pub type ClientName = UniqueName;

#[derive(Debug, Clone)]
pub struct Client {
    pub name: ClientName,
    pub addr: SocketAddr,
    pub uuid: Uuid,
    //realm: Option<Arc<Realm>>,
    pub version: Option<Version>,
    pub last_heartbeat: SystemTime,
}

impl PartialEq for Client {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}
impl PartialOrd for Client {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.uuid.partial_cmp(&other.uuid)
    }
}

impl Serialize for Client {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("Client", 5)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("addr", &self.addr)?;
        state.serialize_field("uuid", &self.uuid.as_u128())?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("last_heartbeat", &self.last_heartbeat)?;
        state.end()
    }
}
