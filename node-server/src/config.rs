use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct NodeServerConfigInterface {
    // "Optional" node ID. If this is None after all configuration
    // sources have been parsed, throw an error. We cannot have a Node
    // running without a UUID:
    pub node_id: Option<Uuid>,
    // As for the public base URL:
    pub public_url: Option<String>,
    // As for the preshared key:
    pub preshared_key: Option<String>,
    // As for the coordinator URL:
    pub coordinator_url: Option<String>,

    // Use a subdirectory in the local directory as default storage
    // path for shards:
    pub shards_path: String,
}

impl Default for NodeServerConfigInterface {
    fn default() -> NodeServerConfigInterface {
        NodeServerConfigInterface {
            node_id: None,
            public_url: None,
            preshared_key: None,
            coordinator_url: None,
            shards_path: "./shards".to_string(),
        }
    }
}
