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

    // How often the node should try to reconstruct a shard before
    // giving up on a reconstruct request. This does not count towards
    // failures of nodes of serve a shard at all (e.g. by sending a
    // 404), but for cases in which nodes don't respond or interrupt a
    // transfer after it was initiated with a 200 OK response.
    pub reconstruct_retries: usize,

    // Minimum interval which this node's statistics need to be
    // scraped by the coordinator. If a node does not see a scrape in
    // this period, it assumes it has to re-register with the
    // coordinator.
    pub min_stats_query_interval_sec: u64,
}

impl Default for NodeServerConfigInterface {
    fn default() -> NodeServerConfigInterface {
        NodeServerConfigInterface {
            node_id: None,
            public_url: None,
            preshared_key: None,
            coordinator_url: None,
            shards_path: "./shards".to_string(),
            reconstruct_retries: 2,
            min_stats_query_interval_sec: 30,
        }
    }
}
