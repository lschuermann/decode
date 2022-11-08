use std::borrow::Cow;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectRetrievalShardSpec {
    /// Hex-encoded shard digest
    pub digest: String,
    /// Node indicies in `node_map` list
    pub nodes: Vec<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectRetrievalMap {
    // Object, chunk & shard size specification:
    /// Size of the entire object in bytes
    pub object_size: u64,
    /// Size of each chunk of the object in bytes
    ///
    /// The number of chunks in an object can be calculated as
    ///
    ///     chunk_count = ceil(object_size / chunk_size).
    ///
    /// Chunks are evenly distributed over the entire object, and the
    /// start address of a chunk `i` can be calculated as
    ///
    ///     chunk_start = i * chunk_size.
    ///
    /// In case `object_size` is not evenly divisible by `chunk_size`,
    /// the last chunk may be smaller than `chunk_size`, its size can
    /// be calculated as `object_size mod chunk_size`.
    pub chunk_size: u64,
    /// Size of each shard in bytes (in this example, ~49MB)
    ///
    /// The number of shards in a chunk can be calculated as
    ///
    ///     shard_count = ceil(chunk_size / shard_size)
    ///
    /// . Shards are evenly distributed over a chunk, and the
    /// start address of a shard `i` can be calculated as
    ///
    ///     shard_start = i * shard_size
    ///
    /// . In case `chunk_size` is not evenly divisible by `shard_size`,
    /// the last shard may be smaller than `shard_size`, its size can
    /// be calculated as `chunk_size mod shard_size`. For purposes of
    /// parity shard calculation and reconstruction, the last chunk is to
    /// be padded to the full `shard_size` with null-bytes.
    pub shard_size: u64,

    // Reed-Solomon Code Parameters:
    /// Number of Reed-Solomon data shards of every chunk
    ///
    /// Must be equal to `ceil(chunk_size / shard_size)`.
    pub code_ratio_data: u8,
    /// Number of Reed-Solomon parity shards of every chunk
    ///
    /// All parity shards are `shard_size` in length.
    pub code_ratio_parity: u8,

    /// Mapping from shards to digests and node indices:
    pub shard_map: Vec<Vec<ObjectRetrievalShardSpec>>,

    /// Mapping from node indices to node base URLs (excluding the /v0
    /// API version subpath):
    pub node_map: Vec<String>,
}

impl ObjectRetrievalMap {
    pub fn get_shard_node<'a>(
        &'a self,
        chunk_idx: usize,
        shard_idx: usize,
        node_idx: usize,
    ) -> Option<&'a str> {
        let node_map_idx = self
            .shard_map
            .get(chunk_idx)?
            .get(shard_idx)?
            .nodes
            .get(node_idx)?;
        self.node_map.get(*node_map_idx).map(|s| s.as_ref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectUploadShardSpec {
    /// Base-URL of the node (excluding the /v0 API version subpath):
    pub node: String,
    /// Opaque ticket to be passed to the node in the `Authorization`
    /// header on the file upload `POST` request
    pub ticket: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObjectUploadMap {
    // Object, chunk & shard size specification:
    /// Size of the entire object in bytes
    pub object_size: u64,
    /// Size of each chunk of the object in bytes
    ///
    /// The number of chunks in an object can be calculated as
    ///
    ///     chunk_count = ceil(object_size / chunk_size).
    ///
    /// Chunks are evenly distributed over the entire object, and the
    /// start address of a chunk `i` can be calculated as
    ///
    ///     chunk_start = i * chunk_size.
    ///
    /// In case `object_size` is not evenly divisible by `chunk_size`,
    /// the last chunk may be smaller than `chunk_size`, its size can
    /// be calculated as `object_size mod chunk_size`.
    pub chunk_size: u64,
    /// Size of each shard in bytes (in this example, ~49MB)
    ///
    /// The number of shards in a chunk can be calculated as
    ///
    ///     shard_count = ceil(chunk_size / shard_size)
    ///
    /// . Shards are evenly distributed over a chunk, and the
    /// start address of a shard `i` can be calculated as
    ///
    ///     shard_start = i * shard_size
    ///
    /// . In case `chunk_size` is not evenly divisible by `shard_size`,
    /// the last shard may be smaller than `shard_size`, its size can
    /// be calculated as `chunk_size mod shard_size`. For purposes of
    /// parity shard calculation and reconstruction, the last chunk is to
    /// be padded to the full `shard_size` with null-bytes.
    pub shard_size: u64,

    // Reed-Solomon Code Parameters:
    /// Number of Reed-Solomon data shards of every chunk
    ///
    /// Must be equal to `ceil(chunk_size / shard_size)`.
    pub code_ratio_data: u8,
    /// Number of Reed-Solomon parity shards of every chunk
    ///
    /// All parity shards are `shard_size` in length.
    pub code_ratio_parity: u8,

    /// Mapping from shards to nodes:
    pub shard_map: Vec<Vec<ObjectUploadShardSpec>>,

    /// Assigned UUID of the object
    pub object_id: Uuid,

    /// Other supplied fields. When re-serializing this payload to
    /// include it within the `PUT` request to finalize an object
    /// upload, the node will check a collection of the fields
    /// supplied here against an included signature field, to avoid
    /// holding state for non-finalized objects. Hence, make sure to
    /// include the signature fields all other fields for
    /// re-serialization:
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectCreateRequest {
    /// Size of the object to create in bytes
    pub object_size: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum APIError<'desc, 'resp_body> {
    /// An unexpected, internal server error
    ///
    /// An unexpected internal server error occurred. The inner
    /// description may contain more information about the
    /// error. Please submit a bug report when encountering this
    /// error.
    InternalServerError { description: Cow<'desc, str> },

    /// We are unable to parse the error response
    InvalidErrorResponse {
        /// The HTTP error code provided by the coordinator
        ///
        /// The error code should never exceed a 3-digit stricly
        /// positive integer. We wrap it into an Option<NonZeroU16>
        /// nonetheless to capture the case whether the request has a
        /// malformed error code.
        code: Option<NonZeroU16>,

        /// The HTTP response body represented as a byte slice
        ///
        /// This is not represented as a string as it might not be
        /// valid UTF-8.
        resp_body: Option<Cow<'resp_body, [u8]>>,
    },
}
