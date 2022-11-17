use std::borrow::Cow;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use uuid::Uuid;

use super::node as node_api;

#[derive(Clone)]
pub struct ResponseBody<'a>(pub Cow<'a, [u8]>);
impl std::fmt::Debug for ResponseBody<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("ResponseBody")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
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
    InternalServerError {
        #[serde(borrow)]
        description: Cow<'desc, str>,
    },

    /// We are unable to parse the response
    #[serde(skip)]
    InvalidResponse {
        /// The HTTP error code provided by the coordinator
        ///
        /// The error code should never exceed a 3-digit stricly
        /// positive integer. We wrap it into an Option<NonZeroU16>
        /// nonetheless to capture the case whether the request has a
        /// malformed error code.
        status: Option<NonZeroU16>,

        /// The HTTP response body represented as a byte slice
        ///
        /// This is not represented as a string as it might not be
        /// valid UTF-8.
        resp_body: Option<ResponseBody<'resp_body>>,
    },
}

impl<'desc, 'resp_body> APIError<'desc, 'resp_body> {
    pub fn http_status_code(&self) -> Option<NonZeroU16> {
        match self {
            // 500: Internal Server Error
            APIError::InternalServerError { .. } => Some(NonZeroU16::new(500).unwrap()),

            // InvalidResponse does not have an associated HTTP status code.
            APIError::InvalidResponse { .. } => None,
        }
    }

    pub fn set_invalid_response_status_code(&mut self, new_status: NonZeroU16) {
        if let APIError::InvalidResponse { ref mut status, .. } = self {
            *status = Some(new_status);
        }
    }

    pub fn into_owned(self) -> APIError<'static, 'static> {
        match self {
            APIError::InternalServerError { description } => APIError::InternalServerError {
                description: Cow::Owned(description.into_owned()),
            },
            APIError::InvalidResponse { status, resp_body } => APIError::InvalidResponse {
                status,
                resp_body: resp_body.map(|b| ResponseBody(Cow::Owned(b.0.into_owned()))),
            },
        }
    }
}

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

#[derive(Debug, Clone)]
pub enum ObjectRetrievalResponse<'desc, 'resp_body> {
    /// Object retrieve request was successfull, the client is provided
    /// with an object retrieval map.
    Success(ObjectRetrievalMap),
    /// An error was returned in response to the object retrieve
    /// request. The error has been deserialized into the
    /// corresponding field.
    APIError(APIError<'desc, 'resp_body>),
}

impl<'desc, 'resp_body> ObjectRetrievalResponse<'desc, 'resp_body> {
    pub fn from_bytes<'a: 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        // Try to parse as success type first:
        serde_json::from_slice::<'a, ObjectRetrievalMap>(bytes)
            .map(ObjectRetrievalResponse::Success)
            .or_else(|_| {
                serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
                    .map(ObjectRetrievalResponse::APIError)
            })
            .unwrap_or_else(|_| {
                ObjectRetrievalResponse::APIError(APIError::InvalidResponse {
                    status: None,
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            })
    }

    pub fn from_http_resp<'a: 'desc + 'resp_body>(status: NonZeroU16, bytes: &'a [u8]) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        // When we've gotten a parsed success and/or error, check that
        // the expected HTTP response code matches the passed one. If
        // not, return an [`APIError::InvalidResponse`].
        let expected_status = match parsed {
            ObjectRetrievalResponse::Success(_) => Some(NonZeroU16::new(200).unwrap()),
            ObjectRetrievalResponse::APIError(ref err) => err.http_status_code(),
        };

        if let Some(expected_status_code) = expected_status {
            if expected_status_code != status {
                ObjectRetrievalResponse::APIError(APIError::InvalidResponse {
                    status: Some(status),
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            } else {
                // Parsing worked, code matches
                parsed
            }
        } else {
            // Parsing did not yield a success variant OR an error
            // with an expected status, pass the parsed result (which
            // likely is a [`APIError::InvalidResponse`])
            // through and set the actual received status:
            if let ObjectRetrievalResponse::APIError(ref mut api_err) = &mut parsed {
                api_err.set_invalid_response_status_code(status);
            }
            parsed
        }
    }

    pub fn into_owned(self) -> ObjectRetrievalResponse<'static, 'static> {
        match self {
            ObjectRetrievalResponse::Success(object_retrieval_map) => {
                ObjectRetrievalResponse::Success(object_retrieval_map)
            }
            ObjectRetrievalResponse::APIError(api_error) => {
                ObjectRetrievalResponse::APIError(api_error.into_owned())
            }
        }
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

#[derive(Debug, Clone)]
pub enum ObjectCreateResponse<'desc, 'resp_body> {
    /// Object create request was successfull, the client is provided
    /// with an object upload map.
    Success(ObjectUploadMap),
    /// An error was returned in response to the object create
    /// request. The error has been deserialized into the
    /// corresponding field.
    APIError(APIError<'desc, 'resp_body>),
}

impl<'desc, 'resp_body> ObjectCreateResponse<'desc, 'resp_body> {
    pub fn from_bytes<'a: 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        // Try to parse as success type first:
        serde_json::from_slice::<'a, ObjectUploadMap>(bytes)
            .map(ObjectCreateResponse::Success)
            .or_else(|_| {
                serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
                    .map(ObjectCreateResponse::APIError)
            })
            .unwrap_or_else(|_| {
                ObjectCreateResponse::APIError(APIError::InvalidResponse {
                    status: None,
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            })
    }

    pub fn from_http_resp<'a: 'desc + 'resp_body>(status: NonZeroU16, bytes: &'a [u8]) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        // When we've gotten a parsed success and/or error, check that
        // the expected HTTP response code matches the passed one. If
        // not, return an [`APIError::InvalidResponse`].
        let expected_status = match parsed {
            ObjectCreateResponse::Success(_) => Some(NonZeroU16::new(200).unwrap()),
            ObjectCreateResponse::APIError(ref err) => err.http_status_code(),
        };

        if let Some(expected_status_code) = expected_status {
            if expected_status_code != status {
                ObjectCreateResponse::APIError(APIError::InvalidResponse {
                    status: Some(status),
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            } else {
                // Parsing worked, code matches
                parsed
            }
        } else {
            // Parsing did not yield a success variant OR an error
            // with an expected status, pass the parsed result (which
            // likely is a [`APIError::InvalidResponse`])
            // through and set the actual received status:
            if let ObjectCreateResponse::APIError(ref mut api_err) = &mut parsed {
                api_err.set_invalid_response_status_code(status);
            }
            parsed
        }
    }

    pub fn into_owned(self) -> ObjectCreateResponse<'static, 'static> {
        match self {
            ObjectCreateResponse::Success(object_upload_map) => {
                ObjectCreateResponse::Success(object_upload_map)
            }
            ObjectCreateResponse::APIError(api_error) => {
                ObjectCreateResponse::APIError(api_error.into_owned())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectFinalizeRequest<'digest, 'receipt> {
    /// The upload map originally returned by the coordinator for this request
    pub object_upload_map: ObjectUploadMap,

    /// The collection of receipts returned by the node
    pub upload_results: Vec<Vec<node_api::ShardUploadReceipt<'digest, 'receipt>>>,
}

#[derive(Debug, Clone)]
pub enum ObjectFinalizeResponse<'desc, 'resp_body> {
    /// Object create request was successfull, the client is provided
    /// with an object upload map.
    Success,
    /// An error was returned in response to the object create
    /// request. The error has been deserialized into the
    /// corresponding field.
    APIError(APIError<'desc, 'resp_body>),
}

impl<'desc, 'resp_body> ObjectFinalizeResponse<'desc, 'resp_body> {
    pub fn from_bytes<'a: 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        // Success (200) response should have an empty payload:
        if bytes.len() == 0 {
            ObjectFinalizeResponse::Success
        } else {
            // Try to parse as an error response, fall back to InvalidResponse
            // otherwise:
            serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
                .map(ObjectFinalizeResponse::APIError)
                .unwrap_or_else(|_| {
                    ObjectFinalizeResponse::APIError(APIError::InvalidResponse {
                        status: None,
                        resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                    })
                })
        }
    }

    pub fn from_http_resp<'a: 'desc + 'resp_body>(status: NonZeroU16, bytes: &'a [u8]) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        // When we've gotten a parsed success and/or error, check that
        // the expected HTTP response code matches the passed one. If
        // not, return an [`APIError::InvalidResponse`].
        let expected_status = match parsed {
            ObjectFinalizeResponse::Success => Some(NonZeroU16::new(200).unwrap()),
            ObjectFinalizeResponse::APIError(ref err) => err.http_status_code(),
        };

        if let Some(expected_status_code) = expected_status {
            if expected_status_code != status {
                ObjectFinalizeResponse::APIError(APIError::InvalidResponse {
                    status: Some(status),
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            } else {
                // Parsing worked, code matches
                parsed
            }
        } else {
            // Parsing did not yield a success variant OR an error
            // with an expected status, pass the parsed result (which
            // likely is a [`APIError::InvalidResponse`])
            // through and set the actual received status:
            if let ObjectFinalizeResponse::APIError(ref mut api_err) = &mut parsed {
                api_err.set_invalid_response_status_code(status);
            }
            parsed
        }
    }

    pub fn into_owned(self) -> ObjectFinalizeResponse<'static, 'static> {
        match self {
            ObjectFinalizeResponse::Success => ObjectFinalizeResponse::Success,
            ObjectFinalizeResponse::APIError(api_error) => {
                ObjectFinalizeResponse::APIError(api_error.into_owned())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeRegisterRequest {
    /// Base URL of the node attempting to register
    pub node_url: String,

    /// List of shard digests held at this node
    pub shards: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum NodeRegisterResponse<'desc, 'resp_body> {
    /// Object create request was successfull, the client is provided
    /// with an object upload map.
    Success,
    /// An error was returned in response to the object create
    /// request. The error has been deserialized into the
    /// corresponding field.
    APIError(APIError<'desc, 'resp_body>),
}

impl<'desc, 'resp_body> NodeRegisterResponse<'desc, 'resp_body> {
    pub fn from_bytes<'a: 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        // Success (200) response should have an empty payload:
        if bytes.len() == 0 {
            NodeRegisterResponse::Success
        } else {
            // Try to parse as an error response, fall back to InvalidResponse
            // otherwise:
            serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
                .map(NodeRegisterResponse::APIError)
                .unwrap_or_else(|_| {
                    NodeRegisterResponse::APIError(APIError::InvalidResponse {
                        status: None,
                        resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                    })
                })
        }
    }

    pub fn from_http_resp<'a: 'desc + 'resp_body>(status: NonZeroU16, bytes: &'a [u8]) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        // When we've gotten a parsed success and/or error, check that
        // the expected HTTP response code matches the passed one. If
        // not, return an [`APIError::InvalidResponse`].
        let expected_status = match parsed {
            NodeRegisterResponse::Success => Some(NonZeroU16::new(200).unwrap()),
            NodeRegisterResponse::APIError(ref err) => err.http_status_code(),
        };

        if let Some(expected_status_code) = expected_status {
            if expected_status_code != status {
                NodeRegisterResponse::APIError(APIError::InvalidResponse {
                    status: Some(status),
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            } else {
                // Parsing worked, code matches
                parsed
            }
        } else {
            // Parsing did not yield a success variant OR an error
            // with an expected status, pass the parsed result (which
            // likely is a [`APIError::InvalidResponse`])
            // through and set the actual received status:
            if let NodeRegisterResponse::APIError(ref mut api_err) = &mut parsed {
                api_err.set_invalid_response_status_code(status);
            }
            parsed
        }
    }

    pub fn into_owned(self) -> NodeRegisterResponse<'static, 'static> {
        match self {
            NodeRegisterResponse::Success => NodeRegisterResponse::Success,
            NodeRegisterResponse::APIError(api_error) => {
                NodeRegisterResponse::APIError(api_error.into_owned())
            }
        }
    }
}
