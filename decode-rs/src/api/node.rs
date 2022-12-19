use std::borrow::Cow;
use std::num::NonZeroU16;

use log;

use serde::{Deserialize, Serialize};
use serde_json;

use super::coord as coord_api;

/// Hard-coded JSON-encoded fallback error for when the encoding of
/// [`APIError`] fails.
const FALLBACK_API_ERROR: &'static str = "{\
    \"type\":\"internal_server_error\",\
    \"description\":\"\
        The encoding of a prior error failed. This is a fallback error. Please \
        report this bug.\
    \"}\
";

#[derive(Clone)]
pub struct ResponseBody<'a>(pub Cow<'a, [u8]>);
impl std::fmt::Debug for ResponseBody<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("ResponseBody")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum APIError<'desc, 'resp_body> {
    /// Shard attempted to upload is too large for the node server
    ///
    /// The `max_bytes` field will hint at how many bytes the node is
    /// willing to accept in a single shard currently.
    ShardTooLarge {
        max_bytes: u64,
        #[serde(borrow)]
        description: Cow<'desc, str>,
    },

    /// The requested resource could not be found
    ResourceNotFound {
        #[serde(borrow)]
        description: Cow<'desc, str>,
    },

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

    /// We are unable to parse the error response
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
    pub fn serialize_json(&self) -> String {
        // Serializing the error may fail. In this case, we provide a
        // fallback, preencoded string.
        serde_json::to_string(self).unwrap_or_else(|err| {
            log::error!(
                "Error occurred while JSON-encoding APIError instance: {:?}, {:?}",
                self,
                err
            );
            FALLBACK_API_ERROR.to_string()
        })
    }

    pub fn http_status_code(&self) -> Option<NonZeroU16> {
        match self {
            // 413: Payload Too Large
            APIError::ShardTooLarge { .. } => Some(NonZeroU16::new(413).unwrap()),

            // 404: Not Found
            APIError::ResourceNotFound { .. } => Some(NonZeroU16::new(404).unwrap()),

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
            APIError::ShardTooLarge {
                max_bytes,
                description,
            } => APIError::ShardTooLarge {
                max_bytes,
                description: Cow::Owned(description.into_owned()),
            },

            APIError::ResourceNotFound { description } => APIError::ResourceNotFound {
                description: Cow::Owned(description.into_owned()),
            },

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

#[test]
fn test_fallback_api_error_deserialize() {
    println!(
        "Attempting to deserialize fallback API error string: {}",
        FALLBACK_API_ERROR
    );

    let err: APIError =
        serde_json::from_str(FALLBACK_API_ERROR).expect("Failed to parse fallback API error!");

    match err {
        // Expect it to be deserialized towards an InternalServerError
        APIError::InternalServerError { .. } => (),

        // In all other cases, fail the test
        _ => panic!("Fallback API error deserialized to unexpected variant!"),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo<'digest> {
    /// Hex-encoded SHA3-256 digest of the uploaded shard
    #[serde(borrow)]
    pub digest: Cow<'digest, str>,

    /// Shard size in bytes
    pub size: u64,
}

impl ShardInfo<'_> {
    pub fn into_owned(self) -> ShardInfo<'static> {
        ShardInfo {
            digest: Cow::Owned(self.digest.into_owned()),
            size: self.size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardUploadReceipt<'digest, 'receipt> {
    /// Hex-encoded SHA3-256 digest of the uploaded shard
    #[serde(borrow)]
    pub digest: Cow<'digest, str>,

    /// Opaque (digitally signed) receipt to confirm that this shard
    /// has been uploaded to the node.
    pub receipt: Cow<'receipt, str>,
}

impl ShardUploadReceipt<'_, '_> {
    pub fn into_owned(self) -> ShardUploadReceipt<'static, 'static> {
        ShardUploadReceipt {
            digest: Cow::Owned(self.digest.into_owned()),
            receipt: Cow::Owned(self.receipt.into_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ShardUploadResponse<'digest, 'receipt, 'desc, 'resp_body> {
    /// The shard was uploaded successfully, we are returned some
    /// metadata.
    Success(ShardUploadReceipt<'digest, 'receipt>),

    /// An error was returned in response to the shard upload
    /// request. The error has been deserialized into the
    /// corresponding field.
    APIError(APIError<'desc, 'resp_body>),
}

impl<'digest, 'receipt, 'desc, 'resp_body>
    ShardUploadResponse<'digest, 'receipt, 'desc, 'resp_body>
{
    pub fn from_bytes<'a: 'digest + 'receipt + 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        // Try to parse as success type first:
        serde_json::from_slice::<'a, ShardUploadReceipt>(bytes)
            .map(ShardUploadResponse::Success)
            .or_else(|_| {
                serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
                    .map(ShardUploadResponse::APIError)
            })
            .unwrap_or_else(|_| {
                ShardUploadResponse::APIError(APIError::InvalidResponse {
                    status: None,
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            })
    }

    pub fn from_http_resp<'a: 'digest + 'receipt + 'desc + 'resp_body>(
        status: NonZeroU16,
        bytes: &'a [u8],
    ) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        // When we've gotten a parsed success and/or error, check that
        // the expected HTTP response code matches the passed one. If
        // not, return an [`APIError::InvalidResponse`].
        let expected_status = match parsed {
            ShardUploadResponse::Success(_) => Some(NonZeroU16::new(200).unwrap()),
            ShardUploadResponse::APIError(ref err) => err.http_status_code(),
        };

        if let Some(expected_status_code) = expected_status {
            if expected_status_code != status {
                ShardUploadResponse::APIError(APIError::InvalidResponse {
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
            if let ShardUploadResponse::APIError(ref mut api_err) = &mut parsed {
                api_err.set_invalid_response_status_code(status);
            }
            parsed
        }
    }

    pub fn into_owned(self) -> ShardUploadResponse<'static, 'static, 'static, 'static> {
        match self {
            ShardUploadResponse::Success(object_upload_map) => {
                ShardUploadResponse::Success(object_upload_map.into_owned())
            }
            ShardUploadResponse::APIError(api_error) => {
                ShardUploadResponse::APIError(api_error.into_owned())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardRetrievalError<'desc, 'resp_body>(pub APIError<'desc, 'resp_body>);

impl<'desc, 'resp_body> ShardRetrievalError<'desc, 'resp_body> {
    pub fn from_bytes<'a: 'desc + 'resp_body>(bytes: &'a [u8]) -> Self {
        serde_json::from_slice::<'a, APIError<'a, 'a>>(bytes)
            .map(ShardRetrievalError)
            .unwrap_or_else(|_| {
                ShardRetrievalError(APIError::InvalidResponse {
                    status: None,
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            })
    }

    pub fn from_http_resp<'a: 'desc + 'resp_body>(status: NonZeroU16, bytes: &'a [u8]) -> Self {
        // Use the regular `from_bytes` to parse:
        let mut parsed = Self::from_bytes(bytes);

        if let Some(expected_status_code) = parsed.0.http_status_code() {
            if expected_status_code != status {
                ShardRetrievalError(APIError::InvalidResponse {
                    status: Some(status),
                    resp_body: Some(ResponseBody(Cow::Borrowed(bytes))),
                })
            } else {
                // Parsing worked, code matches
                parsed
            }
        } else {
            // Parsing did not yield an error with an expected status, pass the
            // parsed result (which likely is a [`APIError::InvalidResponse`])
            // through and set the actual received status:
            parsed.0.set_invalid_response_status_code(status);
            parsed
        }
    }

    pub fn into_owned(self) -> ShardRetrievalError<'static, 'static> {
        ShardRetrievalError(self.0.into_owned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatistics {
    pub load_avg: f32,
    pub disk_capacity: u64,
    pub disk_free: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardFetchRequest<'a> {
    /// Node URL to fetch this shard from
    #[serde(borrow)]
    pub source_node: Cow<'a, str>,

    /// Ticket to present the node to fetch the shard from (download
    /// authorization)
    #[serde(borrow)]
    pub ticket: Cow<'a, str>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardReconstructRequest {
    /// Size of the entire object in bytes
    pub chunk_size: u64,

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
    pub shard_map: Vec<coord_api::ObjectRetrievalShardSpec>,

    /// Mapping from node indices to node base URLs (excluding the /v0
    /// API version subpath):
    pub node_map: Vec<String>,
}
