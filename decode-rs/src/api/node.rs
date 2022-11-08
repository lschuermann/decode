use std::borrow::Cow;
use std::num::NonZeroU16;

use log;

use serde::{Deserialize, Serialize};
use serde_json;

/// Hard-coded JSON-encoded fallback error for when the encoding of
/// [`APIError`] fails.
const FALLBACK_API_ERROR: &'static str = "{\
    \"type\":\"internal_server_error\",\
    \"description\":\"\
        The encoding of a prior error failed. This is a fallback error. Please \
        report this bug.\
    \"}\
";

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
        description: Cow<'desc, str>,
    },

    /// The requested resource could not be found
    ResourceNotFound { description: Cow<'desc, str> },

    /// An unexpected, internal server error
    ///
    /// An unexpected internal server error occurred. The inner
    /// description may contain more information about the
    /// error. Please submit a bug report when encountering this
    /// error.
    InternalServerError { description: Cow<'desc, str> },

    /// We are unable to parse the error response
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
        resp_body: Option<Cow<'resp_body, [u8]>>,
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
                resp_body: resp_body.map(|b| Cow::Owned(b.into_owned())),
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
