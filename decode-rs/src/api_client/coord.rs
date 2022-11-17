use std::num::NonZeroU16;

use reqwest;

use crate::api::coord as coord_api;
use crate::api::node as node_api;

#[derive(Debug, Clone)]
pub enum CoordAPIClientError {
    /// A timeout occured while performing the request
    Timeout,

    /// Unable to connect to the remote host
    Connect,

    // TODO: Don't expose this generic error type. All API errors
    // should be converted into specific variants of this generic
    // client error enum type, or specific error enums of the various
    // request we support.
    APIError(coord_api::APIError<'static, 'static>),

    /// Unknown error
    Unknown,
}

impl CoordAPIClientError {
    fn from_reqwest_generic_err(err: reqwest::Error) -> Result<Self, reqwest::Error> {
        if err.is_timeout() {
            Ok(CoordAPIClientError::Timeout)
        } else if err.is_connect() {
            Ok(CoordAPIClientError::Connect)
        } else {
            Err(err)
        }
    }
}

#[derive(Debug, Clone)]
pub enum CoordAPIUploadError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(CoordAPIClientError),
}

impl CoordAPIUploadError {
    fn from_reqwest_error(err: reqwest::Error) -> CoordAPIUploadError {
        // Handle the generic reqwest error cases first:
        match CoordAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => CoordAPIUploadError::MiscError(generic_err),
            Err(_err) => {
                // There should be no upload-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                CoordAPIUploadError::MiscError(CoordAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: coord_api::APIError<'desc, 'resp_body>,
    ) -> CoordAPIUploadError {
        CoordAPIUploadError::MiscError(CoordAPIClientError::APIError(apierr.into_owned()))
    }
}

#[derive(Debug, Clone)]
pub enum CoordAPIObjectFinalizeError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(CoordAPIClientError),
}

impl CoordAPIObjectFinalizeError {
    fn from_reqwest_error(err: reqwest::Error) -> CoordAPIObjectFinalizeError {
        // Handle the generic reqwest error cases first:
        match CoordAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => CoordAPIObjectFinalizeError::MiscError(generic_err),
            Err(_err) => {
                // There should be no finalize-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                CoordAPIObjectFinalizeError::MiscError(CoordAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: coord_api::APIError<'desc, 'resp_body>,
    ) -> CoordAPIObjectFinalizeError {
        CoordAPIObjectFinalizeError::MiscError(CoordAPIClientError::APIError(apierr.into_owned()))
    }
}

#[derive(Debug, Clone)]
pub enum CoordAPIObjectRetrievalError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(CoordAPIClientError),
}

impl CoordAPIObjectRetrievalError {
    fn from_reqwest_error(err: reqwest::Error) -> CoordAPIObjectRetrievalError {
        // Handle the generic reqwest error cases first:
        match CoordAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => CoordAPIObjectRetrievalError::MiscError(generic_err),
            Err(_err) => {
                // There should be no finalize-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                CoordAPIObjectRetrievalError::MiscError(CoordAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: coord_api::APIError<'desc, 'resp_body>,
    ) -> CoordAPIObjectRetrievalError {
        CoordAPIObjectRetrievalError::MiscError(CoordAPIClientError::APIError(apierr.into_owned()))
    }
}

#[derive(Debug, Clone)]
pub enum CoordAPIRegisterError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(CoordAPIClientError),
}

impl CoordAPIRegisterError {
    fn from_reqwest_error(err: reqwest::Error) -> CoordAPIRegisterError {
        // Handle the generic reqwest error cases first:
        match CoordAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => CoordAPIRegisterError::MiscError(generic_err),
            Err(_err) => {
                // There should be no upload-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                CoordAPIRegisterError::MiscError(CoordAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: coord_api::APIError<'desc, 'resp_body>,
    ) -> CoordAPIRegisterError {
        CoordAPIRegisterError::MiscError(CoordAPIClientError::APIError(apierr.into_owned()))
    }
}

pub struct CoordAPIClient {
    http_client: reqwest::Client,
    base_url: reqwest::Url,
}

impl CoordAPIClient {
    pub fn new(base_url: reqwest::Url) -> Self {
        CoordAPIClient {
            http_client: reqwest::Client::new(),
            base_url,
        }
    }

    pub async fn register_node<'a>(
        &self,
        node_id: &uuid::Uuid,
        node_url: &reqwest::Url,
        shard_digests: Vec<String>,
    ) -> Result<(), CoordAPIRegisterError> {
        let request_body = coord_api::NodeRegisterRequest {
            node_url: node_url.to_string(),
            shards: shard_digests,
        };

        let resp = self
            .http_client
            .put(
                self.base_url
                    .join("/v0/node/")
                    .and_then(|url| url.join(&node_id.to_string()))
                    .expect("Failed to construct node register URL"),
            )
            .json(&request_body)
            .send()
            .await
            .map_err(CoordAPIRegisterError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        let bytes = resp
            .bytes()
            .await
            .map_err(CoordAPIRegisterError::from_reqwest_error)?;
        let parsed = coord_api::NodeRegisterResponse::from_http_resp(status, bytes.as_ref());

        match parsed {
            coord_api::NodeRegisterResponse::Success => Ok(()),
            coord_api::NodeRegisterResponse::APIError(api_err) => {
                Err(CoordAPIRegisterError::from_api_error(api_err))
            }
        }
    }

    /// TODO: wrap the ObjectUploadMap type in something better suited
    /// for further processing.
    pub async fn upload_object(
        &self,
        object_size: u64,
    ) -> Result<coord_api::ObjectUploadMap, CoordAPIUploadError> {
        let request_body = coord_api::ObjectCreateRequest { object_size };

        let resp = self
            .http_client
            .post(
                self.base_url
                    .join("/v0/object")
                    .expect("Failed to construct object upload URL"),
            )
            .json(&request_body)
            .send()
            .await
            .map_err(CoordAPIUploadError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        let bytes = resp
            .bytes()
            .await
            .map_err(CoordAPIUploadError::from_reqwest_error)?;
        let parsed = coord_api::ObjectCreateResponse::from_http_resp(status, bytes.as_ref());

        match parsed {
            coord_api::ObjectCreateResponse::Success(object_upload_map) => Ok(object_upload_map),
            coord_api::ObjectCreateResponse::APIError(api_err) => {
                Err(CoordAPIUploadError::from_api_error(api_err))
            }
        }
    }

    pub async fn finalize_object<'digest, 'receipt>(
        &self,
        object_upload_map: coord_api::ObjectUploadMap,
        upload_results: Vec<Vec<node_api::ShardUploadReceipt<'digest, 'receipt>>>,
    ) -> Result<(), CoordAPIObjectFinalizeError> {
        let request_body = coord_api::ObjectFinalizeRequest {
            object_upload_map,
            upload_results,
        };

        let resp = self
            .http_client
            .put(
                self.base_url
                    .join("/v0/object/")
                    .and_then(|url| {
                        url.join(&format!(
                            "{}/",
                            request_body.object_upload_map.object_id.to_string()
                        ))
                    })
                    .and_then(|url| url.join("finalize"))
                    .expect("Failed to construct object finalize URL"),
            )
            .json(&request_body)
            .send()
            .await
            .map_err(CoordAPIObjectFinalizeError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        let bytes = resp
            .bytes()
            .await
            .map_err(CoordAPIObjectFinalizeError::from_reqwest_error)?;
        let parsed = coord_api::ObjectFinalizeResponse::from_http_resp(status, bytes.as_ref());

        match parsed {
            coord_api::ObjectFinalizeResponse::Success => Ok(()),
            coord_api::ObjectFinalizeResponse::APIError(api_err) => {
                Err(CoordAPIObjectFinalizeError::from_api_error(api_err))
            }
        }
    }

    pub async fn get_object<'digest>(
        &self,
        object_id: uuid::Uuid,
    ) -> Result<coord_api::ObjectRetrievalMap, CoordAPIObjectRetrievalError> {
        let resp = self
            .http_client
            .get(
                self.base_url
                    .join("/v0/object/")
                    .and_then(|url| url.join(&object_id.to_string()))
                    .expect("Failed to construct object retrieval URL"),
            )
            .send()
            .await
            .map_err(CoordAPIObjectRetrievalError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        let bytes = resp
            .bytes()
            .await
            .map_err(CoordAPIObjectRetrievalError::from_reqwest_error)?;
        let parsed = coord_api::ObjectRetrievalResponse::from_http_resp(status, bytes.as_ref());

        match parsed {
            coord_api::ObjectRetrievalResponse::Success(object_retrieval_map) => {
                Ok(object_retrieval_map)
            }
            coord_api::ObjectRetrievalResponse::APIError(api_err) => {
                Err(CoordAPIObjectRetrievalError::from_api_error(api_err))
            }
        }
    }
}
