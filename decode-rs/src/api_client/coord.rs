use std::num::NonZeroU16;

use reqwest;

use crate::api::coord as coord_api;

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
    fn from_reqwest_err(err: reqwest::Error) -> CoordAPIUploadError {
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

    async fn from_error_response(resp: reqwest::Response) -> CoordAPIUploadError {
	let resp_status = resp.status();

	// TODO: fix the error handling
	let apierr = match resp
	    .json::<coord_api::APIError>()
	    .await {
		Err(_reqwerr) =>
		    coord_api::APIError::InvalidErrorResponse {
			code: NonZeroU16::new(resp_status.as_u16()),
			// resp.bytes().await.map(|b| Cow::Owned(b.to_vec())).ok(),
			resp_body: None,
		    },
		Ok(apierr) => apierr,
	    };

	// TODO: further parse the API error and convert it into the
	// CoordAPIUploadError

	CoordAPIUploadError::MiscError(CoordAPIClientError::APIError(apierr))
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

    /// TODO: wrap the ObjectUploadMap type in something better suited
    /// for further processing.
    pub async fn upload_object(&self, object_size: u64) -> Result<coord_api::ObjectUploadMap, CoordAPIUploadError> {
	let request_body = coord_api::ObjectCreateRequest {
	    object_size,
	};

	match
	    self.http_client.post(self.base_url.join("/v0/object/upload").expect("Failed to construct object upload URL"))
	    .json(&request_body)
	    .send()
	    .await {
		Err(reqwerr) => Err(CoordAPIUploadError::from_reqwest_err(reqwerr)),
		Ok(resp) => {
		    // Try to parse the response. TODO: this doesn't
		    // actually allow us to recover the original error
		    // response. Refactor!
		    resp.json::<coord_api::ObjectUploadMap>().await.map_err(|_err| {
			println!("Failed to parse upload body");
			CoordAPIUploadError::MiscError(CoordAPIClientError::Unknown)
		    })
		},
	    }
    }
}
