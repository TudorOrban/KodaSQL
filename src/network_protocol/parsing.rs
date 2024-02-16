use crate::shared::errors::Error;

use super::types::{Request, Response};
use bincode;

pub fn parse_request(data: &[u8]) -> Result<Request, Error> {
    bincode::deserialize(data).map_err(|_| Error::UnknownError)
}

pub fn format_response(response: &Response) -> Vec<u8> {
    bincode::serialize(response).unwrap_or_else(|_| vec![])
}