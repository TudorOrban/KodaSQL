use serde::{Deserialize, Serialize};

use crate::shared::errors::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub message_type: MessageType,
    pub sql: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub status: ResponseStatus,
    pub data: Option<String>,
    pub error: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MessageType {
    Query,
    Command,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseStatus {
    Success,
    Error,
}

impl From<Error> for Response {
    fn from(error: Error) -> Self {
        Response {
            status: ResponseStatus::Error,
            data: None,
            error: Some(format!("{}", error)),
        }
    }
}