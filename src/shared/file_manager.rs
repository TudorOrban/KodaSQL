use std::{fs::{self, File}, io::{BufReader, Read}};

use serde::{de::DeserializeOwned, Serialize};

use super::errors::Error;

pub fn read_file_to_string(file_path: &String) -> Result<String, Error> {
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).map_err(|e| Error::IOError(e))?;

    Ok(contents)
}

pub fn read_json_file<T: DeserializeOwned>(file_path: &String) -> Result<T, Error> {
    let content_string = read_file_to_string(file_path)?;
    let content: T = serde_json::from_str(&content_string).map_err(|e| Error::SerdeJsonError(e))?;

    Ok(content)
}

pub fn write_string_into_file(file_path: &String, contents: &String) -> Result<(), Error> {
    fs::write(file_path, contents).map_err(|e| Error::IOError(e))?;

    Ok(())
}

pub fn write_json_into_file<T: Serialize>(file_path: &String, contents: &T) -> Result<(), Error> {
    let contents_string = serde_json::to_string(contents).map_err(|e| Error::SerdeJsonError(e))?;
    write_string_into_file(file_path, &contents_string)?;

    Ok(())
}