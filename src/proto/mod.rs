// The MIT License (MIT)

// Copyright (c) 2014 Y. T. CHUNG <zonyitoo@gmail.com>

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::fmt::{Show, Formatter, mod};
use std::collections::TreeMap;
use std::io;

use version::Version;

pub use self::binary::BinaryProto;

mod binarydef;
mod binary;

#[deriving(Clone)]
pub enum ErrorKind {
    MemCachedError(binarydef::Status),
    IoError(io::IoErrorKind),
    OtherError,
}

#[deriving(Clone)]
pub struct Error {
    pub kind: ErrorKind,
    pub desc: &'static str,
    pub detail: Option<String>,
}

impl Error {
    pub fn new(kind: ErrorKind, desc: &'static str, detail: Option<String>) -> Error {
        Error {
            kind: kind,
            desc: desc,
            detail: detail,
        }
    }
}

impl Show for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.detail {
            Some(ref detail) => write!(f, "{}", detail),
            None => write!(f, "{}", self.desc),
        }
    }
}

pub trait Proto {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error>;
    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error>;
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error>;
    fn get(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32), Error>;
    fn getk(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32), Error>;
    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error>;
    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error>;
    fn quit(&mut self) -> Result<(), Error>;
    fn flush(&mut self, expiration: u32) -> Result<(), Error>;
    fn noop(&mut self) -> Result<(), Error>;
    fn version(&mut self) -> Result<Version, Error>;
    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn prepend(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn stat(&mut self) -> Result<TreeMap<String, String>, Error>;
}
