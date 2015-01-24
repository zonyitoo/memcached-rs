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

//! Memcached protocol

use std::fmt::{Display, Formatter, self};
use std::collections::BTreeMap;
use std::io;

use version;

pub use self::binary::BinaryProto;

mod binarydef;
pub mod binary;

/// Protocol type
#[derive(Copy)]
pub enum ProtoType {
    Binary,
}

#[derive(Clone, Copy, Debug)]
pub enum ErrorKind {
    BinaryProtoError(binarydef::Status),
    IoError(io::IoErrorKind),
    OtherError,
}

pub type MemCachedResult<T> = Result<T, Error>;

#[derive(Clone, Debug)]
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

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.detail {
            Some(ref detail) => write!(f, "{}", detail),
            None => write!(f, "{}", self.desc),
        }
    }
}

pub trait Proto: Operation + MultiOperation + ServerOperation + NoReplyOperation + CasOperation {
    fn clone(&self) -> Box<Proto + Send>;
}

pub trait Operation {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn delete(&mut self, key: &[u8]) -> MemCachedResult<()>;
    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn get(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32)>;
    fn getk(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32)>;
    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64>;
    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64>;
    fn append(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()>;
    fn prepend(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()>;
    fn touch(&mut self, key: &[u8], expiration: u32) -> MemCachedResult<()>;
}

pub trait CasOperation {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64>;
    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<u64>;
    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64>;
    fn get_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32, u64)>;
    fn getk_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32, u64)>;
    fn increment_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
        -> MemCachedResult<(u64, u64)>;
    fn decrement_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
        -> MemCachedResult<(u64, u64)>;
    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64>;
    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64>;
    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> MemCachedResult<u64>;
}

pub trait ServerOperation {
    fn quit(&mut self) -> MemCachedResult<()>;
    fn flush(&mut self, expiration: u32) -> MemCachedResult<()>;
    fn noop(&mut self) -> MemCachedResult<()>;
    fn version(&mut self) -> MemCachedResult<version::Version>;
    fn stat(&mut self) -> MemCachedResult<BTreeMap<String, String>>;
}

pub trait MultiOperation {
    fn set_multi(&mut self, kv: BTreeMap<&[u8], (&[u8], u32, u32)>) -> MemCachedResult<()>;
    fn delete_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<()>;
    fn get_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<BTreeMap<Vec<u8>, (Vec<u8>, u32)>>;
}

pub trait NoReplyOperation {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn delete_noreply(&mut self, key: &[u8]) -> MemCachedResult<()>;
    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()>;
    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()>;
    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()>;
    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()>;
    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()>;
}
