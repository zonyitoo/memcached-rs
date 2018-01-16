// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

//! Memcached protocol

use std::collections::BTreeMap;
use std::convert::From;
use std::error;
use std::fmt::{self, Display};
use std::io;

use semver::Version;

use byteorder;

pub use self::binary::BinaryProto;

mod binarydef;
pub mod binary;

/// Protocol type
#[derive(Copy, Clone)]
pub enum ProtoType {
    Binary,
}

#[derive(Debug)]
pub enum Error {
    BinaryProtoError(binary::Error),
    IoError(io::Error),
    OtherError {
        desc: &'static str,
        detail: Option<String>,
    },
}

pub type MemCachedResult<T> = Result<T, Error>;

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::BinaryProtoError(ref err) => err.description(),
            &Error::IoError(ref err) => err.description(),
            &Error::OtherError { desc, .. } => desc,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::BinaryProtoError(ref err) => err.fmt(f),
            &Error::IoError(ref err) => err.fmt(f),
            &Error::OtherError { desc, ref detail } => {
                write!(f, "{}", desc)?;
                match detail {
                    &Some(ref s) => write!(f, " ({})", s),
                    &None => Ok(()),
                }
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<binary::Error> for Error {
    fn from(err: binary::Error) -> Error {
        Error::BinaryProtoError(err)
    }
}

impl From<byteorder::Error> for Error {
    fn from(err: byteorder::Error) -> Error {
        Error::IoError(From::from(err))
    }
}

pub trait Proto
    : Operation + MultiOperation + ServerOperation + NoReplyOperation + CasOperation
    {
    // fn clone(&self) -> Box<Proto + Send>;
}

impl<T> Proto for T
where
    T: Operation + MultiOperation + ServerOperation + NoReplyOperation + CasOperation,
{
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
    fn increment_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)>;
    fn decrement_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)>;
    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64>;
    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64>;
    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> MemCachedResult<u64>;
}

pub trait ServerOperation {
    fn quit(&mut self) -> MemCachedResult<()>;
    fn flush(&mut self, expiration: u32) -> MemCachedResult<()>;
    fn noop(&mut self) -> MemCachedResult<()>;
    fn version(&mut self) -> MemCachedResult<Version>;
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

#[derive(Debug)]
pub enum AuthResponse {
    Continue(Vec<u8>),
    Succeeded,
    Failed,
}

pub trait AuthOperation {
    fn list_mechanisms(&mut self) -> MemCachedResult<Vec<String>>;
    fn auth_start(&mut self, mech: &str, init: &[u8]) -> MemCachedResult<AuthResponse>;
    fn auth_continue(&mut self, mech: &str, data: &[u8]) -> MemCachedResult<AuthResponse>;
}
