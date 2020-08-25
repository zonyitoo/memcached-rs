// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

//! This module is for serializing binary packet
//!
//! The protocol specification is defined in
//! [BinaryProtocolRevamped](https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped)
//!
// General format of a packet:
//
// Byte/     0       |       1       |       2       |       3       |
//    /              |               |               |               |
//   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//   +---------------+---------------+---------------+---------------+
//  0/ HEADER                                                        /
//   /                                                               /
//   /                                                               /
//   /                                                               /
//   +---------------+---------------+---------------+---------------+
// 24/ COMMAND-SPECIFIC EXTRAS (as needed)                           /
//  +/  (note length in the extras length header field)              /
//   +---------------+---------------+---------------+---------------+
//  m/ Key (as needed)                                               /
//  +/  (note length in key length header field)                     /
//   +---------------+---------------+---------------+---------------+
//  n/ Value (as needed)                                             /
//  +/  (note length is total body length header field, minus        /
//  +/   sum of the extras and key length body fields)               /
//   +---------------+---------------+---------------+---------------+
//   Total 24 bytes

#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]

use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Bytes, Buf, BytesMut};

#[rustfmt::skip]
mod consts {
    pub const MAGIC_REQUEST:  u8 = 0x80;
    pub const MAGIC_RESPONSE: u8 = 0x81;

    pub const STATUS_NO_ERROR:                             u16 = 0x0000;
    pub const STATUS_KEY_NOT_FOUND:                        u16 = 0x0001;
    pub const STATUS_KEY_EXISTS:                           u16 = 0x0002;
    pub const STATUS_VALUE_TOO_LARGE:                      u16 = 0x0003;
    pub const STATUS_INVALID_ARGUMENTS:                    u16 = 0x0004;
    pub const STATUS_ITEM_NOT_STORED:                      u16 = 0x0005;
    pub const STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE:    u16 = 0x0006;
    pub const STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER:      u16 = 0x0007;
    pub const STATUS_AUTHENTICATION_ERROR:                 u16 = 0x0008;
    pub const STATUS_AUTHENTICATION_CONTINUE:              u16 = 0x0009;
    pub const STATUS_UNKNOWN_COMMAND:                      u16 = 0x0081;
    pub const STATUS_OUT_OF_MEMORY:                        u16 = 0x0082;
    pub const STATUS_NOT_SUPPORTED:                        u16 = 0x0083;
    pub const STATUS_INTERNAL_ERROR:                       u16 = 0x0084;
    pub const STATUS_BUSY:                                 u16 = 0x0085;
    pub const STATUS_TEMPORARY_FAILURE:                    u16 = 0x0086;
    pub const STATUS_AUTHENTICATION_REQUIRED:              u16 = 0x0020;
    pub const STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED: u16 = 0x0021;

    pub const OPCODE_GET:                  u8 = 0x00;
    pub const OPCODE_SET:                  u8 = 0x01;
    pub const OPCODE_ADD:                  u8 = 0x02;
    pub const OPCODE_REPLACE:              u8 = 0x03;
    pub const OPCODE_DEL:                  u8 = 0x04;
    pub const OPCODE_INCR:                 u8 = 0x05;
    pub const OPCODE_DECR:                 u8 = 0x06;
    pub const OPCODE_QUIT:                 u8 = 0x07;
    pub const OPCODE_FLUSH:                u8 = 0x08;
    pub const OPCODE_GETQ:                 u8 = 0x09;
    pub const OPCODE_NOP:                  u8 = 0x0A;
    pub const OPCODE_VERSION:              u8 = 0x0B;
    pub const OPCODE_GETK:                 u8 = 0x0C;
    pub const OPCODE_GETKQ:                u8 = 0x0D;
    pub const OPCODE_APPEND:               u8 = 0x0E;
    pub const OPCODE_PREPEND:              u8 = 0x0F;
    pub const OPCODE_STAT:                 u8 = 0x10;
    pub const OPCODE_SETQ:                 u8 = 0x11;
    pub const OPCODE_ADDQ:                 u8 = 0x12;
    pub const OPCODE_REPLACEQ:             u8 = 0x13;
    pub const OPCODE_DELQ:                 u8 = 0x14;
    pub const OPCODE_INCRQ:                u8 = 0x15;
    pub const OPCODE_DECRQ:                u8 = 0x16;
    pub const OPCODE_QUITQ:                u8 = 0x17;
    pub const OPCODE_FLUSHQ:               u8 = 0x18;
    pub const OPCODE_APPENDQ:              u8 = 0x19;
    pub const OPCODE_PREPENDQ:             u8 = 0x1A;
    pub const OPCODE_VERBOSITY:            u8 = 0x1B;
    pub const OPCODE_TOUCH:                u8 = 0x1C;
    pub const OPCODE_GAT:                  u8 = 0x1D;
    pub const OPCODE_GATQ:                 u8 = 0x1E;
    pub const OPCODE_SASL_LIST_MECHS:      u8 = 0x20;
    pub const OPCODE_SASL_AUTH:            u8 = 0x21;
    pub const OPCODE_SASL_STEP:            u8 = 0x22;
    pub const OPCODE_RGET:                 u8 = 0x30;
    pub const OPCODE_RSET:                 u8 = 0x31;
    pub const OPCODE_RSETQ:                u8 = 0x32;
    pub const OPCODE_RAPPEND:              u8 = 0x33;
    pub const OPCODE_RAPPENDQ:             u8 = 0x34;
    pub const OPCODE_RPREPEND:             u8 = 0x35;
    pub const OPCODE_RPREPENDQ:            u8 = 0x36;
    pub const OPCODE_RDEL:                 u8 = 0x37;
    pub const OPCODE_RDELQ:                u8 = 0x38;
    pub const OPCODE_RINCR:                u8 = 0x39;
    pub const OPCODE_RINCRQ:               u8 = 0x3A;
    pub const OPCODE_RDECR:                u8 = 0x3B;
    pub const OPCODE_RDECRQ:               u8 = 0x3C;
    pub const OPCODE_SET_VBUCKET:          u8 = 0x3D;
    pub const OPCODE_GET_VBUCKET:          u8 = 0x3E;
    pub const OPCODE_DEL_VBUCKET:          u8 = 0x3F;
    pub const OPCODE_TAP_CONNECT:          u8 = 0x40;
    pub const OPCODE_TAP_MUTATION:         u8 = 0x41;
    pub const OPCODE_TAP_DEL:              u8 = 0x42;
    pub const OPCODE_TAP_FLUSH:            u8 = 0x43;
    pub const OPCODE_TAP_OPAQUE:           u8 = 0x44;
    pub const OPCODE_TAP_VBUCKET_SET:      u8 = 0x45;
    pub const OPCODE_TAP_CHECKPOINT_START: u8 = 0x46;
    pub const OPCODE_TAP_CHECKPOINT_END:   u8 = 0x47;

    pub const DATA_TYPE_RAW_BYTES: u8 = 0x00;
}

/// Memcached response status
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u16)]
#[rustfmt::skip]
pub enum Status {
    NoError                           = consts::STATUS_NO_ERROR,
    KeyNotFound                       = consts::STATUS_KEY_NOT_FOUND,
    KeyExists                         = consts::STATUS_KEY_EXISTS,
    ValueTooLarge                     = consts::STATUS_VALUE_TOO_LARGE,
    InvalidArguments                  = consts::STATUS_INVALID_ARGUMENTS,
    ItemNotStored                     = consts::STATUS_ITEM_NOT_STORED,
    IncrDecrOnNonNumericValue         = consts::STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE,
    VBucketBelongsToOtherServer       = consts::STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER,
    AuthenticationError               = consts::STATUS_AUTHENTICATION_ERROR,
    AuthenticationContinue            = consts::STATUS_AUTHENTICATION_CONTINUE,
    UnknownCommand                    = consts::STATUS_UNKNOWN_COMMAND,
    OutOfMemory                       = consts::STATUS_OUT_OF_MEMORY,
    NotSupported                      = consts::STATUS_NOT_SUPPORTED,
    InternalError                     = consts::STATUS_INTERNAL_ERROR,
    Busy                              = consts::STATUS_BUSY,
    TemporaryFailure                  = consts::STATUS_TEMPORARY_FAILURE,
    AuthenticationRequired            = consts::STATUS_AUTHENTICATION_REQUIRED,
    AuthenticationFurtherStepRequired = consts::STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED,
}

impl Status {
    /// Get the binary code of the status
    #[inline]
    pub fn to_u16(self) -> u16 {
        self as u16
    }

    /// Generate a Status from binary code
    #[inline]
    #[rustfmt::skip]
    pub fn from_u16(code: u16) -> Option<Status> {
        match code {
            consts::STATUS_NO_ERROR                             => Some(Status::NoError),
            consts::STATUS_KEY_NOT_FOUND                        => Some(Status::KeyNotFound),
            consts::STATUS_KEY_EXISTS                           => Some(Status::KeyExists),
            consts::STATUS_VALUE_TOO_LARGE                      => Some(Status::ValueTooLarge),
            consts::STATUS_INVALID_ARGUMENTS                    => Some(Status::InvalidArguments),
            consts::STATUS_ITEM_NOT_STORED                      => Some(Status::ItemNotStored),
            consts::STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE    => Some(Status::IncrDecrOnNonNumericValue),
            consts::STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER      => Some(Status::VBucketBelongsToOtherServer),
            consts::STATUS_AUTHENTICATION_ERROR                 => Some(Status::AuthenticationError),
            consts::STATUS_AUTHENTICATION_CONTINUE              => Some(Status::AuthenticationContinue),
            consts::STATUS_UNKNOWN_COMMAND                      => Some(Status::UnknownCommand),
            consts::STATUS_OUT_OF_MEMORY                        => Some(Status::OutOfMemory),
            consts::STATUS_NOT_SUPPORTED                        => Some(Status::NotSupported),
            consts::STATUS_INTERNAL_ERROR                       => Some(Status::InternalError),
            consts::STATUS_BUSY                                 => Some(Status::Busy),
            consts::STATUS_TEMPORARY_FAILURE                    => Some(Status::TemporaryFailure),
            consts::STATUS_AUTHENTICATION_REQUIRED              => Some(Status::AuthenticationRequired),
            consts::STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED => Some(Status::AuthenticationFurtherStepRequired),
            _ => None,
        }
    }

    /// Get a short description
    #[inline]
    #[rustfmt::skip]
    pub fn desc(self) -> &'static str {
        match self {
            Status::NoError                           => "no error",
            Status::KeyNotFound                       => "key not found",
            Status::KeyExists                         => "key exists",
            Status::ValueTooLarge                     => "value too large",
            Status::InvalidArguments                  => "invalid argument",
            Status::ItemNotStored                     => "item not stored",
            Status::IncrDecrOnNonNumericValue         => "incr or decr on non-numeric value",
            Status::VBucketBelongsToOtherServer       => "vbucket belongs to other server",
            Status::AuthenticationError               => "authentication error",
            Status::AuthenticationContinue            => "authentication continue",
            Status::UnknownCommand                    => "unknown command",
            Status::OutOfMemory                       => "out of memory",
            Status::NotSupported                      => "not supported",
            Status::InternalError                     => "internal error",
            Status::Busy                              => "busy",
            Status::TemporaryFailure                  => "temporary failure",
            Status::AuthenticationRequired            => "authentication required/not successful",
            Status::AuthenticationFurtherStepRequired => "further authentication steps required",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
#[rustfmt::skip]
pub enum Command {
    Get                = consts::OPCODE_GET,
    Set                = consts::OPCODE_SET,
    Add                = consts::OPCODE_ADD,
    Replace            = consts::OPCODE_REPLACE,
    Delete             = consts::OPCODE_DEL,
    Increment          = consts::OPCODE_INCR,
    Decrement          = consts::OPCODE_DECR,
    Quit               = consts::OPCODE_QUIT,
    Flush              = consts::OPCODE_FLUSH,
    GetQuietly         = consts::OPCODE_GETQ,
    Noop               = consts::OPCODE_NOP,
    Version            = consts::OPCODE_VERSION,
    GetKey             = consts::OPCODE_GETK,
    GetKeyQuietly      = consts::OPCODE_GETKQ,
    Append             = consts::OPCODE_APPEND,
    Prepend            = consts::OPCODE_PREPEND,
    Stat               = consts::OPCODE_STAT,
    SetQuietly         = consts::OPCODE_SETQ,
    AddQuietly         = consts::OPCODE_ADDQ,
    ReplaceQuietly     = consts::OPCODE_REPLACEQ,
    DeleteQuietly      = consts::OPCODE_DELQ,
    IncrementQuietly   = consts::OPCODE_INCRQ,
    DecrementQuietly   = consts::OPCODE_DECRQ,
    QuitQuietly        = consts::OPCODE_QUITQ,
    FlushQuietly       = consts::OPCODE_FLUSHQ,
    AppendQuietly      = consts::OPCODE_APPENDQ,
    PrependQuietly     = consts::OPCODE_PREPENDQ,
    Verbosity          = consts::OPCODE_VERBOSITY,
    Touch              = consts::OPCODE_TOUCH,
    GetAndTouch        = consts::OPCODE_GAT,
    GetAndTouchQuietly = consts::OPCODE_GATQ,
    SaslListMechanisms = consts::OPCODE_SASL_LIST_MECHS,
    SaslAuthenticate   = consts::OPCODE_SASL_AUTH,
    SaslStep           = consts::OPCODE_SASL_STEP,
    RGet               = consts::OPCODE_RGET,
    RSet               = consts::OPCODE_RSET,
    RSetQuietly        = consts::OPCODE_RSETQ,
    RAppend            = consts::OPCODE_RAPPEND,
    RAppendQuietly     = consts::OPCODE_RAPPENDQ,
    RPrepend           = consts::OPCODE_RPREPEND,
    RPrependQuietly    = consts::OPCODE_RPREPENDQ,
    RDelete            = consts::OPCODE_RDEL,
    RDeleteQuietly     = consts::OPCODE_RDELQ,
    RIncrement         = consts::OPCODE_RINCR,
    RIncrementQuietly  = consts::OPCODE_RINCRQ,
    RDecrement         = consts::OPCODE_RDECR,
    RDecrementQuietly  = consts::OPCODE_RDECRQ,
    SetVBucket         = consts::OPCODE_SET_VBUCKET,
    GetVBucket         = consts::OPCODE_GET_VBUCKET,
    DelVBucket         = consts::OPCODE_DEL_VBUCKET,
    TapConnect         = consts::OPCODE_TAP_CONNECT,
    TapMutation        = consts::OPCODE_TAP_MUTATION,
    TapDelete          = consts::OPCODE_TAP_DEL,
    TapFlush           = consts::OPCODE_TAP_FLUSH,
    TapOpaque          = consts::OPCODE_TAP_OPAQUE,
    TapVBucketSet      = consts::OPCODE_TAP_VBUCKET_SET,
    TapCheckpointStart = consts::OPCODE_TAP_CHECKPOINT_START,
    TapCheckpointEnd   = consts::OPCODE_TAP_CHECKPOINT_END,
}

impl Command {
    #[inline]
    fn to_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    #[rustfmt::skip]
    fn from_u8(code: u8) -> Option<Command> {
        match code {
            consts::OPCODE_GET                  => Some(Command::Get),
            consts::OPCODE_SET                  => Some(Command::Set),
            consts::OPCODE_ADD                  => Some(Command::Add),
            consts::OPCODE_REPLACE              => Some(Command::Replace),
            consts::OPCODE_DEL                  => Some(Command::Delete),
            consts::OPCODE_INCR                 => Some(Command::Increment),
            consts::OPCODE_DECR                 => Some(Command::Decrement),
            consts::OPCODE_QUIT                 => Some(Command::Quit),
            consts::OPCODE_FLUSH                => Some(Command::Flush),
            consts::OPCODE_GETQ                 => Some(Command::GetQuietly),
            consts::OPCODE_NOP                  => Some(Command::Noop),
            consts::OPCODE_VERSION              => Some(Command::Version),
            consts::OPCODE_GETK                 => Some(Command::GetKey),
            consts::OPCODE_GETKQ                => Some(Command::GetKeyQuietly),
            consts::OPCODE_APPEND               => Some(Command::Append),
            consts::OPCODE_PREPEND              => Some(Command::Prepend),
            consts::OPCODE_STAT                 => Some(Command::Stat),
            consts::OPCODE_SETQ                 => Some(Command::SetQuietly),
            consts::OPCODE_ADDQ                 => Some(Command::AddQuietly),
            consts::OPCODE_REPLACEQ             => Some(Command::ReplaceQuietly),
            consts::OPCODE_DELQ                 => Some(Command::DeleteQuietly),
            consts::OPCODE_INCRQ                => Some(Command::IncrementQuietly),
            consts::OPCODE_DECRQ                => Some(Command::DecrementQuietly),
            consts::OPCODE_QUITQ                => Some(Command::QuitQuietly),
            consts::OPCODE_FLUSHQ               => Some(Command::FlushQuietly),
            consts::OPCODE_APPENDQ              => Some(Command::AppendQuietly),
            consts::OPCODE_PREPENDQ             => Some(Command::PrependQuietly),
            consts::OPCODE_VERBOSITY            => Some(Command::Verbosity),
            consts::OPCODE_TOUCH                => Some(Command::Touch),
            consts::OPCODE_GAT                  => Some(Command::GetAndTouch),
            consts::OPCODE_GATQ                 => Some(Command::GetAndTouchQuietly),
            consts::OPCODE_SASL_LIST_MECHS      => Some(Command::SaslListMechanisms),
            consts::OPCODE_SASL_AUTH            => Some(Command::SaslAuthenticate),
            consts::OPCODE_SASL_STEP            => Some(Command::SaslStep),
            consts::OPCODE_RGET                 => Some(Command::RGet),
            consts::OPCODE_RSET                 => Some(Command::RSet),
            consts::OPCODE_RSETQ                => Some(Command::RSetQuietly),
            consts::OPCODE_RAPPEND              => Some(Command::RAppend),
            consts::OPCODE_RAPPENDQ             => Some(Command::RAppendQuietly),
            consts::OPCODE_RPREPEND             => Some(Command::RPrepend),
            consts::OPCODE_RPREPENDQ            => Some(Command::RPrependQuietly),
            consts::OPCODE_RDEL                 => Some(Command::RDelete),
            consts::OPCODE_RDELQ                => Some(Command::RDeleteQuietly),
            consts::OPCODE_RINCR                => Some(Command::RIncrement),
            consts::OPCODE_RINCRQ               => Some(Command::RIncrementQuietly),
            consts::OPCODE_RDECR                => Some(Command::RDecrement),
            consts::OPCODE_RDECRQ               => Some(Command::RDecrementQuietly),
            consts::OPCODE_SET_VBUCKET          => Some(Command::SetVBucket),
            consts::OPCODE_GET_VBUCKET          => Some(Command::GetVBucket),
            consts::OPCODE_DEL_VBUCKET          => Some(Command::DelVBucket),
            consts::OPCODE_TAP_CONNECT          => Some(Command::TapConnect),
            consts::OPCODE_TAP_MUTATION         => Some(Command::TapMutation),
            consts::OPCODE_TAP_DEL              => Some(Command::TapDelete),
            consts::OPCODE_TAP_FLUSH            => Some(Command::TapFlush),
            consts::OPCODE_TAP_OPAQUE           => Some(Command::TapOpaque),
            consts::OPCODE_TAP_VBUCKET_SET      => Some(Command::TapVBucketSet),
            consts::OPCODE_TAP_CHECKPOINT_START => Some(Command::TapCheckpointStart),
            consts::OPCODE_TAP_CHECKPOINT_END   => Some(Command::TapCheckpointEnd),
            _                                   => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    RawBytes,
}

impl DataType {
    #[inline]
    fn to_u8(self) -> u8 {
        match self {
            DataType::RawBytes => consts::DATA_TYPE_RAW_BYTES,
        }
    }

    #[inline]
    fn from_u8(code: u8) -> Option<DataType> {
        match code {
            consts::DATA_TYPE_RAW_BYTES => Some(DataType::RawBytes),
            _ => None,
        }
    }
}

// Byte/     0       |       1       |       2       |       3       |
//    /              |               |               |               |
//   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//   +---------------+---------------+---------------+---------------+
//  0| Magic         | Opcode        | Key length                    |
//   +---------------+---------------+---------------+---------------+
//  4| Extras length | Data type     | vbucket id                    |
//   +---------------+---------------+---------------+---------------+
//  8| Total body length                                             |
//   +---------------+---------------+---------------+---------------+
// 12| Opaque                                                        |
//   +---------------+---------------+---------------+---------------+
// 16| CAS                                                           |
//   |                                                               |
//   +---------------+---------------+---------------+---------------+
//   Total 24 bytes
#[derive(Clone, Debug)]
pub struct RequestHeader {
    pub command: Command,
    key_len: u16,
    extra_len: u8,
    pub data_type: DataType,
    pub vbucket_id: u16,
    body_len: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl RequestHeader {
    pub fn new(
        cmd: Command,
        dtype: DataType,
        vbid: u16,
        opaque: u32,
        cas: u64,
        key_len: u16,
        extra_len: u8,
        body_len: u32,
    ) -> RequestHeader {
        RequestHeader {
            command: cmd,
            key_len,
            extra_len,
            data_type: dtype,
            vbucket_id: vbid,
            body_len,
            opaque,
            cas,
        }
    }

    pub fn from_payload(
        cmd: Command,
        dtype: DataType,
        vbid: u16,
        opaque: u32,
        cas: u64,
        key: &[u8],
        extra: &[u8],
        value: &[u8],
    ) -> RequestHeader {
        let key_len = key.len() as u16;
        let extra_len = extra.len() as u8;
        let body_len = (key.len() + extra.len() + value.len()) as u32;

        RequestHeader::new(cmd, dtype, vbid, opaque, cas, key_len, extra_len, body_len)
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(consts::MAGIC_REQUEST)?;
        writer.write_u8(self.command.to_u8())?;
        writer.write_u16::<BigEndian>(self.key_len)?;
        writer.write_u8(self.extra_len)?;
        writer.write_u8(self.data_type.to_u8())?;
        writer.write_u16::<BigEndian>(self.vbucket_id)?;
        writer.write_u32::<BigEndian>(self.body_len)?;
        writer.write_u32::<BigEndian>(self.opaque)?;
        writer.write_u64::<BigEndian>(self.cas)?;

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<RequestHeader> {
        let magic = reader.read_u8()?;

        if magic != consts::MAGIC_REQUEST {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid magic"));
        }

        Ok(RequestHeader {
            command: match Command::from_u8(reader.read_u8()?) {
                Some(c) => c,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid command")),
            },
            key_len: reader.read_u16::<BigEndian>()?,
            extra_len: reader.read_u8()?,
            data_type: match DataType::from_u8(reader.read_u8()?) {
                Some(d) => d,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid data type")),
            },
            vbucket_id: reader.read_u16::<BigEndian>()?,
            body_len: reader.read_u32::<BigEndian>()?,
            opaque: reader.read_u32::<BigEndian>()?,
            cas: reader.read_u64::<BigEndian>()?,
        })
    }
}

// Byte/     0       |       1       |       2       |       3       |
//    /              |               |               |               |
//   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//   +---------------+---------------+---------------+---------------+
//  0| Magic         | Opcode        | Key Length                    |
//   +---------------+---------------+---------------+---------------+
//  4| Extras length | Data type     | Status                        |
//   +---------------+---------------+---------------+---------------+
//  8| Total body length                                             |
//   +---------------+---------------+---------------+---------------+
// 12| Opaque                                                        |
//   +---------------+---------------+---------------+---------------+
// 16| CAS                                                           |
//   |                                                               |
//   +---------------+---------------+---------------+---------------+
//   Total 24 bytes
#[derive(Clone, Debug)]
pub struct ResponseHeader {
    pub command: Command,
    key_len: u16,
    extra_len: u8,
    pub data_type: DataType,
    pub status: Status,
    body_len: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl ResponseHeader {
    pub fn new(
        command: Command,
        data_type: DataType,
        status: Status,
        opaque: u32,
        cas: u64,
        key_len: u16,
        extra_len: u8,
        body_len: u32,
    ) -> ResponseHeader {
        ResponseHeader {
            command,
            key_len,
            extra_len,
            data_type,
            status,
            body_len,
            opaque,
            cas,
        }
    }

    pub fn from_payload(
        cmd: Command,
        dtype: DataType,
        status: Status,
        opaque: u32,
        cas: u64,
        key: &[u8],
        extra: &[u8],
        value: &[u8],
    ) -> ResponseHeader {
        let key_len = key.len() as u16;
        let extra_len = extra.len() as u8;
        let body_len = (key.len() + extra.len() + value.len()) as u32;

        ResponseHeader::new(
            cmd, dtype, status, opaque, cas, key_len, extra_len, body_len,
        )
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(consts::MAGIC_RESPONSE)?;
        writer.write_u8(self.command.to_u8())?;
        writer.write_u16::<BigEndian>(self.key_len)?;
        writer.write_u8(self.extra_len)?;
        writer.write_u8(self.data_type.to_u8())?;
        writer.write_u16::<BigEndian>(self.status.to_u16())?;
        writer.write_u32::<BigEndian>(self.body_len)?;
        writer.write_u32::<BigEndian>(self.opaque)?;
        writer.write_u64::<BigEndian>(self.cas)?;

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<ResponseHeader> {
        let magic = reader.read_u8()?;

        if magic != consts::MAGIC_RESPONSE {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid magic"));
        }

        Ok(ResponseHeader {
            command: match Command::from_u8(reader.read_u8()?) {
                Some(c) => c,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid command")),
            },
            key_len: reader.read_u16::<BigEndian>()?,
            extra_len: reader.read_u8()?,
            data_type: match DataType::from_u8(reader.read_u8()?) {
                Some(d) => d,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid data type")),
            },
            status: match Status::from_u16(reader.read_u16::<BigEndian>()?) {
                Some(s) => s,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid status")),
            },
            body_len: reader.read_u32::<BigEndian>()?,
            opaque: reader.read_u32::<BigEndian>()?,
            cas: reader.read_u64::<BigEndian>()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct RequestPacket {
    pub header: RequestHeader,
    pub extra: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl RequestPacket {
    pub fn new(
        cmd: Command,
        dtype: DataType,
        vbid: u16,
        opaque: u32,
        cas: u64,
        extra: Bytes,
        key: Bytes,
        value: Bytes,
    ) -> RequestPacket {
        RequestPacket {
            header: RequestHeader::from_payload(
                cmd,
                dtype,
                vbid,
                opaque,
                cas,
                key.bytes(),
                extra.bytes(),
                value.bytes(),
            ),
            extra,
            key,
            value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        writer.write_all(self.extra.bytes())?;
        writer.write_all(self.key.bytes())?;
        writer.write_all(self.value.bytes())?;

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<RequestPacket> {
        let header = RequestHeader::read_from(reader)?;

        let extra_len = header.extra_len as usize;
        let key_len = header.key_len as usize;
        let body_len =  header.body_len as usize;

        let mut buf = BytesMut::with_capacity(body_len);
        unsafe { buf.set_len(body_len); }

        let mut extra = buf.split_to(extra_len);
        let mut key = buf.split_to(key_len);
        let mut value = buf;
        reader.read_exact(extra.as_mut())?;
        reader.read_exact(key.as_mut())?;
        reader.read_exact(value.as_mut())?;

        Ok(RequestPacket {
            header,
            extra: extra.freeze(),
            key: key.freeze(),
            value: value.freeze(),
        })
    }

    pub fn as_ref(&self) -> RequestPacketRef<'_> {
        RequestPacketRef::new(
            &self.header,
            &self.extra[..],
            &self.key[..],
            &self.value[..],
        )
    }
}

#[derive(Debug)]
pub struct RequestPacketRef<'a> {
    pub header: &'a RequestHeader,
    pub extra: &'a [u8],
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> RequestPacketRef<'a> {
    pub fn new(
        header: &'a RequestHeader,
        extra: &'a [u8],
        key: &'a [u8],
        value: &'a [u8],
    ) -> RequestPacketRef<'a> {
        RequestPacketRef {
            header,
            extra,
            key,
            value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        writer.write_all(self.extra)?;
        writer.write_all(self.key)?;
        writer.write_all(self.value)?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ResponsePacket {
    pub header: ResponseHeader,
    pub extra: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl ResponsePacket {
    pub fn new(
        cmd: Command,
        dtype: DataType,
        status: Status,
        opaque: u32,
        cas: u64,
        extra: Bytes,
        key: Bytes,
        value: Bytes,
    ) -> ResponsePacket {
        ResponsePacket {
            header: ResponseHeader::from_payload(
                cmd,
                dtype,
                status,
                opaque,
                cas,
                key.bytes(),
                extra.bytes(),
                value.bytes(),
            ),
            extra,
            key,
            value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        writer.write_all(self.extra.bytes())?;
        writer.write_all(self.key.bytes())?;
        writer.write_all(self.value.bytes())?;

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<ResponsePacket> {
        let header = ResponseHeader::read_from(reader)?;

        let extra_len = header.extra_len as usize;
        let key_len = header.key_len as usize;
        let body_len =  header.body_len as usize;

        let mut buf = BytesMut::with_capacity(body_len);
        unsafe { buf.set_len(body_len); }

        let mut extra = buf.split_to(extra_len);
        let mut key = buf.split_to(key_len);
        let mut value = buf;
        reader.read_exact(extra.as_mut())?;
        reader.read_exact(key.as_mut())?;
        reader.read_exact(value.as_mut())?;

        Ok(ResponsePacket {
            header,
            extra: extra.freeze(),
            key: key.freeze(),
            value: value.freeze(),
        })
    }
}

pub struct ResponsePacketRef<'a> {
    pub header: &'a ResponseHeader,
    pub extra: &'a [u8],
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> ResponsePacketRef<'a> {
    pub fn new(
        header: &'a ResponseHeader,
        extra: &'a [u8],
        key: &'a [u8],
        value: &'a [u8],
    ) -> ResponsePacketRef<'a> {
        ResponsePacketRef {
            header,
            extra,
            key,
            value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.header.write_to(writer)?;
        writer.write_all(self.extra)?;
        writer.write_all(self.key)?;
        writer.write_all(self.value)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::net::TcpStream;

    use crate::proto;
    use crate::proto::binarydef::{Command, DataType, RequestPacket, ResponsePacket};

    use bufstream::BufStream;
    use bytes::Bytes;

    fn test_stream() -> TcpStream {
        TcpStream::connect("127.0.0.1:11211").unwrap()
    }

    #[test]
    fn test_binary_protocol() {
        let mut stream = BufStream::new(test_stream());

        {
            let req_packet = RequestPacket::new(
                Command::Set,
                DataType::RawBytes,
                0,
                0,
                0,
                vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x0e, 0x10].into(),
                b"test:binary_proto:hello".as_ref().into(),
                b"world".as_ref().into(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.header.status, proto::binary::Status::NoError);
        }

        {
            let req_packet = RequestPacket::new(
                Command::Get,
                DataType::RawBytes,
                0,
                0,
                0,
                Bytes::new(),
                b"test:binary_proto:hello".as_ref().into(),
                Bytes::new(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.header.status, proto::binary::Status::NoError);
            assert_eq!(&resp_packet.value[..], b"world");
        }

        {
            let req_packet = RequestPacket::new(
                Command::Delete,
                DataType::RawBytes,
                0,
                0,
                0,
                Bytes::new(),
                b"test:binary_proto:hello".as_ref().into(),
                Bytes::new(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.header.status, proto::binary::Status::NoError);
        }
    }
}
