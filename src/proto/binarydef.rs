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

use std::io::{self, Write, Read};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

const MAGIC_REQUEST: u8 = 0x80;
const MAGIC_RESPONSE: u8 = 0x81;

const STATUS_NO_ERROR: u16 = 0x0000;
const STATUS_KEY_NOT_FOUND: u16 = 0x0001;
const STATUS_KEY_EXISTS: u16 = 0x0002;
const STATUS_VALUE_TOO_LARGE: u16 = 0x0003;
const STATUS_INVALID_ARGUMENTS: u16 = 0x0004;
const STATUS_ITEM_NOT_STORED: u16 = 0x0005;
const STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE: u16 = 0x0006;
const STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER: u16 = 0x0007;
const STATUS_AUTHENTICATION_ERROR: u16 = 0x0008;
const STATUS_AUTHENTICATION_CONTINUE: u16 = 0x0009;
const STATUS_UNKNOWN_COMMAND: u16 = 0x0081;
const STATUS_OUT_OF_MEMORY: u16 = 0x0082;
const STATUS_NOT_SUPPORTED: u16 = 0x0083;
const STATUS_INTERNAL_ERROR: u16 = 0x0084;
const STATUS_BUSY: u16 = 0x0085;
const STATUS_TEMPORARY_FAILURE: u16 = 0x0086;
const STATUS_AUTHENTICATION_REQUIRED: u16 = 0x0020;
const STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED: u16 = 0x0021;

const OPCODE_GET: u8 = 0x00;
const OPCODE_SET: u8 = 0x01;
const OPCODE_ADD: u8 = 0x02;
const OPCODE_REPLACE: u8 = 0x03;
const OPCODE_DEL: u8 = 0x04;
const OPCODE_INCR: u8 = 0x05;
const OPCODE_DECR: u8 = 0x06;
const OPCODE_QUIT: u8 = 0x07;
const OPCODE_FLUSH: u8 = 0x08;
const OPCODE_GETQ: u8 = 0x09;
const OPCODE_NOP: u8 = 0x0A;
const OPCODE_VERSION: u8 = 0x0B;
const OPCODE_GETK: u8 = 0x0C;
const OPCODE_GETKQ: u8 = 0x0D;
const OPCODE_APPEND: u8 = 0x0E;
const OPCODE_PREPEND: u8 = 0x0F;
const OPCODE_STAT: u8 = 0x10;
const OPCODE_SETQ: u8 = 0x11;
const OPCODE_ADDQ: u8 = 0x12;
const OPCODE_REPLACEQ: u8 = 0x13;
const OPCODE_DELQ: u8 = 0x14;
const OPCODE_INCRQ: u8 = 0x15;
const OPCODE_DECRQ: u8 = 0x16;
const OPCODE_QUITQ: u8 = 0x17;
const OPCODE_FLUSHQ: u8 = 0x18;
const OPCODE_APPENDQ: u8 = 0x19;
const OPCODE_PREPENDQ: u8 = 0x1A;
const OPCODE_VERBOSITY: u8 = 0x1B;
const OPCODE_TOUCH: u8 = 0x1C;
const OPCODE_GAT: u8 = 0x1D;
const OPCODE_GATQ: u8 = 0x1E;
const OPCODE_SASL_LIST_MECHS: u8 = 0x20;
const OPCODE_SASL_AUTH: u8 = 0x21;
const OPCODE_SASL_STEP: u8 = 0x22;
const OPCODE_RGET: u8 = 0x30;
const OPCODE_RSET: u8 = 0x31;
const OPCODE_RSETQ: u8 = 0x32;
const OPCODE_RAPPEND: u8 = 0x33;
const OPCODE_RAPPENDQ: u8 = 0x34;
const OPCODE_RPREPEND: u8 = 0x35;
const OPCODE_RPREPENDQ: u8 = 0x36;
const OPCODE_RDEL: u8 = 0x37;
const OPCODE_RDELQ: u8 = 0x38;
const OPCODE_RINCR: u8 = 0x39;
const OPCODE_RINCRQ: u8 = 0x3A;
const OPCODE_RDECR: u8 = 0x3B;
const OPCODE_RDECRQ: u8 = 0x3C;
const OPCODE_SET_VBUCKET: u8 = 0x3D;
const OPCODE_GET_VBUCKET: u8 = 0x3E;
const OPCODE_DEL_VBUCKET: u8 = 0x3F;
const OPCODE_TAP_CONNECT: u8 = 0x40;
const OPCODE_TAP_MUTATION: u8 = 0x41;
const OPCODE_TAP_DEL: u8 = 0x42;
const OPCODE_TAP_FLUSH: u8 = 0x43;
const OPCODE_TAP_OPAQUE: u8 = 0x44;
const OPCODE_TAP_VBUCKET_SET: u8 = 0x45;
const OPCODE_TAP_CHECKPOINT_START: u8 = 0x46;
const OPCODE_TAP_CHECKPOINT_END: u8 = 0x47;

const DATA_TYPE_RAW_BYTES: u8 = 0x00;

/// Memcached response status
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Status {
    NoError                             = STATUS_NO_ERROR,
    KeyNotFound                         = STATUS_KEY_NOT_FOUND,
    KeyExists                           = STATUS_KEY_EXISTS,
    ValueTooLarge                       = STATUS_VALUE_TOO_LARGE,
    InvalidArguments                    = STATUS_INVALID_ARGUMENTS,
    ItemNotStored                       = STATUS_ITEM_NOT_STORED,
    IncrDecrOnNonNumericValue           = STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE,
    VBucketBelongsToOtherServer         = STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER,
    AuthenticationError                 = STATUS_AUTHENTICATION_ERROR,
    AuthenticationContinue              = STATUS_AUTHENTICATION_CONTINUE,
    UnknownCommand                      = STATUS_UNKNOWN_COMMAND,
    OutOfMemory                         = STATUS_OUT_OF_MEMORY,
    NotSupported                        = STATUS_NOT_SUPPORTED,
    InternalError                       = STATUS_INTERNAL_ERROR,
    Busy                                = STATUS_BUSY,
    TemporaryFailure                    = STATUS_TEMPORARY_FAILURE,
    AuthenticationRequired              = STATUS_AUTHENTICATION_REQUIRED,
    AuthenticationFurtherStepRequired   = STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED,
}

impl Status {
    /// Get the binary code of the status
    #[inline]
    pub fn to_u16(&self) -> u16 {
        *self as u16
    }

    /// Generate a Status from binary code
    #[inline]
    pub fn from_u16(code: u16) -> Option<Status> {
        match code {
            STATUS_NO_ERROR => Some(Status::NoError),
            STATUS_KEY_NOT_FOUND => Some(Status::KeyNotFound),
            STATUS_KEY_EXISTS => Some(Status::KeyExists),
            STATUS_VALUE_TOO_LARGE => Some(Status::ValueTooLarge),
            STATUS_INVALID_ARGUMENTS => Some(Status::InvalidArguments),
            STATUS_ITEM_NOT_STORED => Some(Status::ItemNotStored),
            STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE => Some(Status::IncrDecrOnNonNumericValue),
            STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER => Some(Status::VBucketBelongsToOtherServer),
            STATUS_AUTHENTICATION_ERROR => Some(Status::AuthenticationError),
            STATUS_AUTHENTICATION_CONTINUE => Some(Status::AuthenticationContinue),
            STATUS_UNKNOWN_COMMAND => Some(Status::UnknownCommand),
            STATUS_OUT_OF_MEMORY => Some(Status::OutOfMemory),
            STATUS_NOT_SUPPORTED => Some(Status::NotSupported),
            STATUS_INTERNAL_ERROR => Some(Status::InternalError),
            STATUS_BUSY => Some(Status::Busy),
            STATUS_TEMPORARY_FAILURE => Some(Status::TemporaryFailure),
            STATUS_AUTHENTICATION_REQUIRED => Some(Status::AuthenticationRequired),
            STATUS_AUTHENTICATION_FURTHER_STEP_REQUIRED => Some(Status::AuthenticationFurtherStepRequired),
            _ => None
        }
    }

    /// Get a short description
    #[inline]
    pub fn desc(&self) -> &'static str {
        match *self {
            Status::NoError => "no error",
            Status::KeyNotFound => "key not found",
            Status::KeyExists => "key exists",
            Status::ValueTooLarge => "value too large",
            Status::InvalidArguments => "invalid argument",
            Status::ItemNotStored => "item not stored",
            Status::IncrDecrOnNonNumericValue => "incr or decr on non-numeric value",
            Status::VBucketBelongsToOtherServer => "vbucket belongs to other server",
            Status::AuthenticationError => "authentication error",
            Status::AuthenticationContinue => "authentication continue",
            Status::UnknownCommand => "unknown command",
            Status::OutOfMemory => "out of memory",
            Status::NotSupported => "not supported",
            Status::InternalError => "internal error",
            Status::Busy => "busy",
            Status::TemporaryFailure => "temporary failure",
            Status::AuthenticationRequired => "authentication required/not successful",
            Status::AuthenticationFurtherStepRequired => "further authentication steps required",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Command {
    Get                 = OPCODE_GET,
    Set                 = OPCODE_SET,
    Add                 = OPCODE_ADD,
    Replace             = OPCODE_REPLACE,
    Delete              = OPCODE_DEL,
    Increment           = OPCODE_INCR,
    Decrement           = OPCODE_DECR,
    Quit                = OPCODE_QUIT,
    Flush               = OPCODE_FLUSH,
    GetQuietly          = OPCODE_GETQ,
    Noop                = OPCODE_NOP,
    Version             = OPCODE_VERSION,
    GetKey              = OPCODE_GETK,
    GetKeyQuietly       = OPCODE_GETKQ,
    Append              = OPCODE_APPEND,
    Prepend             = OPCODE_PREPEND,
    Stat                = OPCODE_STAT,
    SetQuietly          = OPCODE_SETQ,
    AddQuietly          = OPCODE_ADDQ,
    ReplaceQuietly      = OPCODE_REPLACEQ,
    DeleteQuietly       = OPCODE_DELQ,
    IncrementQuietly    = OPCODE_INCRQ,
    DecrementQuietly    = OPCODE_DECRQ,
    QuitQuietly         = OPCODE_QUITQ,
    FlushQuietly        = OPCODE_FLUSHQ,
    AppendQuietly       = OPCODE_APPENDQ,
    PrependQuietly      = OPCODE_PREPENDQ,
    Verbosity           = OPCODE_VERBOSITY,
    Touch               = OPCODE_TOUCH,
    GetAndTouch         = OPCODE_GAT,
    GetAndTouchQuietly  = OPCODE_GATQ,
    SaslListMechanisms  = OPCODE_SASL_LIST_MECHS,
    SaslAuthenticate    = OPCODE_SASL_AUTH,
    SaslStep            = OPCODE_SASL_STEP,
    RGet                = OPCODE_RGET,
    RSet                = OPCODE_RSET,
    RSetQuietly         = OPCODE_RSETQ,
    RAppend             = OPCODE_RAPPEND,
    RAppendQuietly      = OPCODE_RAPPENDQ,
    RPrepend            = OPCODE_RPREPEND,
    RPrependQuietly     = OPCODE_RPREPENDQ,
    RDelete             = OPCODE_RDEL,
    RDeleteQuietly      = OPCODE_RDELQ,
    RIncrement          = OPCODE_RINCR,
    RIncrementQuietly   = OPCODE_RINCRQ,
    RDecrement          = OPCODE_RDECR,
    RDecrementQuietly   = OPCODE_RDECRQ,
    SetVBucket          = OPCODE_SET_VBUCKET,
    GetVBucket          = OPCODE_GET_VBUCKET,
    DelVBucket          = OPCODE_DEL_VBUCKET,
    TapConnect          = OPCODE_TAP_CONNECT,
    TapMutation         = OPCODE_TAP_MUTATION,
    TapDelete           = OPCODE_TAP_DEL,
    TapFlush            = OPCODE_TAP_FLUSH,
    TapOpaque           = OPCODE_TAP_OPAQUE,
    TapVBucketSet       = OPCODE_TAP_VBUCKET_SET,
    TapCheckpointStart  = OPCODE_TAP_CHECKPOINT_START,
    TapCheckpointEnd    = OPCODE_TAP_CHECKPOINT_END,
}

impl Command {
    #[inline]
    fn to_u8(&self) -> u8 {
        *self as u8
    }

    #[inline]
    fn from_u8(code: u8) -> Option<Command> {
        match code {
            OPCODE_GET => Some(Command::Get),
            OPCODE_SET => Some(Command::Set),
            OPCODE_ADD => Some(Command::Add),
            OPCODE_REPLACE => Some(Command::Replace),
            OPCODE_DEL => Some(Command::Delete),
            OPCODE_INCR => Some(Command::Increment),
            OPCODE_DECR => Some(Command::Decrement),
            OPCODE_QUIT => Some(Command::Quit),
            OPCODE_FLUSH => Some(Command::Flush),
            OPCODE_GETQ => Some(Command::GetQuietly),
            OPCODE_NOP => Some(Command::Noop),
            OPCODE_VERSION => Some(Command::Version),
            OPCODE_GETK => Some(Command::GetKey),
            OPCODE_GETKQ => Some(Command::GetKeyQuietly),
            OPCODE_APPEND => Some(Command::Append),
            OPCODE_PREPEND => Some(Command::Prepend),
            OPCODE_STAT => Some(Command::Stat),
            OPCODE_SETQ => Some(Command::SetQuietly),
            OPCODE_ADDQ => Some(Command::AddQuietly),
            OPCODE_REPLACEQ => Some(Command::ReplaceQuietly),
            OPCODE_DELQ => Some(Command::DeleteQuietly),
            OPCODE_INCRQ => Some(Command::IncrementQuietly),
            OPCODE_DECRQ => Some(Command::DecrementQuietly),
            OPCODE_QUITQ => Some(Command::QuitQuietly),
            OPCODE_FLUSHQ => Some(Command::FlushQuietly),
            OPCODE_APPENDQ => Some(Command::AppendQuietly),
            OPCODE_PREPENDQ => Some(Command::PrependQuietly),
            OPCODE_VERBOSITY => Some(Command::Verbosity),
            OPCODE_TOUCH => Some(Command::Touch),
            OPCODE_GAT => Some(Command::GetAndTouch),
            OPCODE_GATQ => Some(Command::GetAndTouchQuietly),
            OPCODE_SASL_LIST_MECHS => Some(Command::SaslListMechanisms),
            OPCODE_SASL_AUTH => Some(Command::SaslAuthenticate),
            OPCODE_SASL_STEP => Some(Command::SaslStep),
            OPCODE_RGET => Some(Command::RGet),
            OPCODE_RSET => Some(Command::RSet),
            OPCODE_RSETQ => Some(Command::RSetQuietly),
            OPCODE_RAPPEND => Some(Command::RAppend),
            OPCODE_RAPPENDQ => Some(Command::RAppendQuietly),
            OPCODE_RPREPEND => Some(Command::RPrepend),
            OPCODE_RPREPENDQ => Some(Command::RPrependQuietly),
            OPCODE_RDEL => Some(Command::RDelete),
            OPCODE_RDELQ => Some(Command::RDeleteQuietly),
            OPCODE_RINCR => Some(Command::RIncrement),
            OPCODE_RINCRQ => Some(Command::RIncrementQuietly),
            OPCODE_RDECR => Some(Command::RDecrement),
            OPCODE_RDECRQ => Some(Command::RDecrementQuietly),
            OPCODE_SET_VBUCKET => Some(Command::SetVBucket),
            OPCODE_GET_VBUCKET => Some(Command::GetVBucket),
            OPCODE_DEL_VBUCKET => Some(Command::DelVBucket),
            OPCODE_TAP_CONNECT => Some(Command::TapConnect),
            OPCODE_TAP_MUTATION => Some(Command::TapMutation),
            OPCODE_TAP_DEL => Some(Command::TapDelete),
            OPCODE_TAP_FLUSH => Some(Command::TapFlush),
            OPCODE_TAP_OPAQUE => Some(Command::TapOpaque),
            OPCODE_TAP_VBUCKET_SET => Some(Command::TapVBucketSet),
            OPCODE_TAP_CHECKPOINT_START => Some(Command::TapCheckpointStart),
            OPCODE_TAP_CHECKPOINT_END => Some(Command::TapCheckpointEnd),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    RawBytes,
}

impl DataType {
    #[inline]
    fn to_u8(&self) -> u8 {
        match *self {
            DataType::RawBytes => DATA_TYPE_RAW_BYTES,
        }
    }

    #[inline]
    fn from_u8(code: u8) -> Option<DataType> {
        match code {
            DATA_TYPE_RAW_BYTES => Some(DataType::RawBytes),
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
    pub fn new(cmd: Command, dtype: DataType, vbid: u16, opaque: u32, cas: u64,
               key_len: u16, extra_len: u8, body_len: u32) -> RequestHeader {
        RequestHeader {
            command: cmd,
            key_len: key_len,
            extra_len: extra_len,
            data_type: dtype,
            vbucket_id: vbid,
            body_len: body_len,
            opaque: opaque,
            cas: cas,
        }
    }

    pub fn from_payload(cmd: Command, dtype: DataType, vbid: u16, opaque: u32, cas: u64,
                        key: &[u8], extra: &[u8], value: &[u8]) -> RequestHeader {
        let key_len = key.len() as u16;
        let extra_len = extra.len() as u8;
        let body_len = (key.len() + extra.len() + value.len()) as u32;

        RequestHeader::new(cmd, dtype, vbid, opaque, cas, key_len, extra_len, body_len)
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(writer.write_u8(MAGIC_REQUEST));
        try!(writer.write_u8(self.command.to_u8()));
        try!(writer.write_u16::<BigEndian>(self.key_len));
        try!(writer.write_u8(self.extra_len));
        try!(writer.write_u8(self.data_type.to_u8()));
        try!(writer.write_u16::<BigEndian>(self.vbucket_id));
        try!(writer.write_u32::<BigEndian>(self.body_len));
        try!(writer.write_u32::<BigEndian>(self.opaque));
        try!(writer.write_u64::<BigEndian>(self.cas));

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<RequestHeader> {
        let magic = try!(reader.read_u8());

        if magic != MAGIC_REQUEST {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid magic"));
        }

        Ok(RequestHeader {
            command: match Command::from_u8(try!(reader.read_u8())) {
                Some(c) => c,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid command")),
            },
            key_len: try!(reader.read_u16::<BigEndian>()),
            extra_len: try!(reader.read_u8()),
            data_type: match DataType::from_u8(try!(reader.read_u8())) {
                Some(d) => d,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid data type"))
            },
            vbucket_id: try!(reader.read_u16::<BigEndian>()),
            body_len: try!(reader.read_u32::<BigEndian>()),
            opaque: try!(reader.read_u32::<BigEndian>()),
            cas: try!(reader.read_u64::<BigEndian>()),
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
    pub fn new(cmd: Command, dtype: DataType, status: Status, opaque: u32, cas: u64,
               key_len: u16, extra_len: u8, body_len: u32) -> ResponseHeader {
        ResponseHeader {
            command: cmd,
            key_len: key_len,
            extra_len: extra_len,
            data_type: dtype,
            status: status,
            body_len: body_len,
            opaque: opaque,
            cas: cas,
        }
    }

    pub fn from_payload(cmd: Command, dtype: DataType, status: Status, opaque: u32, cas: u64,
                        key: &[u8], extra: &[u8], value: &[u8]) -> ResponseHeader {
        let key_len = key.len() as u16;
        let extra_len = extra.len() as u8;
        let body_len = (key.len() + extra.len() + value.len()) as u32;

        ResponseHeader::new(cmd, dtype, status, opaque, cas, key_len, extra_len, body_len)
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(writer.write_u8(MAGIC_RESPONSE));
        try!(writer.write_u8(self.command.to_u8()));
        try!(writer.write_u16::<BigEndian>(self.key_len));
        try!(writer.write_u8(self.extra_len));
        try!(writer.write_u8(self.data_type.to_u8()));
        try!(writer.write_u16::<BigEndian>(self.status.to_u16()));
        try!(writer.write_u32::<BigEndian>(self.body_len));
        try!(writer.write_u32::<BigEndian>(self.opaque));
        try!(writer.write_u64::<BigEndian>(self.cas));

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<ResponseHeader> {
        let magic = try!(reader.read_u8());

        if magic != MAGIC_RESPONSE {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid magic"));
        }

        Ok(ResponseHeader {
            command: match Command::from_u8(try!(reader.read_u8())) {
                Some(c) => c,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid command")),
            },
            key_len: try!(reader.read_u16::<BigEndian>()),
            extra_len: try!(reader.read_u8()),
            data_type: match DataType::from_u8(try!(reader.read_u8())) {
                Some(d) => d,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid data type"))
            },
            status: match Status::from_u16(try!(reader.read_u16::<BigEndian>())) {
                Some(s) => s,
                None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid status")),
            },
            body_len: try!(reader.read_u32::<BigEndian>()),
            opaque: try!(reader.read_u32::<BigEndian>()),
            cas: try!(reader.read_u64::<BigEndian>()),
        })
    }
}

#[derive(Clone, Debug)]
pub struct RequestPacket {
    pub header: RequestHeader,
    pub extra: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl RequestPacket {
    pub fn new(cmd: Command, dtype: DataType, vbid: u16, opaque: u32, cas: u64,
               extra: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> RequestPacket {
        RequestPacket {
            header: RequestHeader::from_payload(cmd, dtype, vbid, opaque, cas,
                                                &key[..], &extra[..], &value[..]),
            extra: extra,
            key: key,
            value: value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(self.header.write_to(writer));
        try!(writer.write_all(&self.extra[..]));
        try!(writer.write_all(&self.key[..]));
        try!(writer.write_all(&self.value[..]));

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<RequestPacket> {
        let header = try!(RequestHeader::read_from(reader));

        let extra_len = header.extra_len as usize;
        let key_len = header.key_len as usize;
        let value_len = header.body_len as usize - extra_len - key_len;

        let extra = {
            let mut buf = Vec::with_capacity(extra_len as usize);
            try!(reader.take(extra_len as u64).read_to_end(&mut buf));
            buf
        };

        let key = {
            let mut buf = Vec::with_capacity(key_len as usize);
            try!(reader.take(key_len as u64).read_to_end(&mut buf));
            buf
        };

        let value = {
            let mut buf = Vec::with_capacity(value_len as usize);
            try!(reader.take(value_len as u64).read_to_end(&mut buf));
            buf
        };

        Ok(RequestPacket {
            header: header,
            extra: extra,
            key: key,
            value: value,
        })
    }

    pub fn as_ref<'a>(&'a self) -> RequestPacketRef<'a> {
        RequestPacketRef::new(&self.header, &self.extra[..], &self.key[..], &self.value[..])
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
    pub fn new(header: &'a RequestHeader, extra: &'a [u8], key: &'a [u8], value: &'a [u8]) -> RequestPacketRef<'a> {
        RequestPacketRef {
            header: header,
            extra: extra,
            key: key,
            value: value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(self.header.write_to(writer));
        try!(writer.write_all(self.extra));
        try!(writer.write_all(self.key));
        try!(writer.write_all(self.value));

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ResponsePacket {
    pub header: ResponseHeader,
    pub extra: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ResponsePacket {
    pub fn new(cmd: Command, dtype: DataType, status: Status, opaque: u32, cas: u64,
               extra: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> ResponsePacket {
        ResponsePacket {
            header: ResponseHeader::from_payload(cmd, dtype, status, opaque, cas,
                                                 &key[..], &extra[..], &value[..]),
            extra: extra,
            key: key,
            value: value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(self.header.write_to(writer));
        try!(writer.write_all(&self.extra[..]));
        try!(writer.write_all(&self.key[..]));
        try!(writer.write_all(&self.value[..]));

        Ok(())
    }

    #[inline]
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<ResponsePacket> {
        let header = try!(ResponseHeader::read_from(reader));

        let extra_len = header.extra_len as usize;
        let key_len = header.key_len as usize;
        let value_len = header.body_len as usize - extra_len - key_len;

        let extra = {
            let mut buf = Vec::with_capacity(extra_len as usize);
            try!(reader.take(extra_len as u64).read_to_end(&mut buf));
            buf
        };

        let key = {
            let mut buf = Vec::with_capacity(key_len as usize);
            try!(reader.take(key_len as u64).read_to_end(&mut buf));
            buf
        };

        let value = {
            let mut buf = Vec::with_capacity(value_len as usize);
            try!(reader.take(value_len as u64).read_to_end(&mut buf));
            buf
        };

        Ok(ResponsePacket {
            header: header,
            extra: extra,
            key: key,
            value: value,
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
    pub fn new(header: &'a ResponseHeader, extra: &'a [u8], key: &'a [u8], value: &'a [u8]) -> ResponsePacketRef<'a> {
        ResponsePacketRef {
            header: header,
            extra: extra,
            key: key,
            value: value,
        }
    }

    #[inline]
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        try!(self.header.write_to(writer));
        try!(writer.write_all(self.extra));
        try!(writer.write_all(self.key));
        try!(writer.write_all(self.value));

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::net::TcpStream;
    use std::io::{BufStream, Write};

    use proto;
    use proto::binarydef::{RequestPacket, ResponsePacket, Command, DataType};

    fn test_stream() -> TcpStream {
        TcpStream::connect("127.0.0.1:11211").unwrap()
    }

    #[test]
    fn test_binary_protocol() {
        let mut stream = BufStream::new(test_stream());

        {
            let req_packet = RequestPacket::new(
                                Command::Set, DataType::RawBytes, 0, 0, 0,
                                vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x0e, 0x10],
                                b"test:binary_proto:hello".to_vec(),
                                b"world".to_vec());

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == proto::binary::Status::NoError);
        }

        {
            let req_packet = RequestPacket::new(
                                Command::Get, DataType::RawBytes, 0, 0, 0,
                                vec![],
                                b"test:binary_proto:hello".to_vec(),
                                vec![]);

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == proto::binary::Status::NoError);
            assert_eq!(&resp_packet.value[..], b"world");
        }

        {
            let req_packet = RequestPacket::new(
                                Command::Delete, DataType::RawBytes, 0, 0, 0,
                                vec![],
                                b"test:binary_proto:hello".to_vec(),
                                vec![]);

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == proto::binary::Status::NoError);
        }
    }
}
