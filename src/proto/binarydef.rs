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

use std::io::{Writer, Reader, IoResult, IoError, OtherIoError};

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
#[deriving(Clone, Show, Eq, PartialEq)]
pub enum Status {
    NoError,
    KeyNotFound,
    KeyExists,
    ValueTooLarge,
    InvalidArguments,
    ItemNotStored,
    IncrDecrOnNonNumericValue,
    VBucketBelongsToOtherServer,
    AuthenticationError,
    AuthenticationContinue,
    UnknownCommand,
    OutOfMemory,
    NotSupported,
    InternalError,
    Busy,
    TemporaryFailure,
}

impl Status {
    /// Get the binary code of the status
    pub fn code(&self) -> u16 {
        match *self {
            Status::NoError => STATUS_NO_ERROR,
            Status::KeyNotFound => STATUS_KEY_NOT_FOUND,
            Status::KeyExists => STATUS_KEY_EXISTS,
            Status::ValueTooLarge => STATUS_VALUE_TOO_LARGE,
            Status::InvalidArguments => STATUS_INVALID_ARGUMENTS,
            Status::ItemNotStored => STATUS_ITEM_NOT_STORED,
            Status::IncrDecrOnNonNumericValue => STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE,
            Status::VBucketBelongsToOtherServer => STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER,
            Status::AuthenticationError => STATUS_AUTHENTICATION_ERROR,
            Status::AuthenticationContinue => STATUS_AUTHENTICATION_CONTINUE,
            Status::UnknownCommand => STATUS_UNKNOWN_COMMAND,
            Status::OutOfMemory => STATUS_OUT_OF_MEMORY,
            Status::NotSupported => STATUS_NOT_SUPPORTED,
            Status::InternalError => STATUS_INTERNAL_ERROR,
            Status::Busy => STATUS_BUSY,
            Status::TemporaryFailure => STATUS_TEMPORARY_FAILURE,
        }
    }

    /// Generate a Status from binary code
    pub fn from_code(code: u16) -> Option<Status> {
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
            _ => None
        }
    }

    /// Get a short description
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
        }
    }
}

#[deriving(Clone, Show, Eq, PartialEq)]
pub enum Command {
    Get,
    Set,
    Add,
    Replace,
    Delete,
    Increment,
    Decrement,
    Quit,
    Flush,
    GetQuietly,
    Noop,
    Version,
    GetKey,
    GetKeyQuietly,
    Append,
    Prepend,
    Stat,
    SetQuietly,
    AddQuietly,
    ReplaceQuietly,
    DeleteQuietly,
    IncrementQuietly,
    DecrementQuietly,
    QuitQuietly,
    FlushQuietly,
    AppendQuietly,
    PrependQuietly,
    Verbosity,
    Touch,
    GetAndTouch,
    GetAndTouchQuietly,
    SaslListMechanisms,
    SaslAuthenticate,
    SaslStep,
    RGet,
    RSet,
    RSetQuietly,
    RAppend,
    RAppendQuietly,
    RPrepend,
    RPrependQuietly,
    RDelete,
    RDeleteQuietly,
    RIncrement,
    RIncrementQuietly,
    RDecrement,
    RDecrementQuietly,
    SetVBucket,
    GetVBucket,
    DelVBucket,
    TapConnect,
    TapMutation,
    TapDelete,
    TapFlush,
    TapOpaque,
    TapVBucketSet,
    TapCheckpointStart,
    TapCheckpointEnd,
}

impl Command {
    fn code(&self) -> u8 {
        match *self {
            Command::Get => OPCODE_GET,
            Command::Set => OPCODE_SET,
            Command::Add => OPCODE_ADD,
            Command::Replace => OPCODE_REPLACE,
            Command::Delete => OPCODE_DEL,
            Command::Increment => OPCODE_INCR,
            Command::Decrement => OPCODE_DECR,
            Command::Quit => OPCODE_QUIT,
            Command::Flush => OPCODE_FLUSH,
            Command::GetQuietly => OPCODE_GETQ,
            Command::Noop => OPCODE_NOP,
            Command::Version => OPCODE_VERSION,
            Command::GetKey => OPCODE_GETK,
            Command::GetKeyQuietly => OPCODE_GETKQ,
            Command::Append => OPCODE_APPEND,
            Command::Prepend => OPCODE_PREPEND,
            Command::Stat => OPCODE_STAT,
            Command::SetQuietly => OPCODE_SETQ,
            Command::AddQuietly => OPCODE_ADDQ,
            Command::ReplaceQuietly => OPCODE_REPLACEQ,
            Command::DeleteQuietly => OPCODE_DELQ,
            Command::IncrementQuietly => OPCODE_INCRQ,
            Command::DecrementQuietly => OPCODE_DECRQ,
            Command::QuitQuietly => OPCODE_QUITQ,
            Command::FlushQuietly => OPCODE_FLUSHQ,
            Command::AppendQuietly => OPCODE_APPENDQ,
            Command::PrependQuietly => OPCODE_PREPENDQ,
            Command::Verbosity => OPCODE_VERBOSITY,
            Command::Touch => OPCODE_TOUCH,
            Command::GetAndTouch => OPCODE_GAT,
            Command::GetAndTouchQuietly => OPCODE_GATQ,
            Command::SaslListMechanisms => OPCODE_SASL_LIST_MECHS,
            Command::SaslAuthenticate => OPCODE_SASL_AUTH,
            Command::SaslStep => OPCODE_SASL_STEP,
            Command::RGet => OPCODE_RGET,
            Command::RSet => OPCODE_RSET,
            Command::RSetQuietly => OPCODE_RSETQ,
            Command::RAppend => OPCODE_RAPPEND,
            Command::RAppendQuietly => OPCODE_RAPPENDQ,
            Command::RPrepend => OPCODE_RPREPEND,
            Command::RPrependQuietly => OPCODE_RPREPENDQ,
            Command::RDelete => OPCODE_RDEL,
            Command::RDeleteQuietly => OPCODE_RDELQ,
            Command::RIncrement => OPCODE_RINCR,
            Command::RIncrementQuietly => OPCODE_RINCRQ,
            Command::RDecrement => OPCODE_RDECR,
            Command::RDecrementQuietly => OPCODE_RDECRQ,
            Command::SetVBucket => OPCODE_SET_VBUCKET,
            Command::GetVBucket => OPCODE_GET_VBUCKET,
            Command::DelVBucket => OPCODE_DEL_VBUCKET,
            Command::TapConnect => OPCODE_TAP_CONNECT,
            Command::TapMutation => OPCODE_TAP_MUTATION,
            Command::TapDelete => OPCODE_TAP_DEL,
            Command::TapFlush => OPCODE_TAP_FLUSH,
            Command::TapOpaque => OPCODE_TAP_OPAQUE,
            Command::TapVBucketSet => OPCODE_TAP_VBUCKET_SET,
            Command::TapCheckpointStart => OPCODE_TAP_CHECKPOINT_START,
            Command::TapCheckpointEnd => OPCODE_TAP_CHECKPOINT_END,
        }
    }

    fn from_code(code: u8) -> Option<Command> {
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

#[deriving(Clone, Show, Eq, PartialEq)]
pub enum DataType {
    RawBytes,
}

impl DataType {
    fn code(&self) -> u8 {
        match *self {
            DataType::RawBytes => DATA_TYPE_RAW_BYTES,
        }
    }

    fn from_code(code: u8) -> Option<DataType> {
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
#[deriving(Clone, Show)]
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
    pub fn new(cmd: Command, dtype: DataType, vbid: u16, opaque: u32, cas: u64) -> RequestHeader {
        RequestHeader {
            command: cmd,
            key_len: 0,
            extra_len: 0,
            data_type: dtype,
            vbucket_id: vbid,
            body_len: 0,
            opaque: opaque,
            cas: cas,
        }
    }

    pub fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        try!(writer.write_u8(MAGIC_REQUEST));
        try!(writer.write_u8(self.command.code()));
        try!(writer.write_be_u16(self.key_len));
        try!(writer.write_u8(self.extra_len));
        try!(writer.write_u8(self.data_type.code()));
        try!(writer.write_be_u16(self.vbucket_id));
        try!(writer.write_be_u32(self.body_len));
        try!(writer.write_be_u32(self.opaque));
        try!(writer.write_be_u64(self.cas));

        Ok(())
    }

    pub fn read_from(reader: &mut Reader) -> IoResult<RequestHeader> {
        let magic = try!(reader.read_u8());

        if magic != MAGIC_REQUEST {
            return Err(make_io_error("Invalid magic", None));
        }

        Ok(RequestHeader {
            command: match Command::from_code(try!(reader.read_u8())) {
                Some(c) => c,
                None => return Err(make_io_error("Invalid command", None)),
            },
            key_len: try!(reader.read_be_u16()),
            extra_len: try!(reader.read_u8()),
            data_type: match DataType::from_code(try!(reader.read_u8())) {
                Some(d) => d,
                None => return Err(make_io_error("Invalid data type", None))
            },
            vbucket_id: try!(reader.read_be_u16()),
            body_len: try!(reader.read_be_u32()),
            opaque: try!(reader.read_be_u32()),
            cas: try!(reader.read_be_u64()),
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
#[deriving(Clone, Show)]
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
    pub fn new(cmd: Command, dtype: DataType, status: Status, opaque: u32, cas: u64) -> ResponseHeader {
        ResponseHeader {
            command: cmd,
            key_len: 0,
            extra_len: 0,
            data_type: dtype,
            status: status,
            body_len: 0,
            opaque: opaque,
            cas: cas,
        }
    }

    pub fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        try!(writer.write_u8(MAGIC_RESPONSE));
        try!(writer.write_u8(self.command.code()));
        try!(writer.write_be_u16(self.key_len));
        try!(writer.write_u8(self.extra_len));
        try!(writer.write_u8(self.data_type.code()));
        try!(writer.write_be_u16(self.status.code()));
        try!(writer.write_be_u32(self.body_len));
        try!(writer.write_be_u32(self.opaque));
        try!(writer.write_be_u64(self.cas));

        Ok(())
    }

    pub fn read_from(reader: &mut Reader) -> IoResult<ResponseHeader> {
        let magic = try!(reader.read_u8());

        if magic != MAGIC_RESPONSE {
            return Err(make_io_error("Invalid magic", None));
        }

        Ok(ResponseHeader {
            command: match Command::from_code(try!(reader.read_u8())) {
                Some(c) => c,
                None => return Err(make_io_error("Invalid command", None)),
            },
            key_len: try!(reader.read_be_u16()),
            extra_len: try!(reader.read_u8()),
            data_type: match DataType::from_code(try!(reader.read_u8())) {
                Some(d) => d,
                None => return Err(make_io_error("Invalid data type", None))
            },
            status: match Status::from_code(try!(reader.read_be_u16())) {
                Some(s) => s,
                None => return Err(make_io_error("Invalid status", None)),
            },
            body_len: try!(reader.read_be_u32()),
            opaque: try!(reader.read_be_u32()),
            cas: try!(reader.read_be_u64()),
        })
    }
}

#[deriving(Clone, Show)]
pub struct RequestPacket {
    pub header: RequestHeader,
    pub extra: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl RequestPacket {
    pub fn new(header: RequestHeader, extra: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> RequestPacket {
        RequestPacket {
            header: header,
            extra: extra,
            key: key,
            value: value,
        }
    }

    pub fn write_to(&mut self, writer: &mut Writer) -> IoResult<()> {
        self.header.key_len = self.key.len() as u16;
        self.header.extra_len = self.extra.len() as u8;
        self.header.body_len = (self.key.len() + self.extra.len() + self.value.len()) as u32;

        try!(self.header.write_to(writer));
        try!(writer.write(self.extra.as_slice()));
        try!(writer.write(self.key.as_slice()));
        try!(writer.write(self.value.as_slice()));

        Ok(())
    }

    pub fn read_from(reader: &mut Reader) -> IoResult<RequestPacket> {
        let header = try!(RequestHeader::read_from(reader));

        let value_len = header.body_len as uint - header.extra_len as uint - header.key_len as uint;

        Ok(RequestPacket {
            header: header,
            extra: try!(reader.read_exact(header.extra_len as uint)),
            key: try!(reader.read_exact(header.key_len as uint)),
            value: try!(reader.read_exact(value_len)),
        })
    }
}

#[deriving(Clone, Show)]
pub struct ResponsePacket {
    pub header: ResponseHeader,
    pub extra: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ResponsePacket {
    pub fn new(header: ResponseHeader, extra: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> ResponsePacket {
        ResponsePacket {
            header: header,
            extra: extra,
            key: key,
            value: value,
        }
    }

    pub fn write_to(&mut self, writer: &mut Writer) -> IoResult<()> {
        self.header.key_len = self.key.len() as u16;
        self.header.extra_len = self.extra.len() as u8;
        self.header.body_len = (self.key.len() + self.extra.len() + self.value.len()) as u32;

        try!(self.header.write_to(writer));
        try!(writer.write(self.extra.as_slice()));
        try!(writer.write(self.key.as_slice()));
        try!(writer.write(self.value.as_slice()));

        Ok(())
    }

    pub fn read_from(reader: &mut Reader) -> IoResult<ResponsePacket> {
        let header = try!(ResponseHeader::read_from(reader));

        let value_len = header.body_len as uint - header.extra_len as uint - header.key_len as uint;

        Ok(ResponsePacket {
            header: header,
            extra: try!(reader.read_exact(header.extra_len as uint)),
            key: try!(reader.read_exact(header.key_len as uint)),
            value: try!(reader.read_exact(value_len)),
        })
    }
}

fn make_io_error(desc: &'static str, detail: Option<String>) -> IoError {
    IoError {
        kind: OtherIoError,
        desc: desc,
        detail: detail,
    }
}

#[cfg(test)]
mod test {
    use std::io::net::tcp::TcpStream;
    use std::io::BufferedStream;

    use proto;
    use proto::binarydef::{RequestHeader, RequestPacket, ResponsePacket, Command, DataType};

    fn test_stream() -> TcpStream {
        TcpStream::connect("127.0.0.1:11211").unwrap()
    }

    #[test]
    fn test_binary_protocol() {
        let mut stream = BufferedStream::new(test_stream());

        {
            let req_header = RequestHeader::new(Command::Set, DataType::RawBytes, 0, 0, 0);
            let mut req_packet = RequestPacket::new(
                                req_header,
                                vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x0e, 0x10],
                                b"test:binary_proto:hello".to_vec(),
                                b"world".to_vec());

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == proto::binary::Status::NoError);
        }

        {
            let req_header = RequestHeader::new(Command::Get, DataType::RawBytes, 0, 0, 0);
            let mut req_packet = RequestPacket::new(
                                req_header,
                                vec![],
                                b"test:binary_proto:hello".to_vec(),
                                vec![]);

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == proto::binary::Status::NoError);
            assert_eq!(resp_packet.value.as_slice(), b"world");
        }

        {
            let req_header = RequestHeader::new(Command::Delete, DataType::RawBytes, 0, 0, 0);
            let mut req_packet = RequestPacket::new(
                                req_header,
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
