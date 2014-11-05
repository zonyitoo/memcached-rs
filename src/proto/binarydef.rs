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
//   0/ HEADER                                                        /
//   /                                                               /
//   /                                                               /
//   /                                                               /
//   +---------------+---------------+---------------+---------------+
//   24/ COMMAND-SPECIFIC EXTRAS (as needed)                           /
//   +/  (note length in the extras length header field)              /
//   +---------------+---------------+---------------+---------------+
//   m/ Key (as needed)                                               /
//   +/  (note length in key length header field)                     /
//   +---------------+---------------+---------------+---------------+
//   n/ Value (as needed)                                             /
//   +/  (note length is total body length header field, minus        /
//   +/   sum of the extras and key length body fields)               /
//   +---------------+---------------+---------------+---------------+
//   Total 24 bytes

#![allow(dead_code)]

use std::io::{Writer, Reader, IoResult, IoError, OtherIoError};

pub const MAGIC_REQUEST: u8 = 0x80;
pub const MAGIC_RESPONSE: u8 = 0x81;

pub const STATUS_NO_ERROR: u16 = 0x0000;
pub const STATUS_KEY_NOT_FOUND: u16 = 0x0001;
pub const STATUS_KEY_EXISTS: u16 = 0x0002;
pub const STATUS_VALUE_TOO_LARGE: u16 = 0x0003;
pub const STATUS_INVALID_ARGUMENTS: u16 = 0x0004;
pub const STATUS_ITEM_NOT_STORED: u16 = 0x0005;
pub const STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE: u16 = 0x0006;
pub const STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER: u16 = 0x0007;
pub const STATUS_AUTHENTICATION_ERROR: u16 = 0x0008;
pub const STATUS_AUTHENTICATION_CONTINUE: u16 = 0x0009;
pub const STATUS_UNKNOWN_COMMAND: u16 = 0x0081;
pub const STATUS_OUT_OF_MEMORY: u16 = 0x0082;
pub const STATUS_NOT_SUPPORTED: u16 = 0x0083;
pub const STATUS_INTERNAL_ERROR: u16 = 0x0084;
pub const STATUS_BUSY: u16 = 0x0085;
pub const STATUS_TEMPORARY_FAILURE: u16 = 0x0086;

pub const OPCODE_GET: u8 = 0x00;
pub const OPCODE_SET: u8 = 0x01;
pub const OPCODE_ADD: u8 = 0x02;
pub const OPCODE_REPLACE: u8 = 0x03;
pub const OPCODE_DEL: u8 = 0x04;
pub const OPCODE_INCR: u8 = 0x05;
pub const OPCODE_DECR: u8 = 0x06;
pub const OPCODE_QUIT: u8 = 0x07;
pub const OPCODE_FLUSH: u8 = 0x08;
pub const OPCODE_GETQ: u8 = 0x09;
pub const OPCODE_NOP: u8 = 0x0A;
pub const OPCODE_VERSION: u8 = 0x0B;
pub const OPCODE_GETK: u8 = 0x0C;
pub const OPCODE_GETKQ: u8 = 0x0D;
pub const OPCODE_APPEND: u8 = 0x0E;
pub const OPCODE_PREPEND: u8 = 0x0F;
pub const OPCODE_STAT: u8 = 0x10;
pub const OPCODE_SETQ: u8 = 0x11;
pub const OPCODE_ADDQ: u8 = 0x12;
pub const OPCODE_REPLACEQ: u8 = 0x13;
pub const OPCODE_DELQ: u8 = 0x14;
pub const OPCODE_INCRQ: u8 = 0x15;
pub const OPCODE_DECRQ: u8 = 0x16;
pub const OPCODE_QUITQ: u8 = 0x17;
pub const OPCODE_FLUSHQ: u8 = 0x18;
pub const OPCODE_APPENDQ: u8 = 0x19;
pub const OPCODE_PREPENDQ: u8 = 0x1A;
pub const OPCODE_VERBOSITY: u8 = 0x1B;
pub const OPCODE_TOUCH: u8 = 0x1C;
pub const OPCODE_GAT: u8 = 0x1D;
pub const OPCODE_GATQ: u8 = 0x1E;
pub const OPCODE_SASL_LIST_MECHS: u8 = 0x20;
pub const OPCODE_SASL_AUTH: u8 = 0x21;
pub const OPCODE_SASL_STEP: u8 = 0x22;
pub const OPCODE_RGET: u8 = 0x30;
pub const OPCODE_RSET: u8 = 0x31;
pub const OPCODE_RSETQ: u8 = 0x32;
pub const OPCODE_RAPPEND: u8 = 0x33;
pub const OPCODE_RAPPENDQ: u8 = 0x34;
pub const OPCODE_RPREPEND: u8 = 0x35;
pub const OPCODE_RPREPENDQ: u8 = 0x36;
pub const OPCODE_RDEL: u8 = 0x37;
pub const OPCODE_RDELQ: u8 = 0x38;
pub const OPCODE_RINCR: u8 = 0x39;
pub const OPCODE_RINCRQ: u8 = 0x3A;
pub const OPCODE_RDECR: u8 = 0x3B;
pub const OPCODE_RDECRQ: u8 = 0x3C;
pub const OPCODE_SET_VBUCKET: u8 = 0x3D;
pub const OPCODE_GET_VBUCKET: u8 = 0x3E;
pub const OPCODE_DEL_VBUCKET: u8 = 0x3F;
pub const OPCODE_TAP_CONNECT: u8 = 0x40;
pub const OPCODE_TAP_MUTATION: u8 = 0x41;
pub const OPCODE_TAP_DEL: u8 = 0x42;
pub const OPCODE_TAP_FLUSH: u8 = 0x43;
pub const OPCODE_TAP_OPAQUE: u8 = 0x44;
pub const OPCODE_TAP_VBUCKET_SET: u8 = 0x45;
pub const OPCODE_TAP_CHECKPOINT_START: u8 = 0x46;
pub const OPCODE_TAP_CHECKPOINT_END: u8 = 0x47;

pub const DATA_TYPE_RAW_BYTES: u8 = 0x00;

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
    fn code(&self) -> u16 {
        match *self {
            NoError => STATUS_NO_ERROR,
            KeyNotFound => STATUS_KEY_NOT_FOUND,
            KeyExists => STATUS_KEY_EXISTS,
            ValueTooLarge => STATUS_VALUE_TOO_LARGE,
            InvalidArguments => STATUS_INVALID_ARGUMENTS,
            ItemNotStored => STATUS_ITEM_NOT_STORED,
            IncrDecrOnNonNumericValue => STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE,
            VBucketBelongsToOtherServer => STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER,
            AuthenticationError => STATUS_AUTHENTICATION_ERROR,
            AuthenticationContinue => STATUS_AUTHENTICATION_CONTINUE,
            UnknownCommand => STATUS_UNKNOWN_COMMAND,
            OutOfMemory => STATUS_OUT_OF_MEMORY,
            NotSupported => STATUS_NOT_SUPPORTED,
            InternalError => STATUS_INTERNAL_ERROR,
            Busy => STATUS_BUSY,
            TemporaryFailure => STATUS_TEMPORARY_FAILURE,
        }
    }

    fn from_code(code: u16) -> Option<Status> {
        match code {
            STATUS_NO_ERROR => Some(NoError),
            STATUS_KEY_NOT_FOUND => Some(KeyNotFound),
            STATUS_KEY_EXISTS => Some(KeyExists),
            STATUS_VALUE_TOO_LARGE => Some(ValueTooLarge),
            STATUS_INVALID_ARGUMENTS => Some(InvalidArguments),
            STATUS_ITEM_NOT_STORED => Some(ItemNotStored),
            STATUS_INCR_OR_DECR_ON_NON_NUMERIC_VALUE => Some(IncrDecrOnNonNumericValue),
            STATUS_VBUCKET_BELONGS_TO_OTHER_SERVER => Some(VBucketBelongsToOtherServer),
            STATUS_AUTHENTICATION_ERROR => Some(AuthenticationError),
            STATUS_AUTHENTICATION_CONTINUE => Some(AuthenticationContinue),
            STATUS_UNKNOWN_COMMAND => Some(UnknownCommand),
            STATUS_OUT_OF_MEMORY => Some(OutOfMemory),
            STATUS_NOT_SUPPORTED => Some(NotSupported),
            STATUS_INTERNAL_ERROR => Some(InternalError),
            STATUS_BUSY => Some(Busy),
            STATUS_TEMPORARY_FAILURE => Some(TemporaryFailure),
            _ => None
        }
    }

    pub fn desc(&self) -> &'static str {
        match *self {
            NoError => "no error",
            KeyNotFound => "key not found",
            KeyExists => "key exists",
            ValueTooLarge => "value too large",
            InvalidArguments => "invalid argument",
            ItemNotStored => "item not stored",
            IncrDecrOnNonNumericValue => "incr or decr on non numeric value",
            VBucketBelongsToOtherServer => "vbucket belongs to other server",
            AuthenticationError => "authentication error",
            AuthenticationContinue => "authentication continue",
            UnknownCommand => "unknown command",
            OutOfMemory => "out of memory",
            NotSupported => "not supported",
            InternalError => "internal error",
            Busy => "busy",
            TemporaryFailure => "temporary failure",
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
    GetQuiet,
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
            Get => OPCODE_GET,
            Set => OPCODE_SET,
            Add => OPCODE_ADD,
            Replace => OPCODE_REPLACE,
            Delete => OPCODE_DEL,
            Increment => OPCODE_INCR,
            Decrement => OPCODE_DECR,
            Quit => OPCODE_QUIT,
            Flush => OPCODE_FLUSH,
            GetQuiet => OPCODE_GETQ,
            Noop => OPCODE_NOP,
            Version => OPCODE_VERSION,
            GetKey => OPCODE_GETK,
            GetKeyQuietly => OPCODE_GETKQ,
            Append => OPCODE_APPEND,
            Prepend => OPCODE_PREPEND,
            Stat => OPCODE_STAT,
            SetQuietly => OPCODE_SETQ,
            AddQuietly => OPCODE_ADDQ,
            ReplaceQuietly => OPCODE_REPLACEQ,
            DeleteQuietly => OPCODE_DELQ,
            IncrementQuietly => OPCODE_INCRQ,
            DecrementQuietly => OPCODE_DECRQ,
            QuitQuietly => OPCODE_QUITQ,
            FlushQuietly => OPCODE_FLUSHQ,
            AppendQuietly => OPCODE_APPENDQ,
            PrependQuietly => OPCODE_PREPENDQ,
            Verbosity => OPCODE_VERBOSITY,
            Touch => OPCODE_TOUCH,
            GetAndTouch => OPCODE_GAT,
            GetAndTouchQuietly => OPCODE_GATQ,
            SaslListMechanisms => OPCODE_SASL_LIST_MECHS,
            SaslAuthenticate => OPCODE_SASL_AUTH,
            SaslStep => OPCODE_SASL_STEP,
            RGet => OPCODE_RGET,
            RSet => OPCODE_RSET,
            RSetQuietly => OPCODE_RSETQ,
            RAppend => OPCODE_RAPPEND,
            RAppendQuietly => OPCODE_RAPPENDQ,
            RPrepend => OPCODE_RPREPEND,
            RPrependQuietly => OPCODE_RPREPENDQ,
            RDelete => OPCODE_RDEL,
            RDeleteQuietly => OPCODE_RDELQ,
            RIncrement => OPCODE_RINCR,
            RIncrementQuietly => OPCODE_RINCRQ,
            RDecrement => OPCODE_RDECR,
            RDecrementQuietly => OPCODE_RDECRQ,
            SetVBucket => OPCODE_SET_VBUCKET,
            GetVBucket => OPCODE_GET_VBUCKET,
            DelVBucket => OPCODE_DEL_VBUCKET,
            TapConnect => OPCODE_TAP_CONNECT,
            TapMutation => OPCODE_TAP_MUTATION,
            TapDelete => OPCODE_TAP_DEL,
            TapFlush => OPCODE_TAP_FLUSH,
            TapOpaque => OPCODE_TAP_OPAQUE,
            TapVBucketSet => OPCODE_TAP_VBUCKET_SET,
            TapCheckpointStart => OPCODE_TAP_CHECKPOINT_START,
            TapCheckpointEnd => OPCODE_TAP_CHECKPOINT_END,
        }
    }

    fn from_code(code: u8) -> Option<Command> {
        match code {
            OPCODE_GET => Some(Get),
            OPCODE_SET => Some(Set),
            OPCODE_ADD => Some(Add),
            OPCODE_REPLACE => Some(Replace),
            OPCODE_DEL => Some(Delete),
            OPCODE_INCR => Some(Increment),
            OPCODE_DECR => Some(Decrement),
            OPCODE_QUIT => Some(Quit),
            OPCODE_FLUSH => Some(Flush),
            OPCODE_GETQ => Some(GetQuiet),
            OPCODE_NOP => Some(Noop),
            OPCODE_VERSION => Some(Version),
            OPCODE_GETK => Some(GetKey),
            OPCODE_GETKQ => Some(GetKeyQuietly),
            OPCODE_APPEND => Some(Append),
            OPCODE_PREPEND => Some(Prepend),
            OPCODE_STAT => Some(Stat),
            OPCODE_SETQ => Some(SetQuietly),
            OPCODE_ADDQ => Some(AddQuietly),
            OPCODE_REPLACEQ => Some(ReplaceQuietly),
            OPCODE_DELQ => Some(DeleteQuietly),
            OPCODE_INCRQ => Some(IncrementQuietly),
            OPCODE_DECRQ => Some(DecrementQuietly),
            OPCODE_QUITQ => Some(QuitQuietly),
            OPCODE_FLUSHQ => Some(FlushQuietly),
            OPCODE_APPENDQ => Some(AppendQuietly),
            OPCODE_PREPENDQ => Some(PrependQuietly),
            OPCODE_VERBOSITY => Some(Verbosity),
            OPCODE_TOUCH => Some(Touch),
            OPCODE_GAT => Some(GetAndTouch),
            OPCODE_GATQ => Some(GetAndTouchQuietly),
            OPCODE_SASL_LIST_MECHS => Some(SaslListMechanisms),
            OPCODE_SASL_AUTH => Some(SaslAuthenticate),
            OPCODE_SASL_STEP => Some(SaslStep),
            OPCODE_RGET => Some(RGet),
            OPCODE_RSET => Some(RSet),
            OPCODE_RSETQ => Some(RSetQuietly),
            OPCODE_RAPPEND => Some(RAppend),
            OPCODE_RAPPENDQ => Some(RAppendQuietly),
            OPCODE_RPREPEND => Some(RPrepend),
            OPCODE_RPREPENDQ => Some(RPrependQuietly),
            OPCODE_RDEL => Some(RDelete),
            OPCODE_RDELQ => Some(RDeleteQuietly),
            OPCODE_RINCR => Some(RIncrement),
            OPCODE_RINCRQ => Some(RIncrementQuietly),
            OPCODE_RDECR => Some(RDecrement),
            OPCODE_RDECRQ => Some(RDecrementQuietly),
            OPCODE_SET_VBUCKET => Some(SetVBucket),
            OPCODE_GET_VBUCKET => Some(GetVBucket),
            OPCODE_DEL_VBUCKET => Some(DelVBucket),
            OPCODE_TAP_CONNECT => Some(TapConnect),
            OPCODE_TAP_MUTATION => Some(TapMutation),
            OPCODE_TAP_DEL => Some(TapDelete),
            OPCODE_TAP_FLUSH => Some(TapFlush),
            OPCODE_TAP_OPAQUE => Some(TapOpaque),
            OPCODE_TAP_VBUCKET_SET => Some(TapVBucketSet),
            OPCODE_TAP_CHECKPOINT_START => Some(TapCheckpointStart),
            OPCODE_TAP_CHECKPOINT_END => Some(TapCheckpointEnd),
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
            RawBytes => DATA_TYPE_RAW_BYTES,
        }
    }

    fn from_code(code: u8) -> Option<DataType> {
        match code {
            DATA_TYPE_RAW_BYTES => Some(RawBytes),
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

    use proto::binarydef;

    fn test_stream() -> TcpStream {
        TcpStream::connect("127.0.0.1", 11211).unwrap()
    }

    #[test]
    fn test_binary_protocol() {
        let mut stream = BufferedStream::new(test_stream());

        {
            let req_header = binarydef::RequestHeader::new(binarydef::Set, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x0e, 0x10],
                                b"hello".to_vec(),
                                b"world".to_vec());

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = binarydef::ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == binarydef::NoError);
        }

        {
            let req_header = binarydef::RequestHeader::new(binarydef::Get, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                vec![],
                                b"hello".to_vec(),
                                vec![]);

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = binarydef::ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == binarydef::NoError);
            assert_eq!(resp_packet.value.as_slice(), b"world");
        }

        {
            let req_header = binarydef::RequestHeader::new(binarydef::Delete, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                vec![],
                                b"hello".to_vec(),
                                vec![]);

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = binarydef::ResponsePacket::read_from(&mut stream).unwrap();

            assert!(resp_packet.header.status == binarydef::NoError);
        }
    }
}
