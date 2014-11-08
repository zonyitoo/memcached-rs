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

use std::io::{BufferedStream, MemWriter, BufReader};
use std::io::net::tcp::TcpStream;
use std::string::String;
use std::str;
use std::collections::TreeMap;
use std::rand::random;

use proto::{Operation, MultiOperation, ServerOperation, NoReplyOperation, CasOperation};
use proto::{Error, OtherError, binarydef, mod};
use version::Version;

macro_rules! try_response(
    ($packet:expr) => ({
        let pk = $packet;
        match pk.header.status {
            proto::NoError => {
                pk
            }
            _ => {
                use proto::MemCachedError;
                return Err(Error::new(MemCachedError(pk.header.status),
                                      pk.header.status.desc(),
                                      match String::from_utf8(pk.value) {
                                          Ok(s) => Some(s),
                                          Err(..) => None,
                                      }))
            }
        }
    });
    ($packet:expr, ignore: $($ignored:pat)|+) => ({
        let pk = $packet;
        match pk.header.status {
            $(
                $ignored => { pk }
            )+
            _ => {
                use proto::MemCachedError;
                return Err(Error::new(MemCachedError(pk.header.status),
                                      pk.header.status.desc(),
                                      match String::from_utf8(pk.value) {
                                          Ok(s) => Some(s),
                                          Err(..) => None,
                                      }))
            }
        }
    });
)

macro_rules! try_io(
    ($do_io:expr) => ( {
        let io_result = $do_io;
        match io_result {
            Ok(ret) => { ret },
            Err(err) => {
                use proto::IoError;
                return Err(Error::new(IoError(err.kind), err.desc, err.detail));
            }
        }
    });
)

pub struct BinaryProto {
    stream: BufferedStream<TcpStream>,
}

impl proto::Proto for BinaryProto {
    fn clone(&self) -> Box<proto::Proto + Send> {
        box BinaryProto { stream: BufferedStream::new(self.stream.get_ref().clone()) }
    }
}

impl BinaryProto {
    pub fn new(stream: TcpStream) -> BinaryProto {
        BinaryProto {
            stream: BufferedStream::new(stream),
        }
    }

    fn send_noop(&mut self) -> Result<u32, Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Noop, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                Vec::new(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(opaque)
    }
}

impl Operation for BinaryProto {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Set, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);
        Ok(())
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Add, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Delete, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);
        Ok(())
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Replace, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Get, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut extrabufr = BufReader::new(resp.extra.as_slice());
        let flags = try_io!(extrabufr.read_be_u32());

        Ok((resp.value, flags))
    }

    fn getk(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::GetKey, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut extrabufr = BufReader::new(resp.extra.as_slice());
        let flags = try_io!(extrabufr.read_be_u32());

        Ok((resp.key, resp.value, flags))
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Increment, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut bufr = BufReader::new(resp.value.as_slice());
        Ok(try_io!(bufr.read_be_u64()))
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Decrement, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut bufr = BufReader::new(resp.value.as_slice());
        Ok(try_io!(bufr.read_be_u64()))
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Append, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);

        Ok(())
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Prepend, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);

        Ok(())
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(4);
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Touch, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);

        Ok(())
    }
}

impl ServerOperation for BinaryProto {
    fn quit(&mut self) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Quit, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                Vec::new(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);

        Ok(())
    }

    fn flush(&mut self, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(4);
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Flush, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                Vec::new(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);
        Ok(())
    }

    fn noop(&mut self) -> Result<(), Error> {
        let opaque = try!(self.send_noop());
        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        try_response!(resp_packet);

        Ok(())
    }

    fn version(&mut self) -> Result<Version, Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Version, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                Vec::new(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }

        let val = try_response!(resp_packet).value;
        let verstr = match str::from_utf8(val.as_slice()) {
            Some(vs) => vs,
            None => return Err(Error::new(OtherError, "Response is not a string", None)),
        };

        Ok(match from_str(verstr) {
            Some(v) => v,
            None => return Err(Error::new(OtherError, "Unrecognized version string", None)),
        })
    }

    fn stat(&mut self) -> Result<TreeMap<String, String>, Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Stat, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                Vec::new(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut result = TreeMap::new();
        loop {
            let resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
            if resp_packet.header.opaque != opaque {
                continue;
            }
            let resp = try_response!(resp_packet);

            if resp.key.len() == 0 && resp.value.len() == 0 {
                break;
            }

            let key = match String::from_utf8(resp.key) {
                Ok(k) => k,
                Err(..) => return Err(Error::new(OtherError, "Key is not a string", None)),
            };

            let val = match String::from_utf8(resp.value) {
                Ok(k) => k,
                Err(..) => return Err(Error::new(OtherError, "Value is not a string", None)),
            };

            result.insert(key, val);
        }

        Ok(result)
    }
}

impl MultiOperation for BinaryProto {
    fn set_multi(&mut self, kv: TreeMap<Vec<u8>, (Vec<u8>, u32, u32)>) -> Result<(), Error> {
        for (key, (value, flags, expiration)) in kv.into_iter() {
            let mut extra_buf = MemWriter::with_capacity(8);
            try_io!(extra_buf.write_be_u32(flags));
            try_io!(extra_buf.write_be_u32(expiration));

            let req_header = binarydef::RequestHeader::new(binarydef::SetQuietly, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                    req_header,
                                    extra_buf.unwrap(),
                                    key,
                                    value);

            try_io!(req_packet.write_to(&mut self.stream));
        }
        try!(self.send_noop());

        loop {
            let resp = try_response!(try_io!(binarydef::ResponsePacket::read_from(&mut self.stream)));
            if resp.header.command == binarydef::Noop {
                return Ok(())
            }
        }
    }

    fn delete_multi(&mut self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        for key in keys.into_iter() {
            let req_header = binarydef::RequestHeader::new(binarydef::DeleteQuietly, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                    req_header,
                                    Vec::new(),
                                    key,
                                    Vec::new());

            try_io!(req_packet.write_to(&mut self.stream));
        }
        try!(self.send_noop());

        loop {
            let resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
            let resp = try_response!(resp_packet, ignore: proto::NoError | proto::KeyNotFound);

            if resp.header.command == binarydef::Noop {
                return Ok(());
            }
        }
    }

    fn get_multi(&mut self, keys: Vec<Vec<u8>>) -> Result<TreeMap<Vec<u8>, (Vec<u8>, u32)>, Error> {

        for key in keys.into_iter() {
            let req_header = binarydef::RequestHeader::new(binarydef::GetKeyQuietly, binarydef::RawBytes, 0, 0, 0);
            let mut req_packet = binarydef::RequestPacket::new(
                                    req_header,
                                    Vec::new(),
                                    key,
                                    Vec::new());

            try_io!(req_packet.write_to(&mut self.stream));
        }
        try!(self.send_noop());

        let mut result = TreeMap::new();
        loop {
            let resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
            let resp = try_response!(resp_packet);

            if resp.header.command == binarydef::Noop {
                return Ok(result);
            }

            let mut extrabufr = BufReader::new(resp.extra.as_slice());
            let flags = try_io!(extrabufr.read_be_u32());

            result.insert(resp.key, (resp.value, flags));
        }
    }
}

impl NoReplyOperation for BinaryProto {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::SetQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::AddQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn delete_noreply(&mut self, key: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::DeleteQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::ReplaceQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::IncrementQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::DecrementQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::AppendQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::PrependQuietly, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        Ok(())
    }
}

impl CasOperation for BinaryProto {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Set, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);
        Ok(resp.header.cas)
    }

    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Add, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);
        Ok(resp.header.cas)
    }

    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(8);
        try_io!(extra_buf.write_be_u32(flags));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Replace, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);
        Ok(resp.header.cas)
    }

    fn get_cas(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32, u64), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Get, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut extrabufr = BufReader::new(resp.extra.as_slice());
        let flags = try_io!(extrabufr.read_be_u32());

        Ok((resp.value, flags, resp.header.cas))
    }

    fn getk_cas(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32, u64), Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::GetKey, binarydef::RawBytes, 0, opaque, 0);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut extrabufr = BufReader::new(resp.extra.as_slice());
        let flags = try_io!(extrabufr.read_be_u32());

        Ok((resp.key, resp.value, flags, resp.header.cas))
    }

    fn increment_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> Result<(u64, u64), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Increment, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut bufr = BufReader::new(resp.value.as_slice());
        Ok((try_io!(bufr.read_be_u64()), resp.header.cas))
    }

    fn decrement_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> Result<(u64, u64), Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(20);
        try_io!(extra_buf.write_be_u64(amount));
        try_io!(extra_buf.write_be_u64(initial));
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Decrement, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        let mut bufr = BufReader::new(resp.value.as_slice());
        Ok((try_io!(bufr.read_be_u64()), cas))
    }

    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Append, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        Ok(resp.header.cas)
    }

    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let req_header = binarydef::RequestHeader::new(binarydef::Prepend, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                Vec::new(),
                                key.to_vec(),
                                value.to_vec());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        Ok(resp.header.cas)
    }

    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> Result<u64, Error> {
        let opaque = random::<u32>();
        let mut extra_buf = MemWriter::with_capacity(4);
        try_io!(extra_buf.write_be_u32(expiration));

        let req_header = binarydef::RequestHeader::new(binarydef::Touch, binarydef::RawBytes, 0, opaque, cas);
        let mut req_packet = binarydef::RequestPacket::new(
                                req_header,
                                extra_buf.unwrap(),
                                key.to_vec(),
                                Vec::new());

        try_io!(req_packet.write_to(&mut self.stream));
        try_io!(self.stream.flush());

        let mut resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        while resp_packet.header.opaque != opaque {
            resp_packet = try_io!(binarydef::ResponsePacket::read_from(&mut self.stream));
        }
        let resp = try_response!(resp_packet);

        Ok(resp.header.cas)
    }
}

#[cfg(test)]
mod test {
    use std::io::net::tcp::TcpStream;
    use std::collections::TreeMap;
    use proto::{Operation, MultiOperation, ServerOperation, NoReplyOperation, CasOperation, BinaryProto};

    const SERVER_ADDR: &'static str = "127.0.0.1:11211";

    fn get_client() -> BinaryProto {
        let stream = TcpStream::connect(SERVER_ADDR).unwrap();
        BinaryProto::new(stream)
    }

    #[test]
    fn test_set_get_delete() {
        let mut client = get_client();
        assert!(client.set(b"test:Hello", b"world", 0xdeadbeef, 120).is_ok());

        let get_resp = client.get(b"test:Hello");
        assert!(get_resp.is_ok());
        assert_eq!(get_resp.unwrap(), (b"world".to_vec(), 0xdeadbeef));

        let getk_resp = client.getk(b"test:Hello");
        assert!(getk_resp.is_ok());
        assert_eq!(getk_resp.unwrap(), (b"test:Hello".to_vec(), b"world".to_vec(), 0xdeadbeef));

        assert!(client.delete(b"test:Hello").is_ok());
    }

    #[test]
    fn test_incr_decr() {
        let mut client = get_client();
        {
            let incr_resp = client.increment(b"test:incr", 1, 0, 120);
            assert!(incr_resp.is_ok());
            assert_eq!(incr_resp.unwrap(), 0);
        }

        {
            let incr_resp = client.increment(b"test:incr", 10, 0, 120);
            assert!(incr_resp.is_ok());
            assert_eq!(incr_resp.unwrap(), 10);
        }

        {
            let decr_resp = client.decrement(b"test:incr", 5, 0, 120);
            assert!(decr_resp.is_ok());
            assert_eq!(decr_resp.unwrap(), 5);
        }

        {
            let decr_resp = client.decrement(b"test:incr", 20, 0, 120);
            assert!(decr_resp.is_ok());
            assert_eq!(decr_resp.unwrap(), 0);
        }

        assert!(client.delete(b"test:incr").is_ok())
    }

    #[test]
    fn test_version() {
        let mut client = get_client();
        assert!(client.version().is_ok());
    }

    #[test]
    fn test_noop() {
        let mut client = get_client();
        assert!(client.noop().is_ok());
    }

    #[test]
    #[should_fail]
    fn test_quit() {
        let mut client = get_client();
        assert!(client.quit().is_ok());

        client.noop().unwrap();
    }

    #[test]
    fn test_flush() {
        let mut client = get_client();
        assert!(client.flush(2).is_ok());
    }

    #[test]
    fn test_add() {
        let mut client = get_client();

        {
            let add_resp = client.add(b"test:add_key", b"initial", 0xdeadbeef, 120);
            assert!(add_resp.is_ok());
        }

        {
            let get_resp = client.get(b"test:add_key");
            assert!(get_resp.is_ok());

            assert_eq!(get_resp.unwrap(), (b"initial".to_vec(), 0xdeadbeef));
            let add_resp = client.add(b"test:add_key", b"added", 0xdeadbeef, 120);
            assert!(add_resp.is_err());
        }

        assert!(client.delete(b"test:add_key").is_ok());
    }

    #[test]
    fn test_replace() {
        let mut client = get_client();

        {
            let rep_resp = client.replace(b"test:replace_key", b"replaced", 0xdeadbeef, 120);
            assert!(rep_resp.is_err());
        }

        {
            let add_resp = client.add(b"test:replace_key", b"just_add", 0xdeadbeef, 120);
            assert!(add_resp.is_ok());
            let rep_resp = client.replace(b"test:replace_key", b"replaced", 0xdeadbeef, 120);
            assert!(rep_resp.is_ok());
            assert!(client.delete(b"test:replace_key").is_ok());
        }
    }

    #[test]
    fn test_append_prepend() {
        let mut client = get_client();
        {
            let app_resp = client.append(b"test:append_key", b"appended");
            assert!(app_resp.is_err());
            let pre_resp = client.prepend(b"test:append_key", b"prepended");
            assert!(pre_resp.is_err());
        }

        {
            let add_resp = client.add(b"test:append_key", b"just_add", 0xdeadbeef, 120);
            assert!(add_resp.is_ok());

            let app_resp = client.append(b"test:append_key", b"appended");
            assert!(app_resp.is_ok());
            let get_resp = client.get(b"test:append_key");
            assert!(get_resp.is_ok());
            assert_eq!(get_resp.unwrap(), (b"just_addappended".to_vec(), 0xdeadbeef));

            let pre_resp = client.prepend(b"test:append_key", b"prepended");
            assert!(pre_resp.is_ok());
            let get_resp = client.get(b"test:append_key");
            assert!(get_resp.is_ok());
            assert_eq!(get_resp.unwrap(), (b"prependedjust_addappended".to_vec(), 0xdeadbeef));
        }

        assert!(client.delete(b"test:append_key").is_ok());
    }

    #[test]
    fn test_stat() {
        let mut client = get_client();
        let stat_resp = client.stat();
        assert!(stat_resp.is_ok());
    }

    #[test]
    fn test_touch() {
        let mut client = get_client();

        let touch_resp = client.touch(b"test:touch", 120);
        assert!(touch_resp.is_err());

        let add_resp = client.add(b"test:touch", b"val", 0xcafebabe, 100);
        assert!(add_resp.is_ok());

        let touch_resp = client.touch(b"test:touch", 120);
        assert!(touch_resp.is_ok());

        assert!(client.delete(b"test:touch").is_ok());
    }

    #[test]
    fn test_set_get_delete_muti() {
        let mut client = get_client();

        let mut data = TreeMap::new();
        data.insert(b"test:multi_hello1".to_vec(), (b"world1".to_vec(), 0xdeadbeef, 120));
        data.insert(b"test:multi_hello2".to_vec(), (b"world2".to_vec(), 0xdeadbeef, 120));
        data.insert(b"test:multi_lastone".to_vec(), (b"last!".to_vec(), 0xdeadbeef, 120));

        let set_resp = client.set_multi(data);
        assert!(set_resp.is_ok());

        let get_resp = client.get_multi(vec![b"test:multi_hello1".to_vec(),
                                             b"test:multi_hello2".to_vec(),
                                             b"test:multi_lastone".to_vec()]);
        assert!(get_resp.is_ok());

        let get_resp_map = get_resp.as_ref().unwrap();
        assert_eq!(get_resp_map.find(&b"test:multi_hello1".to_vec()),
                   Some(&(b"world1".to_vec(), 0xdeadbeef)));
        assert_eq!(get_resp_map.find(&b"test:multi_hello2".to_vec()),
                   Some(&(b"world2".to_vec(), 0xdeadbeef)));
        assert_eq!(get_resp_map.find(&b"test:multi_lastone".to_vec()),
                   Some(&(b"last!".to_vec(), 0xdeadbeef)))

        let del_resp = client.delete_multi(vec![b"test:multi_hello1".to_vec(),
                                                b"test:multi_hello2".to_vec()]);
        assert!(del_resp.is_ok());

        let get_resp = client.get_multi(vec![b"test:multi_hello1".to_vec(),
                                             b"test:multi_hello2".to_vec(),
                                             b"test:multi_lastone".to_vec()]);
        assert!(get_resp.is_ok());

        let get_resp_map = get_resp.as_ref().unwrap();
        assert_eq!(get_resp_map.find(&b"test:multi_hello1".to_vec()),
                   None);
        assert_eq!(get_resp_map.find(&b"test:multi_hello2".to_vec()),
                   None);
        assert_eq!(get_resp_map.find(&b"test:multi_lastone".to_vec()),
                   Some(&(b"last!".to_vec(), 0xdeadbeef)))

        let del_resp = client.delete_multi(vec![b"lastone".to_vec(),
                                                b"not_exists!!!!".to_vec()]);
        assert!(del_resp.is_ok());
    }

    #[test]
    fn test_set_add_replace_noreply() {
        let key = b"test:noreply_key";
        let set_val = b"value";
        let add_val = b"just add";
        let rep_val = b"replaced";

        let mut client = get_client();

        let add_resp = client.add_noreply(key, add_val, 0xdeadbeef, 120);
        assert!(add_resp.is_ok());

        let get_resp = client.get(key);
        assert!(get_resp.is_ok());
        assert_eq!(get_resp.unwrap(), (add_val.to_vec(), 0xdeadbeef));

        let set_resp = client.set_noreply(key, set_val, 0xdeadbeef, 120);
        assert!(set_resp.is_ok());

        let get_resp = client.get(key);
        assert!(get_resp.is_ok());
        assert_eq!(get_resp.unwrap(), (set_val.to_vec(), 0xdeadbeef));

        let rep_resp = client.replace_noreply(key, rep_val, 0xcafebabe, 120);
        assert!(rep_resp.is_ok());

        let get_resp = client.get(key);
        assert!(get_resp.is_ok());
        assert_eq!(get_resp.unwrap(), (rep_val.to_vec(), 0xcafebabe));

        assert!(client.delete(key).is_ok());
    }

    #[test]
    fn test_set_add_replace_cas() {
        let key = b"test:cas_key";
        let set_val = b"value";
        let add_val = b"just add";
        let rep_val = b"replaced";

        let mut client = get_client();

        let add_resp = client.add_cas(key, add_val, 0xdeadbeef, 120);
        assert!(add_resp.is_ok());
        let add_cas = add_resp.unwrap();

        {
            let set_resp = client.set_cas(key, set_val, 0xdeadbeef, 120, add_cas + 1);
            assert!(set_resp.is_err());

            let get_resp = client.get_cas(key);
            assert!(get_resp.is_ok());
            let (_, _, get_cas) = get_resp.unwrap();
            assert_eq!(get_cas, add_cas);

            let rep_resp = client.replace_cas(key, rep_val, 0xdeadbeef, 120, add_cas + 1);
            assert!(rep_resp.is_err());
        }

        {
            let set_resp = client.set_cas(key, set_val, 0xdeadbeef, 120, add_cas);
            assert!(set_resp.is_ok());
            let set_cas = set_resp.unwrap();

            let get_resp = client.get_cas(key);
            assert!(get_resp.is_ok());
            let (_, _, get_cas) = get_resp.unwrap();
            assert_eq!(get_cas, set_cas);

            let rep_resp = client.replace_cas(key, rep_val, 0xdeadbeef, 120, set_cas);
            assert!(rep_resp.is_ok());
        }

        assert!(client.delete(key).is_ok());
    }

    #[test]
    fn test_incr_decr_cas() {
        let key = b"test:incr_decr_cas";
        let mut client = get_client();

        let incr_resp = client.increment_cas(key, 0, 100, 120, 0);
        assert!(incr_resp.is_ok());
        let (_, incr_cas) = incr_resp.unwrap();

        let incr_resp = client.increment_cas(key, 0, 10, 120, incr_cas + 1);
        assert!(incr_resp.is_err());

        let incr_resp = client.increment_cas(key, 0, 10, 120, incr_cas);
        assert!(incr_resp.is_ok());
        let (_, incr_cas) = incr_resp.unwrap();

        let decr_resp = client.decrement_cas(key, 0, 10, 120, incr_cas + 1);
        assert!(decr_resp.is_err());

        let decr_resp = client.decrement_cas(key, 0, 10, 120, incr_cas);
        assert!(decr_resp.is_ok());

        assert!(client.delete(key).is_ok());
    }

    #[test]
    fn test_append_prepend_cas() {
        let key = b"test:append_prepend_cas";
        let mut client = get_client();

        let set_resp = client.set_cas(key, b"appended", 0, 120, 0);
        assert!(set_resp.is_ok());
        let set_cas = set_resp.unwrap();

        let ap_resp = client.append_cas(key, b"appended", set_cas + 1);
        assert!(ap_resp.is_err());

        let ap_resp = client.append_cas(key, b"appended", set_cas);
        assert!(ap_resp.is_ok());
        let ap_cas = ap_resp.unwrap();

        let pr_resp = client.prepend_cas(key, b"prepend", ap_cas + 1);
        assert!(pr_resp.is_err());

        let pr_resp = client.prepend_cas(key, b"prepend", ap_cas);
        assert!(pr_resp.is_ok());

        assert!(client.delete(key).is_ok());
    }

    #[test]
    fn test_if_noreply_failed() {
        let key = b"test:noreply_fail_key";
        let set_val = b"value";
        let add_val = b"just add";

        let mut client = get_client();

        let set_resp = client.set_noreply(key, set_val, 0xdeadbeef, 120);
        assert!(set_resp.is_ok());

        // Should failed, because key is already set
        let add_resp = client.add_noreply(key, add_val, 0xdeadbeef, 120);
        assert!(add_resp.is_ok());

        let get_resp = client.get(key);
        assert!(get_resp.is_ok());
        assert_eq!(get_resp.unwrap(), (set_val.to_vec(), 0xdeadbeef));
    }
}
