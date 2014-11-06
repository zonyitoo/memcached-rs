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

use std::io::net::ip::Port;
use std::io::net::tcp::TcpStream;
use std::io::IoResult;
use std::collections::TreeMap;

use crc32::Crc32;

use proto::{Proto, Operation, MultiOperation, ServerOperation, NoReplyOperation, Error, mod};

struct Server {
    pub proto: Box<Proto + Send>,
}

impl Server {
    fn connect(addr: &str, port: Port, protocol: proto::ProtoType) -> IoResult<Server> {
        Ok(Server {
            proto: match protocol {
                proto::Binary => {
                    let stream = try!(TcpStream::connect(addr, port));
                    box proto::BinaryProto::new(stream) as Box<Proto + Send>
                }
            }
        })
    }
}

/// Memcached client
///
/// ```no_run
/// use memcached::client::Client;
/// use memcached::proto::{Operation, Binary};
///
/// let mut client = Client::connect([("127.0.0.1", 11211, Binary, 1)]);
/// client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
/// let (value, flags) = client.get(b"Foo").unwrap();
///
/// assert_eq!(value.as_slice(), b"Bar");
/// assert_eq!(flags, 0xdeadbeef);
/// ```
pub struct Client {
    servers: Vec<Server>,
    key_hasher: Crc32,
    bucket: Vec<uint>,
}

impl Client {
    pub fn connect(svrs: &[(&str, Port, proto::ProtoType, uint)]) -> Client {
        let mut servers = Vec::new();
        let mut bucket = Vec::new();
        for &(addr, port, p, weight) in svrs.iter() {
            servers.push(Server::connect(addr, port, p).unwrap_or_else(|err| {
                panic!("Cannot connect server {}:{}: {}", addr, port, err);
            }));

            for _ in range(0, weight) {
                bucket.push(servers.len() - 1);
            }
        }
        Client {
            servers: servers,
            key_hasher: Crc32::new(),
            bucket: bucket,
        }
    }

    fn find_server_by_key<'a>(&'a mut self, key: &[u8]) -> &'a mut Server {
        let hash = (self.key_hasher.crc(key) >> 16) & 0x7fff;
        let idx = (hash as uint) % self.bucket.len();
        &mut self.servers[self.bucket[idx]]
    }

    pub fn flush_all(&mut self) -> Result<(), Error> {
        for s in self.servers.iter_mut() {
            try!(s.proto.flush(0));
        }
        Ok(())
    }
}

impl Operation for Client {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.set(key, value, flags, expiration)
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.add(key, value, flags, expiration)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.delete(key)
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.replace(key, value, flags, expiration)
    }

    fn get(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32), Error> {
        let server = self.find_server_by_key(key);
        server.proto.get(key)
    }

    fn getk(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32), Error> {
       let server = self.find_server_by_key(key);
       server.proto.getk(key)
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.proto.increment(key, amount, initial, expiration)
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.proto.increment(key, amount, initial, expiration)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.append(key, value)
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.prepend(key, value)
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.touch(key, expiration)
    }
}

impl MultiOperation for Client {
    fn set_multi(&mut self, kv: TreeMap<&[u8], (&[u8], u32, u32)>) -> Result<Vec<Result<(), Error>>, Error> {
        unimplemented!();
    }

    fn delete_multi(&mut self, keys: &[&[u8]]) -> Result<(), Error> {
        unimplemented!();
    }

    fn get_multi(&mut self, keys: &[&[u8]]) -> Result<TreeMap<Vec<u8>, (Vec<u8>, u32)>, Error> {
        unimplemented!();
    }
}

impl NoReplyOperation for Client {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.set_noreply(key, value, flags, expiration)
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.add_noreply(key, value, flags, expiration)
    }

    fn delete_noreply(&mut self, key: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.delete_noreply(key)
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.replace_noreply(key, value, flags, expiration)
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.increment_noreply(key, amount, initial, expiration)
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.decrement_noreply(key, amount, initial, expiration)
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.append_noreply(key, value)
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.proto.prepend_noreply(key, value)
    }
}
