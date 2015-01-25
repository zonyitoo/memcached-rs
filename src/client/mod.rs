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

//! Memcached client

use std::io::net::tcp::TcpStream;
use std::io::net::pipe::UnixStream;
use std::io::{IoResult, IoError, OtherIoError};

use crc32::Crc32;

use proto::{Proto, Operation, ServerOperation, NoReplyOperation, CasOperation};
use proto::{MemCachedResult, self};

struct Server {
    pub proto: Box<Proto + Send>,
}

impl Server {
    fn connect(addr: &str, protocol: proto::ProtoType) -> IoResult<Server> {
        let mut split = addr.split_str("://");

        Ok(Server {
            proto: match protocol {
                proto::ProtoType::Binary => {
                    match (split.next(), split.next()) {
                        (Some("tcp"), Some(addr)) => {
                            let stream = try!(TcpStream::connect(addr));
                            box proto::BinaryProto::new(stream) as Box<Proto + Send>
                        },
                        (Some("unix"), Some(addr)) => {
                            let stream = try!(UnixStream::connect(&Path::new(addr)));
                            box proto::BinaryProto::new(stream) as Box<Proto + Send>
                        },
                        (Some(prot), _) => {
                            panic!("Unsupported protocol: {}", prot);
                        },
                        _ => panic!("Malformed address"),
                    }
                }
            }
        })
    }
}

impl Clone for Server {
    fn clone(&self) -> Server {
        Server { proto: self.proto.clone() }
    }
}

/// Memcached client
///
/// ```ignore
/// extern crate collect;
///
/// use collect::BTreeMap;
/// use memcached::client::{Client, AddrType};
/// use memcached::proto::{Operation, MultiOperation, NoReplyOperation, CasOperation, ProtoType};
///
/// let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();
///
/// client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
/// let (value, flags) = client.get(b"Foo").unwrap();
/// assert_eq!(value.as_slice(), b"Bar");
/// assert_eq!(flags, 0xdeadbeef);
///
/// client.set_noreply(b"key:dontreply", b"1", 0x00000001, 20).unwrap();
///
/// let (_, cas_val) = client.increment_cas(b"key:numerical", 10, 1, 20, 0).unwrap();
/// client.increment_cas(b"key:numerical", 1, 1, 20, cas_val).unwrap();
/// ```
pub struct Client {
    servers: Vec<Box<Server>>,
    key_hasher: Crc32,
    bucket: Vec<usize>,
}

impl Client {
    /// Connect to Memcached servers
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn connect(svrs: &[(&str, usize)], p: proto::ProtoType) -> IoResult<Client> {
        if svrs.is_empty() {
            return Err(IoError {
                kind: OtherIoError,
                desc: "Empty server list",
                detail: None,
            })
        }
        let mut servers = Vec::with_capacity(svrs.len());
        let mut bucket = Vec::new();
        for &(addr, weight) in svrs.iter() {
            servers.push(box try!(Server::connect(addr, p)));

            for _ in range(0, weight) {
                bucket.push(servers.len() - 1);
            }
        }
        Ok(Client {
            servers: servers,
            key_hasher: Crc32::new(),
            bucket: bucket,
        })
    }

    fn find_server_index_by_key(&mut self, key: &[u8]) -> usize {
        let hash = (self.key_hasher.crc(key) >> 16) & 0x7fff;
        self.bucket[(hash as usize) % self.bucket.len()]
    }

    fn find_server_by_key(&mut self, key: &[u8]) -> &mut Box<Server> {
        let idx = self.find_server_index_by_key(key);
        &mut self.servers[idx]
    }

    /// Flush data in all servers
    pub fn flush_all(&mut self) -> MemCachedResult<()> {
        for s in self.servers.iter_mut() {
            try!(s.proto.flush(0));
        }
        Ok(())
    }
}

impl Operation for Client {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.set(key, value, flags, expiration)
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.add(key, value, flags, expiration)
    }

    fn delete(&mut self, key: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.delete(key)
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.replace(key, value, flags, expiration)
    }

    fn get(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32)> {
        let server = self.find_server_by_key(key);
        server.proto.get(key)
    }

    fn getk(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32)> {
       let server = self.find_server_by_key(key);
       server.proto.getk(key)
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.increment(key, amount, initial, expiration)
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.increment(key, amount, initial, expiration)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.append(key, value)
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.prepend(key, value)
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.touch(key, expiration)
    }
}

impl NoReplyOperation for Client {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.set_noreply(key, value, flags, expiration)
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.add_noreply(key, value, flags, expiration)
    }

    fn delete_noreply(&mut self, key: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.delete_noreply(key)
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.replace_noreply(key, value, flags, expiration)
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.increment_noreply(key, amount, initial, expiration)
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.decrement_noreply(key, amount, initial, expiration)
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.append_noreply(key, value)
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.proto.prepend_noreply(key, value)
    }
}

impl CasOperation for Client {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.set_cas(key, value, flags, expiration, cas)
    }

    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.add_cas(key, value, flags, expiration)
    }

    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.replace_cas(key, value, flags, expiration, cas)
    }

    fn get_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.proto.get_cas(key)
    }

    fn getk_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.proto.getk_cas(key)
    }

    fn increment_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server.proto.increment_cas(key, amount, initial, expiration, cas)
    }

    fn decrement_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server.proto.decrement_cas(key, amount, initial, expiration, cas)
    }

    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.append_cas(key, value, cas)
    }

    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.prepend_cas(key, value, cas)
    }

    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.proto.touch_cas(key, expiration, cas)
    }
}

#[cfg(test)]
mod test {
    use test::Bencher;
    use client::Client;
    use proto::{Operation, NoReplyOperation, ProtoType};
    use std::rand::random;

    fn generate_data(len: usize) -> Vec<u8> {
        range(0, len).map(|_| random()).collect()
    }

    #[bench]
    fn bench_set_64(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(64);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_noreply_64(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(64);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_512(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(512);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_noreply_512(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(512);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_1024(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(1024);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_noreply_1024(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(1024);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_4096(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(4096);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_noreply_4096(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(4096);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_16384(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(16384);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, val.as_slice(), 0, 2));
    }

    #[bench]
    fn bench_set_noreply_16384(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(16384);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, val.as_slice(), 0, 2));
    }
}
