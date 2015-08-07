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

use std::net::TcpStream;
use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use std::ops::Deref;
use std::path::Path;

use bufstream::BufStream;
use conhash::{ConsistentHash, Node};

#[cfg(unix)]
use unix_socket::UnixStream;

use proto::{Proto, Operation, NoReplyOperation, CasOperation};
use proto::{MemCachedResult, self};

struct Server {
    pub proto: Box<Proto + Send>,
    addr: String,
}

impl Server {
    fn connect(addr: &str, protocol: proto::ProtoType) -> io::Result<Server> {
        let mut split = addr.split("://");

        Ok(Server {
            proto: match protocol {
                proto::ProtoType::Binary => {
                    match (split.next(), split.next()) {
                        (Some("tcp"), Some(addr)) => {
                            let stream = try!(TcpStream::connect(addr));
                            Box::new(proto::BinaryProto::new(BufStream::new(stream))) as Box<Proto + Send>
                        },
                        #[cfg(unix)]
                        (Some("unix"), Some(addr)) => {
                            let stream = try!(UnixStream::connect(&Path::new(addr)));
                            Box::new(proto::BinaryProto::new(BufStream::new(stream))) as Box<Proto + Send>
                        },
                        (Some(prot), _) => {
                            panic!("Unsupported protocol: {}", prot);
                        },
                        _ => panic!("Malformed address"),
                    }
                }
            },
            addr: addr.to_owned(),
        })
    }
}

#[derive(Clone)]
struct ServerRef(Rc<RefCell<Server>>);

impl Node for ServerRef {
    fn name(&self) -> String {
        self.0.borrow().addr.clone()
    }
}

impl Deref for ServerRef {
    type Target = Rc<RefCell<Server>>;

    fn deref(&self) -> &Rc<RefCell<Server>> {
        &self.0
    }
}

// impl Clone for Server {
//     fn clone(&self) -> Server {
//         Server { proto: self.proto.clone() }
//     }
// }

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
/// assert_eq!(&value[..], b"Bar");
/// assert_eq!(flags, 0xdeadbeef);
///
/// client.set_noreply(b"key:dontreply", b"1", 0x00000001, 20).unwrap();
///
/// let (_, cas_val) = client.increment_cas(b"key:numerical", 10, 1, 20, 0).unwrap();
/// client.increment_cas(b"key:numerical", 1, 1, 20, cas_val).unwrap();
/// ```
pub struct Client {
    servers: ConsistentHash<ServerRef>,
}

impl Client {
    /// Connect to Memcached servers
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn connect(svrs: &[(&str, usize)], p: proto::ProtoType) -> io::Result<Client> {

        assert!(!svrs.is_empty(), "Server list should not be empty");

        let mut servers = ConsistentHash::new();
        for &(addr, weight) in svrs.iter() {
            let svr = try!(Server::connect(addr, p));
            servers.add(&ServerRef(Rc::new(RefCell::new(svr))), weight);
        }

        Ok(Client {
            servers: servers,
        })
    }

    fn find_server_by_key<'a>(&'a mut self, key: &[u8]) -> &'a mut ServerRef {
        self.servers.get_mut(key).expect("No valid server found")
    }
}

impl Operation for Client {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.set(key, value, flags, expiration)
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.add(key, value, flags, expiration)
    }

    fn delete(&mut self, key: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.delete(key)
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.replace(key, value, flags, expiration)
    }

    fn get(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.get(key)
    }

    fn getk(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32)> {
       let server = self.find_server_by_key(key);
       server.borrow_mut().proto.getk(key)
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.increment(key, amount, initial, expiration)
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.increment(key, amount, initial, expiration)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.append(key, value)
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.prepend(key, value)
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.touch(key, expiration)
    }
}

impl NoReplyOperation for Client {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.set_noreply(key, value, flags, expiration)
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.add_noreply(key, value, flags, expiration)
    }

    fn delete_noreply(&mut self, key: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.delete_noreply(key)
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.replace_noreply(key, value, flags, expiration)
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.increment_noreply(key, amount, initial, expiration)
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.decrement_noreply(key, amount, initial, expiration)
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.append_noreply(key, value)
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.prepend_noreply(key, value)
    }
}

impl CasOperation for Client {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.set_cas(key, value, flags, expiration, cas)
    }

    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.add_cas(key, value, flags, expiration)
    }

    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.replace_cas(key, value, flags, expiration, cas)
    }

    fn get_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.get_cas(key)
    }

    fn getk_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.getk_cas(key)
    }

    fn increment_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.increment_cas(key, amount, initial, expiration, cas)
    }

    fn decrement_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.decrement_cas(key, amount, initial, expiration, cas)
    }

    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.append_cas(key, value, cas)
    }

    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.prepend_cas(key, value, cas)
    }

    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> MemCachedResult<u64> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.touch_cas(key, expiration, cas)
    }
}

#[cfg(all(test, feature = "nightly"))]
mod test {
    use test::Bencher;
    use client::Client;
    use proto::{Operation, NoReplyOperation, ProtoType};
    use rand::random;

    fn generate_data(len: usize) -> Vec<u8> {
        (0..len).map(|_| random()).collect()
    }

    #[bench]
    fn bench_set_64(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(64);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_noreply_64(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(64);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_512(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(512);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_noreply_512(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(512);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_1024(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(1024);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_noreply_1024(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(1024);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_4096(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(4096);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_noreply_4096(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(4096);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_16384(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(16384);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set(key, &val[..], 0, 2));
    }

    #[bench]
    fn bench_set_noreply_16384(b: &mut Bencher) {
        let key = b"test:test_bench";
        let val = generate_data(16384);

        let mut client = Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary).unwrap();

        b.iter(|| client.set_noreply(key, &val[..], 0, 2));
    }
}
