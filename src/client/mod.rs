// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

//! Memcached client

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::ops::Deref;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use conhash::{ConsistentHash, Node};

use bufstream::BufStream;

#[cfg(unix)]
use unix_socket::UnixStream;

use crate::proto::{self, AuthResponse, MemCachedResult};
use crate::proto::{CasOperation, MultiOperation, NoReplyOperation, Operation, Proto};

struct Sasl<'a> {
    username: &'a str,
    password: &'a str,
}

struct ConnectOpts {
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

struct Server {
    pub proto: Box<dyn Proto + Send>,
    addr: String,
}

impl Server {
    fn connect(
        addr: String,
        protocol: proto::ProtoType,
        o_sasl: &Option<Sasl>,
        connect_opts: &Option<ConnectOpts>,
    ) -> io::Result<Server> {
        let proto = {
            let mut split = addr.split("://");
            match protocol {
                proto::ProtoType::Binary => match (split.next(), split.next()) {
                    (Some("tcp"), Some(addr)) => {
                        let stream = match connect_opts.as_ref().and_then(|opts| opts.connect_timeout) {
                            Some(timeout) => {
                                let socket_addr: SocketAddr = addr.to_socket_addrs()?.next().unwrap();
                                TcpStream::connect_timeout(&socket_addr, timeout)?
                            }
                            None => TcpStream::connect(addr)?,
                        };
                        if let Some(opts) = &connect_opts {
                            stream.set_read_timeout(opts.read_timeout)?;
                            stream.set_write_timeout(opts.write_timeout)?;
                        }
                        stream.set_nodelay(true)?;
                        let mut proto =
                            Box::new(proto::BinaryProto::new(BufStream::new(stream))) as Box<dyn Proto + Send>;
                        if let Some(sasl) = o_sasl {
                            let auth_str = format!("\x00{}\x00{}", sasl.username, sasl.password);
                            match proto.auth_start("PLAIN", auth_str.as_bytes()) {
                                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
                                Ok(AuthResponse::Succeeded) => (),
                                Ok(resp) => {
                                    let msg = format!("SASL auth failed with AuthResponse: {:?}", resp);
                                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                                }
                            }
                        }
                        proto
                    }
                    #[cfg(unix)]
                    (Some("unix"), Some(addr)) => {
                        let stream = UnixStream::connect(&Path::new(addr))?;
                        if let Some(opts) = &connect_opts {
                            stream.set_read_timeout(opts.read_timeout)?;
                            stream.set_write_timeout(opts.write_timeout)?;
                        }
                        Box::new(proto::BinaryProto::new(BufStream::new(stream))) as Box<dyn Proto + Send>
                    }
                    (Some(prot), _) => {
                        panic!("Unsupported protocol: {}", prot);
                    }
                    _ => panic!("Malformed address"),
                },
            }
        };
        Ok(Server { proto, addr })
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
/// use memcached::client::{Client};
/// use memcached::proto::{CasOperation, MultiOperation, NoReplyOperation, Operation, ProtoType};
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
    pub fn connect<S: ToString>(svrs: &[(S, usize)], p: proto::ProtoType) -> io::Result<Client> {
        Client::conn(svrs, p, None, None)
    }

    /// Connect to Memcached servers with connect and/or IO timeouts
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn connect_with_opts<S: ToString>(
        svrs: &[(S, usize)],
        p: proto::ProtoType,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Client> {
        Client::conn(
            svrs,
            p,
            None,
            Some(ConnectOpts {
                connect_timeout,
                read_timeout,
                write_timeout,
            }),
        )
    }

    /// Connect to Memcached servers that require SASL authentication
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn connect_sasl<S: ToString>(
        svrs: &[(S, usize)],
        p: proto::ProtoType,
        username: &str,
        password: &str,
    ) -> io::Result<Client> {
        Client::conn(svrs, p, Some(Sasl { username, password }), None)
    }

    /// Connect to Memcached servers that require SASL authentication with connect and/or I/O timeouts
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn connect_sasl_with_opts<S: ToString>(
        svrs: &[(S, usize)],
        p: proto::ProtoType,
        username: &str,
        password: &str,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Client> {
        Client::conn(
            svrs,
            p,
            Some(Sasl { username, password }),
            Some(ConnectOpts {
                connect_timeout,
                read_timeout,
                write_timeout,
            }),
        )
    }

    fn conn<S: ToString>(
        svrs: &[(S, usize)],
        p: proto::ProtoType,
        sasl: Option<Sasl>,
        opts: Option<ConnectOpts>,
    ) -> io::Result<Client> {
        assert!(!svrs.is_empty(), "Server list should not be empty");

        let mut servers = ConsistentHash::new();
        for (addr, weight) in svrs.iter() {
            let svr = Server::connect(addr.to_string(), p, &sasl, &opts)?;
            servers.add(&ServerRef(Rc::new(RefCell::new(svr))), *weight);
        }

        Ok(Client { servers })
    }

    fn find_server_by_key(&mut self, key: &[u8]) -> &mut ServerRef {
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
        server
            .borrow_mut()
            .proto
            .increment_noreply(key, amount, initial, expiration)
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        let server = self.find_server_by_key(key);
        server
            .borrow_mut()
            .proto
            .decrement_noreply(key, amount, initial, expiration)
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
        server
            .borrow_mut()
            .proto
            .replace_cas(key, value, flags, expiration, cas)
    }

    fn get_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.get_cas(key)
    }

    fn getk_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32, u64)> {
        let server = self.find_server_by_key(key);
        server.borrow_mut().proto.getk_cas(key)
    }

    fn increment_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server
            .borrow_mut()
            .proto
            .increment_cas(key, amount, initial, expiration, cas)
    }

    fn decrement_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)> {
        let server = self.find_server_by_key(key);
        server
            .borrow_mut()
            .proto
            .decrement_cas(key, amount, initial, expiration, cas)
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

impl MultiOperation for Client {
    fn set_multi(&mut self, kv: BTreeMap<&[u8], (&[u8], u32, u32)>) -> MemCachedResult<()> {
        assert_eq!(self.servers.len(), 1);
        let server = self.find_server_by_key(kv.keys().next().unwrap());
        server.borrow_mut().proto.set_multi(kv)
    }
    fn delete_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<()> {
        assert_eq!(self.servers.len(), 1);
        let server = self.find_server_by_key(keys[0]);
        server.borrow_mut().proto.delete_multi(keys)
    }
    fn increment_multi<'a>(
        &mut self,
        kv: HashMap<&'a [u8], (u64, u64, u32)>,
    ) -> MemCachedResult<HashMap<&'a [u8], u64>> {
        assert_eq!(self.servers.len(), 1);
        let server = self.find_server_by_key(kv.keys().next().unwrap());
        server.borrow_mut().proto.increment_multi(kv)
    }
    fn get_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<HashMap<Vec<u8>, (Vec<u8>, u32)>> {
        assert_eq!(self.servers.len(), 1);
        let server = self.find_server_by_key(keys[0]);
        server.borrow_mut().proto.get_multi(keys)
    }
}

#[cfg(all(test, feature = "nightly"))]
mod test {
    use super::Client;
    use crate::proto::{NoReplyOperation, Operation, ProtoType};
    use test::Bencher;

    fn generate_data(len: usize) -> Vec<u8> {
        (0..len).map(|_| fastrand::u8(..)).collect()
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
