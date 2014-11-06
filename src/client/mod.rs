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

use std::io::net::ip::Port;
use std::io::net::tcp::TcpStream;
use std::io::IoResult;
use std::collections::{HashMap, TreeMap};
use std::collections::hash_map::{Occupied, Vacant};
use std::sync::{Arc, Mutex};

use crc32::Crc32;

use proto::{Proto, Operation, MultiOperation, ServerOperation, NoReplyOperation, CasOperation};
use proto::{Error, mod};

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
    servers: Vec<Arc<Mutex<Server>>>,
    key_hasher: Crc32,
    bucket: Vec<uint>,
}

impl Client {
    /// Connect to Memcached servers
    ///
    /// This function accept multiple servers, servers information should be represented
    /// as a array of tuples in this form
    ///
    /// `(address, port, ProtoType, weight)`.
    pub fn connect(svrs: &[(&str, Port, proto::ProtoType, uint)]) -> Client {
        let mut servers = Vec::new();
        let mut bucket = Vec::new();
        for &(addr, port, p, weight) in svrs.iter() {
            servers.push(Arc::new(Mutex::new(Server::connect(addr, port, p).unwrap_or_else(|err| {
                panic!("Cannot connect server {}:{}: {}", addr, port, err);
            }))));

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

    fn find_server_index_by_key(&mut self, key: &[u8]) -> uint {
        let hash = (self.key_hasher.crc(key) >> 16) & 0x7fff;
        self.bucket[(hash as uint) % self.bucket.len()]
    }

    fn find_server_by_key(&mut self, key: &[u8]) -> Arc<Mutex<Server>> {
        let idx = self.find_server_index_by_key(key);
        self.servers[idx].clone()
    }

    /// Flush data in all servers
    pub fn flush_all(&mut self) -> Result<(), Error> {
        for s in self.servers.iter_mut() {
            try!(s.lock().proto.flush(0));
        }
        Ok(())
    }
}

impl Operation for Client {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.set(key, value, flags, expiration)
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.add(key, value, flags, expiration)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.delete(key)
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.replace(key, value, flags, expiration)
    }

    fn get(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.get(key)
    }

    fn getk(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32), Error> {
       let server = self.find_server_by_key(key);
       server.lock().proto.getk(key)
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.increment(key, amount, initial, expiration)
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.increment(key, amount, initial, expiration)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.append(key, value)
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.prepend(key, value)
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.touch(key, expiration)
    }
}

impl MultiOperation for Client {
    fn set_multi(&mut self, kv: TreeMap<Vec<u8>, (Vec<u8>, u32, u32)>) -> Result<(), Error> {
        let sk = {
            let mut svrkey: HashMap<uint, TreeMap<Vec<u8>, (Vec<u8>, u32, u32)>> = HashMap::new();
            for (key, v) in kv.into_iter() {
                let svr_idx = self.find_server_index_by_key(key.as_slice());

                match svrkey.entry(svr_idx) {
                    Occupied(entry) => {
                        entry.into_mut().insert(key, v);
                    },
                    Vacant(entry) => {
                        let mut t = TreeMap::new();
                        t.insert(key, v);
                        entry.set(t);
                    }
                }
            }
            svrkey
        };

        let mut chans = Vec::new();
        for (svr_idx, v) in sk.into_iter() {
            let (tx2, rx2) = channel();
            let svr = self.servers[svr_idx].clone();
            spawn(proc() {
                let r = svr.lock().proto.set_multi(v);
                tx2.send(r);
                drop(tx2);
            });
            chans.push(rx2);
        }

        let mut result = Ok(());
        for chan in chans.into_iter() {
            result = result.or(chan.recv());
        }

        result
    }

    fn delete_multi(&mut self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        let sk = {
            let mut svrkey: HashMap<uint, Vec<Vec<u8>>> = HashMap::new();
            for key in keys.into_iter() {
                let svr_idx = self.find_server_index_by_key(key.as_slice());

                match svrkey.entry(svr_idx) {
                    Occupied(entry) => {
                        entry.into_mut().push(key);
                    },
                    Vacant(entry) => {
                        entry.set(vec![key]);
                    }
                }
            }
            svrkey
        };

        let mut chans = Vec::new();
        for (svr_idx, v) in sk.into_iter() {
            let (tx2, rx2) = channel();
            let svr = self.servers[svr_idx].clone();
            spawn(proc() {
                let r = svr.lock().proto.delete_multi(v);
                tx2.send(r);
                drop(tx2);
            });
            chans.push(rx2);
        }

        let mut result = Ok(());
        for chan in chans.into_iter() {
            result = result.or(chan.recv());
        }

        result
    }

    fn get_multi(&mut self, keys: Vec<Vec<u8>>) -> Result<TreeMap<Vec<u8>, (Vec<u8>, u32)>, Error> {
        let sk = {
            let mut svrkey: HashMap<uint, Vec<Vec<u8>>> = HashMap::new();
            for key in keys.into_iter() {
                let svr_idx = self.find_server_index_by_key(key.as_slice());

                match svrkey.entry(svr_idx) {
                    Occupied(entry) => {
                        entry.into_mut().push(key);
                    },
                    Vacant(entry) => {
                        entry.set(vec![key]);
                    }
                }
            }
            svrkey
        };

        let mut chans = Vec::new();
        for (svr_idx, v) in sk.into_iter() {
            let (tx2, rx2) = channel();
            let svr = self.servers[svr_idx].clone();
            spawn(proc() {
                let r = svr.lock().proto.get_multi(v);
                tx2.send(r);
                drop(tx2);
            });
            chans.push(rx2);
        }

        let mut chan_iter = chans.into_iter();
        let mut result = match chan_iter.next() {
            Some(chan) => chan.recv(),
            None => return Ok(TreeMap::new()),
        };
        for rx in chan_iter {
            let r = rx.recv();
            match r {
                Ok(m) => {
                    result = match result {
                        Ok(mut rm) => {
                            for (key, val) in m.into_iter() {
                                rm.insert(key, val);
                            }
                            Ok(rm)
                        },
                        Err(err) => {
                            Err(err)
                        }
                    };
                },
                Err(..) => {}
            }
        }

        result
    }
}

impl NoReplyOperation for Client {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.set_noreply(key, value, flags, expiration)
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.add_noreply(key, value, flags, expiration)
    }

    fn delete_noreply(&mut self, key: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.delete_noreply(key)
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.replace_noreply(key, value, flags, expiration)
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.increment_noreply(key, amount, initial, expiration)
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.decrement_noreply(key, amount, initial, expiration)
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.append_noreply(key, value)
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.prepend_noreply(key, value)
    }
}

impl CasOperation for Client {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.set_cas(key, value, flags, expiration, cas)
    }

    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.add_cas(key, value, flags, expiration)
    }

    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.replace_cas(key, value, flags, expiration, cas)
    }

    fn get_cas(&mut self, key: &[u8]) -> Result<(Vec<u8>, u32, u64), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.get_cas(key)
    }

    fn getk_cas(&mut self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u32, u64), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.getk_cas(key)
    }

    fn increment_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> Result<(u64, u64), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.increment_cas(key, amount, initial, expiration, cas)
    }

    fn decrement_cas(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32, cas: u64)
            -> Result<(u64, u64), Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.decrement_cas(key, amount, initial, expiration, cas)
    }

    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.append_cas(key, value, cas)
    }

    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.prepend_cas(key, value, cas)
    }

    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> Result<u64, Error> {
        let server = self.find_server_by_key(key);
        server.lock().proto.touch_cas(key, expiration, cas)
    }
}
