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

use proto::{Proto, mod};

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

pub struct Client {
    servers: Vec<Server>,
}

impl Client {
    pub fn connect(svrs: &[(&str, Port, proto::ProtoType)]) -> Client {
        let mut servers = Vec::new();
        for &(addr, port, p) in svrs.iter() {
            servers.push(Server::connect(addr, port, p).unwrap_or_else(|err| {
                panic!("Cannot connect server {}:{}: {}", addr, port, err);
            }));
        }
        Client {
            servers: servers,
        }
    }
}
