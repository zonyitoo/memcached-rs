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

#![allow(dead_code)]

use std::io::{BufRead, Read, Write, Cursor, BufReader};
use std::str;
use std::fmt;

use proto::{Operation, MemCachedResult};
use proto;

// Storage commands
const OP_SET: &'static str = "set";
const OP_ADD: &'static str = "add";
const OP_REPLACE: &'static str = "replace";
const OP_APPEND: &'static str = "append";
const OP_PREPEND: &'static str = "prepend";
const OP_CAS: &'static str = "cas";
const OP_DELETE: &'static str = "delete";
const OP_INCR: &'static str = "incr";
const OP_DECR: &'static str = "decr";
const OP_TOUCH: &'static str = "touch";
const OP_STATS: &'static str = "stats";
const OP_VERSION: &'static str = "version";
const OP_FLUSH_ALL: &'static str = "flush_all";
const OP_QUIT: &'static str = "quit";

// Retrival commands
const OP_GET: &'static str = "get";
const OP_GETS: &'static str = "gets";

const REPLY_ERROR: &'static str = "ERROR";
const REPLY_CLIENT_ERROR: &'static str = "CLIENT_ERROR";
const REPLY_SERVER_ERROR: &'static str = "SERVER_ERROR";

const REPLY_STORED: &'static str = "STORED";
const REPLY_NOT_STORED: &'static str = "NOT_STORED";
const REPLY_EXISTS: &'static str = "EXISTS";
const REPLY_NOT_FOUND: &'static str = "NOT_FOUND";
const REPLY_END: &'static str = "END";
const REPLY_VALUE: &'static str = "VALUE";
const REPLY_DELETED: &'static str = "DELETED";
const REPLY_TOUCHED: &'static str = "TOUCHED";
const REPLY_OK: &'static str = "OK";

#[derive(Debug, Clone)]
pub enum Reply {
    Error,
    ClientError(String),
    ServerError(String),

    NotStored,
    Exists,
    NotFound,
}

impl Reply {
    pub fn desc(&self) -> &'static str {
        match self {
            &Reply::Error => "error",
            &Reply::ClientError(..) => "client error",
            &Reply::ServerError(..) => "server error",

            &Reply::NotStored => "not stored",
            &Reply::Exists => "exists",
            &Reply::NotFound => "not found",
        }
    }
}

impl fmt::Display for Reply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Reply::Error => write!(f, "error"),
            &Reply::ClientError(ref e) => write!(f, "client error: {}", e),
            &Reply::ServerError(ref e) => write!(f, "server error: {}", e),

            &Reply::NotStored => write!(f, "not stored"),
            &Reply::Exists => write!(f, "exists"),
            &Reply::NotFound => write!(f, "not found"),
        }
    }
}

#[derive(Debug)]
pub enum CommandType {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas,
    Get,
    Gets,
}

pub struct TextProto<S: BufRead + Write + Send> {
    pub stream: S,
}

impl<S: BufRead + Write + Send> TextProto<S> {
    pub fn new(stream: S) -> TextProto<S> {
        TextProto {
            stream: stream,
        }
    }
}

impl<S: BufRead + Write + Send> Operation for TextProto<S> {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {} {} {}\r\n", OP_SET, strkey, flags, expiration, value.len());
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.write(value));
        try!(self.stream.write(b"\r\n"));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_STORED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {} {} {}\r\n", OP_ADD, strkey, flags, expiration, value.len());
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.write(value));
        try!(self.stream.write(b"\r\n"));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_STORED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn delete(&mut self, key: &[u8]) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };

        let cmd = format!("{} {}\r\n", OP_DELETE, strkey);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_DELETED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_FOUND), None) => {
                Err(proto::Error::TextProtoError(Reply::NotFound))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {} {} {}\r\n", OP_REPLACE, strkey, flags, expiration, value.len());
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.write(value));
        try!(self.stream.write(b"\r\n"));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_STORED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn get(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32)> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError {
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {}\r\n", OP_GET, strkey);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.flush());

        let mut resp = String::new();
        let (_key, flag, val_len) = {
            try!(self.stream.read_line(&mut resp));
            println!("RESP: {:?}", resp);

            let mut splitted = resp.trim_right().split(' ');
            match (splitted.next(), splitted.next(), splitted.next(), splitted.next()) {
                (Some(REPLY_VALUE), Some(key), Some(flag), Some(val_len)) => {
                    let flag = match flag.parse::<i32>() {
                        Ok(f) => f as u32,
                        Err(err) => return Err(proto::Error::OtherError {
                            desc: "Invalid flag",
                            detail: Some(err.to_string()),
                        }),
                    };

                    if val_len.len() == 0 {
                        return Err(proto::Error::OtherError {
                            desc: "Invalid value length",
                            detail: None,
                        });
                    }

                    let val_len = match val_len.parse::<u64>() {
                        Ok(vl) => vl,
                        Err(err) => return Err(proto::Error::OtherError {
                            desc: "Invalid value length",
                            detail: Some(err.to_string()),
                        }),
                    };

                    (key, flag, val_len)
                },
                (Some(REPLY_ERROR), _, _, _) => {
                    return Err(proto::Error::TextProtoError(Reply::Error));
                },
                (Some(REPLY_CLIENT_ERROR), Some(error), _, _) => {
                    return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
                },
                (Some(REPLY_SERVER_ERROR), Some(error), _, _) => {
                    return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
                },
                _ => {
                    return Err(proto::Error::OtherError {
                        desc: "Invalid Response",
                        detail: Some(resp.clone()),
                    });
                }
            }
        };

        let mut val = Vec::new();
        try!((&mut self.stream).take(val_len).read_to_end(&mut val));
        for _ in (&mut self.stream).take(2).bytes() {} // consumes \r\n

        let mut end = String::new();
        try!(self.stream.read_line(&mut end));
        let end = end.trim_right();

        if end.len() == 0 {
            return Err(proto::Error::OtherError {
                desc: "Invalid Response",
                detail: Some(end.to_owned()),
            });
        }

        match end {
            REPLY_END => Ok((val, flag)),
            _ => Err(proto::Error::OtherError {
                desc: "Invalid Response",
                detail: Some(end.to_owned()),
            })
        }
    }

    fn getk(&mut self, _key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32)> {
        panic!("TextProto does not support GetK command");
    }

    fn increment(&mut self, key: &[u8], amount: u64, _initial: u64, _expiration: u32) -> MemCachedResult<u64> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {}\r\n", OP_INCR, strkey, amount);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            (Some(value), None) => {
                match value.parse::<u64>() {
                    Ok(val) => Ok(val),
                    Err(err) => return Err(proto::Error::OtherError {
                        desc: "Invalid value",
                        detail: Some(err.to_string()),
                    }),
                }
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn decrement(&mut self, key: &[u8], amount: u64, _initial: u64, _expiration: u32) -> MemCachedResult<u64> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {}\r\n", OP_DECR, strkey, amount);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            (Some(value), None) => {
                match value.parse::<u64>() {
                    Ok(val) => Ok(val),
                    Err(err) => return Err(proto::Error::OtherError {
                        desc: "Invalid value",
                        detail: Some(err.to_string()),
                    }),
                }
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {}\r\n", OP_APPEND, strkey);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.write(value));
        try!(self.stream.write(b"\r\n"));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_STORED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {}\r\n", OP_PREPEND, strkey);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.write(value));
        try!(self.stream.write(b"\r\n"));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_STORED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_STORED), None) => {
                Err(proto::Error::TextProtoError(Reply::NotStored))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> MemCachedResult<()> {
        let strkey = match str::from_utf8(key) {
            Ok(s) => s,
            Err(..) => return Err(proto::Error::OtherError{
                desc: "Key has to be a valid utf-8 string",
                detail: None
            }),
        };
        let cmd = format!("{} {} {}\r\n", OP_TOUCH, strkey, expiration);
        try!(self.stream.write_all(cmd.as_bytes()));
        try!(self.stream.flush());

        let mut resp = String::new();
        try!(self.stream.read_line(&mut resp));
        let resp_str = resp.trim_right();

        let mut splitted = resp_str.split(' ');
        match (splitted.next(), splitted.next()) {
            (Some(REPLY_TOUCHED), None) => {
                Ok(())
            },
            (Some(REPLY_NOT_FOUND), None) => {
                Err(proto::Error::TextProtoError(Reply::NotFound))
            },
            (Some(REPLY_ERROR), None) => {
                Err(proto::Error::TextProtoError(Reply::Error))
            },
            (Some(REPLY_CLIENT_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ClientError(error.to_owned())));
            },
            (Some(REPLY_SERVER_ERROR), Some(error)) => {
                return Err(proto::Error::TextProtoError(Reply::ServerError(error.to_owned())));
            },
            _ => {
                Err(proto::Error::OtherError {
                    desc: "Unknown reply",
                    detail: Some(resp_str.to_string())
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::TcpStream;
    use std::io::BufStream;
    use proto::text::TextProto;
    use proto::{Operation};

    const SERVER_ADDR: &'static str = "127.0.0.1:11211";

    fn get_client() -> TextProto<BufStream<TcpStream>> {
        let stream = TcpStream::connect(SERVER_ADDR).unwrap();
        TextProto::new(BufStream::new(stream))
    }

    #[test]
    fn test_set_get_delete() {
        let key = b"test:test_text";
        let val = b"val";

        let mut client = get_client();
        let set_resp = client.set(key, val, 0xdead, 200);
        assert!(set_resp.is_ok());

        let (get_val, flag) = client.get(key).unwrap();
        assert_eq!(flag, 0xdead);
        assert_eq!(&get_val[..], val);

        client.delete(key).unwrap();
    }

    #[test]
    fn test_add() {
        let key = b"test:test_add";
        let val = b"val";

        let mut client = get_client();
        client.add(key, val, 0xdead, 20).unwrap();

        let (get_val, flag) = client.get(key).unwrap();
        assert_eq!(flag, 0xdead);
        assert_eq!(&get_val[..], val);

        client.delete(key).unwrap();
    }

    #[test]
    fn test_replace() {
        let key = b"test:test_replace";
        let val = b"val";
        let replaced = b"replaced";

        let mut client = get_client();
        client.add(key, val, 0xdead, 20).unwrap();
        client.replace(key, replaced, 0xdeadbeef, 20).unwrap();

        let (get_val, flag) = client.get(key).unwrap();
        assert_eq!(flag, 0xdeadbeef);
        assert_eq!(&get_val[..], replaced);

        client.delete(key).unwrap();
    }
}
