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

#![feature(phase)]

extern crate memcached;
#[phase(plugin, link)]
extern crate log;
extern crate time;

use std::rand::random;

use memcached::client::Client;
use memcached::proto::{Binary, Operation};
use memcached::proto::MemCachedError;
use memcached::proto::{KeyNotFound};

const SERVERS: &'static [(&'static str, uint)] = [
    ("127.0.0.1:11211", 1),
    // ("127.0.0.1:11212", 1),
    // ("127.0.0.1:11213", 1),
];

const TESTS: &'static [(uint, uint, f64, uint)] = [
    (1, 10240, 0.2, 64),
    (1, 10240, 0.2, 512),
    (1, 10240, 0.2, 1024),
    (1, 10240, 0.2, 4096),
    (1, 10240, 0.2, 16384),

    (10, 8192, 0.2, 64),
    (10, 8192, 0.2, 512),
    (10, 8192, 0.2, 1024),
    (10, 8192, 0.2, 4096),
    (10, 8192, 0.2, 16384),

    (50, 4096, 0.2, 64),
    (50, 4096, 0.2, 512),
    (50, 4096, 0.2, 1024),
    (50, 4096, 0.2, 4096),
    (50, 4096, 0.2, 16384),

    (100, 2048, 0.2, 64),
    (100, 2048, 0.2, 512),
    (100, 2048, 0.2, 1024),
    (100, 2048, 0.2, 4096),
    (100, 2048, 0.2, 16384),

    (300, 1024, 0.2, 64),
    (300, 1024, 0.2, 512),
    (300, 1024, 0.2, 1024),
    (300, 1024, 0.2, 4096),
    (300, 1024, 0.2, 16384),
];

fn main() {
    println!("Concurrent,Repeat,WriteRate,ValueLength,Hit,Miss,Error,Time,TPS");
    for &(concurrent, repeat, write_rate, value_size) in TESTS.iter() {
        let mut rxs = Vec::new();

        let begin_time = time::now().to_timespec();
        for offset in range(0, concurrent) {
            let (tx, rx) = channel();
            spawn(proc() {
                let mut client = Client::connect(SERVERS, Binary).unwrap_or_else(|err| {
                    panic!("{} failed to connect server {}", offset, err);
                });

                let (mut hit, mut miss, mut error) = (0u, 0u, 0u);
                for key in range(offset, offset + (write_rate * repeat as f64) as uint) {
                    client.set(key.to_string().as_slice().as_bytes(), generate_data(value_size).as_slice(),
                               0xdeadbeef, 0).unwrap_or_else(|err| {
                                    panic!("Failed to set: {}", err);
                               })
                }
                for _ in range(0, ((1f64 - write_rate) / write_rate) as uint) {
                    for key in range(offset, offset + (write_rate * repeat as f64) as uint) {
                        match client.get(key.to_string().as_slice().as_bytes()) {
                            Err(e) => {
                                match e.kind {
                                    MemCachedError(err) => {
                                        if err == KeyNotFound {
                                            miss += 1;
                                        } else {
                                            error += 1;
                                        }
                                    },
                                    _ => {
                                        panic!("Failed to get: {}", e);
                                    }
                                }
                            },
                            Ok(..) => {
                                hit += 1;
                            }
                        }
                    }
                }

                tx.send((hit, miss, error));
                // drop(tx);
                drop(client);
            });
            rxs.push(rx);
        }

        let (mut tot_hit, mut tot_miss, mut tot_error) = (0u, 0u, 0u);
        for rx in rxs.into_iter() {
            let (h, m, e) = rx.recv();
            tot_hit += h;
            tot_miss += m;
            tot_error += e;
        }
        let end_time = time::now().to_timespec();

        let duration = end_time - begin_time;
        let tps = (((concurrent * repeat) * 1000) as f64) / (duration.num_milliseconds() as f64);
        println!("{},{},{},{},{},{},{},{},{}",
                 concurrent, repeat, write_rate, value_size,
                 tot_hit, tot_miss, tot_error, duration.num_milliseconds(), tps);
    }
}

fn generate_data(len: uint) -> Vec<u8> {
    Vec::from_fn(len, |_| random())
}
