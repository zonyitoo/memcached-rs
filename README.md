# memcached-rs

[![Build Status](https://travis-ci.org/zonyitoo/memcached-rs.svg)](https://travis-ci.org/zonyitoo/memcached-rs)

Memcached library in Rust

## Usage

```rust
extern crate memcached;

use std::collections::TreeMap;

use memcached::client::Client;
use memcached::proto::{Operation, MultiOperation, NoReplyOperation, CasOperation, Binary};

fn main() {
    let mut client = Client::connect([("127.0.0.1:11211", 1)], Binary);

    client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
    let (value, flags) = client.get(b"Foo").unwrap();
    assert_eq!(value.as_slice(), b"Bar");
    assert_eq!(flags, 0xdeadbeef);

    let mut data = TreeMap::new();
    data.insert(b"key1".to_vec(), (b"val1".to_vec(), 0xdeadbeef, 10));
    data.insert(b"key2".to_vec(), (b"val2".to_vec(), 0xcafebabe, 20));
    client.set_multi(data).unwrap();

    client.set_noreply(b"key:dontreply", b"1", 0x00000001, 20).unwrap();

    let (_, cas_val) = client.increment_cas(b"key:numerical", 10, 1, 20, 0).unwrap();
    client.increment_cas(b"key:numerical", 1, 1, 20, cas_val).unwrap();
}
```

Run `cargo doc --open` for more details.

## License

[The MIT License (MIT)](http://opensource.org/licenses/MIT)

Copyright (c) 2014 Y. T. CHUNG

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
