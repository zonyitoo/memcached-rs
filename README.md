# memcached-rs

[![Build Status](https://travis-ci.org/zonyitoo/memcached-rs.svg)](https://travis-ci.org/zonyitoo/memcached-rs)

Memcached library in Rust

## Usage

```rust
extern crate memcached;

use std::collections::TreeMap;

use memcached::Client;
use memcached::proto::{Operation, MultiOperation, NoReplyOperation, CasOperation, ProtoType};

fn main() {
    let servers = [
        ("tcp://127.0.0.1:11211", 1),
    ];
    let mut client = Client::connect(&servers, ProtoType::Binary, None).unwrap();

    client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
    let (value, flags) = client.get(b"Foo").unwrap();
    assert_eq!(value.as_slice(), b"Bar");
    assert_eq!(flags, 0xdeadbeef);

    client.set_noreply(b"key:dontreply", b"1", 0x00000001, 20).unwrap();

    let (_, cas_val) = client.increment_cas(b"key:numerical", 10, 1, 20, 0).unwrap();
    client.increment_cas(b"key:numerical", 1, 1, 20, cas_val).unwrap();
}
```

Run `cargo doc --open` for more details.

### SASL authentication

TCP connections support `PLAIN` SASL authentication:

```rust
extern crate memcached;

use memcached::proto::{Operation, ProtoType};
use memcached::{Client, Sasl};

fn main() {
    let servers = [("tcp://my-sasl-memcached-server.com:11211", 1)];
    let sasl = Sasl {username: "my-username", password: "my-password"};
    let mut client = Client::connect(&servers, ProtoType::Binary, Some(sasl)).unwrap();

    client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
    let (value, flags) = client.get(b"Foo").unwrap();
    assert_eq!(&value[..], b"Bar");
    assert_eq!(flags, 0xdeadbeef);
}
```

## TODO

* Auto-disable failed servers

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
