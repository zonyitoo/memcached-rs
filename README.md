# memcached-rs

[![Build Status](https://travis-ci.org/zonyitoo/memcached-rs.svg)](https://travis-ci.org/zonyitoo/memcached-rs)
[![crates.io](https://img.shields.io/crates/v/memcached-rs.svg)](https://crates.io/crates/memcached-rs)
[![dependency status](https://deps.rs/repo/github/zonyitoo/memcached-rs/status.svg)](https://deps.rs/repo/github/zonyitoo/memcached-rs)

Memcached library in Rust

## Usage

```rust
use std::collections::TreeMap;

use memcached::Client;
use memcached::proto::{Operation, MultiOperation, NoReplyOperation, CasOperation, ProtoType};

fn main() {
    let servers = [
        ("tcp://127.0.0.1:11211", 1),
    ];
    let mut client = Client::connect(&servers, ProtoType::Binary).unwrap();

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
use memcached::proto::{Operation, ProtoType};
use memcached::Client;

fn main() {
    let servers = [
        ("tcp://my-sasl-memcached-server.com:11211", 1)
    ];
    let mut client = Client::connect_sasl(&servers, ProtoType::Binary, "my-username", "my-password").unwrap();

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
