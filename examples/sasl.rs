extern crate memcached;

use memcached::proto::{Operation, ProtoType};
use memcached::Client;

fn main() {
    let servers = [("tcp://my-sasl-memcached-server.com:11211", 1)];
    let mut client = Client::connect_sasl(&servers, ProtoType::Binary, "my-username", "my-password").unwrap();

    client.set(b"Foo", b"Bar", 0xdead_beef, 2).unwrap();
    let (value, flags) = client.get(b"Foo").unwrap();
    assert_eq!(&value[..], b"Bar");
    assert_eq!(flags, 0xdead_beef);
}
