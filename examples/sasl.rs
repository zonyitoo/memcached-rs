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
