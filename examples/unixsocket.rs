extern crate memcached;
#[macro_use]
extern crate log;
extern crate env_logger;

use memcached::proto::{CasOperation, NoReplyOperation, Operation, ProtoType};
use memcached::Client;

fn main() {
    env_logger::init();

    let servers = [("unix:///tmp/memcached.sock", 10)];
    info!("Using servers: {:?} with Binary protocol", servers);
    let mut client = Client::connect(&servers, ProtoType::Binary).unwrap();

    client.set(b"Foo", b"Bar", 0xdeadbeef, 2).unwrap();
    let (value, flags) = client.get(b"Foo").unwrap();
    assert_eq!(&value[..], b"Bar");
    assert_eq!(flags, 0xdeadbeef);

    client.set_noreply(b"key:dontreply", b"1", 0x00000001, 20).unwrap();

    let (_, cas_val) = client.increment_cas(b"key:numerical", 10, 1, 20, 0).unwrap();
    client.increment_cas(b"key:numerical", 1, 1, 20, cas_val).unwrap();
}
