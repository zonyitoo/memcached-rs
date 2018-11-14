// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

#![crate_type = "lib"]
#![crate_name = "memcached"]
#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

extern crate bufstream;
extern crate byteorder;
extern crate conhash;
#[macro_use]
extern crate log;
extern crate rand;
extern crate semver;
#[cfg(unix)]
extern crate unix_socket;

pub use client::{Client, Sasl};

pub mod client;
pub mod proto;
