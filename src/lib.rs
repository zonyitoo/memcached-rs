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
#![allow(clippy::type_complexity)] // For `crate::proto::MemCachedResult<HashMap<Vec<u8>, (Vec<u8>, u32)>>`
#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

pub use client::Client;

pub mod client;
pub mod proto;
