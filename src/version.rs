// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

//! Memcached version

use std::fmt::{Display, Formatter, self};
use std::str::FromStr;

/// Memcached version
///
/// Version(major, minor, patch)
#[derive(Copy, Debug)]
pub struct Version(u32, u32, u32);

impl Version {
    pub fn new(major: u32, minor: u32, patch: u32) -> Version {
        Version(major, minor, patch)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let &Version(major, minor, patch) = self;
        write!(f, "{}:{}:{}", major, minor, patch)
    }
}

macro_rules! try_option(
    ($inp:expr) => (
        match $inp {
            Some(v) => { v },
            None => { return None; },
        }
    );
);

impl FromStr for Version {
    fn from_str(s: &str) -> Option<Version> {
        let mut sp = s.split('.');
        let major = match sp.next() {
            Some(s) => try_option!(s.parse()),
            None => return None,
        };
        let minor = match sp.next() {
            Some(s) => try_option!(s.parse()),
            None => 0,
        };
        let patch = match sp.next() {
            Some(s) => try_option!(s.parse()),
            None => 0,
        };

        Some(Version::new(major, minor, patch))
    }
}
