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
        let sp: Vec<&str> = s.split('.').collect();
        Some(Version::new(try_option!(sp[0].parse::<u32>()),
                          try_option!(sp[1].parse::<u32>()),
                          try_option!(sp[2].parse::<u32>())))
    }
}
