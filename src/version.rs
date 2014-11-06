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

use std::fmt::{Show, Formatter, mod};
use std::from_str::FromStr;

/// Memcached version
///
/// Version(major, minor, patch)
pub struct Version(uint, uint, uint);

impl Version {
    pub fn new(major: uint, minor: uint, patch: uint) -> Version {
        Version(major, minor, patch)
    }
}

impl Show for Version {
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
)

impl FromStr for Version {
    fn from_str(s: &str) -> Option<Version> {
        let sp: Vec<&str> = s.split('.').collect();
        Some(Version::new(try_option!(from_str::<uint>(sp[0])),
                          try_option!(from_str::<uint>(sp[1])),
                          try_option!(from_str::<uint>(sp[2]))))
    }
}
