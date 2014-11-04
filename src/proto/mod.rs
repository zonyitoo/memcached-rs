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

use std::time::duration::Duration;

pub mod binary;

pub struct Error {
    pub desc: &'static str,
    pub detail: Option<String>,
}

pub trait Proto {
    fn set(&self, key: &[u8], value: &[u8], flags: u32, expiration: Duration) -> Result<(), Error>;
    fn add(&self, key: &[u8], value: &[u8], flags: u32, expiration: Duration) -> Result<(), Error>;
    fn replace(&self, key: &[u8], value: &[u8], flags: u32, expiration: Duration) -> Result<(), Error>;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error>;
    fn getk(&self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error>;
}
