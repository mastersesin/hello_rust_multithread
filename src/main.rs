// Main Entry Point :: A fuse_mt test program.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

#![deny(rust_2018_idioms)]

use std::env;
use std::ffi::{OsStr, OsString};

#[macro_use]
extern crate log;

mod libc_extras;
mod libc_wrappers;
mod passthrough;

struct ConsoleLogger;

impl log::Log for ConsoleLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        println!("{}: {}: {}", record.target(), record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: ConsoleLogger = ConsoleLogger;

fn main() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Debug);
    let filesystem = passthrough::PassthroughFS {
        target: "target".parse().unwrap(),
    };

    let fuse_args = [OsStr::new("-o"), OsStr::new("fsname=passthrufs")];

    fuse_mt::mount(fuse_mt::FuseMT::new(filesystem, 1), "mount", &fuse_args[..]).unwrap();
}
