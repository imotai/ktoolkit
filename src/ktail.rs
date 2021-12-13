//
//
// ktail.rs
// Copyright (C) 2021 peasdb.ai Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

extern crate getopts;
use getopts::Options;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::io::{self, Write};
use std::time::Duration;
use std::{env, process};

mod base;

fn tail(cfg: base::Config, number: i32) {
    let mut c = {
        let mut cb = Consumer::from_hosts(cfg.brokers)
            .with_group(cfg.group)
            .with_fallback_offset(cfg.fallback_offset)
            .with_fetch_max_wait_time(Duration::from_secs(1))
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .with_topic(cfg.topic)
            .with_client_id("ktail".into());
        cb.create().unwrap()
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut buf = Vec::with_capacity(1024);
    let do_commit = !cfg.no_commit;
    let mut count: i32 = 0;
    loop {
        for ms in c.poll().unwrap().iter() {
            for m in ms.messages() {
                // ~ clear the output buffer
                unsafe { buf.set_len(0) };
                // ~ format the message for output
                let _ = write!(buf, "{}:{}@{}:", ms.topic(), ms.partition(), m.offset);
                buf.extend_from_slice(m.value);
                buf.push(b'\n');
                // ~ write to output channel
                stdout.write_all(&buf);
                count += 1;
                if number > 0 && count >= number {
                    return;
                }
            }
            let _ = c.consume_messageset(ms);
        }
        if do_commit {
            c.commit_consumed();
        }
    }
}

/// print usage information for ktail
fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} brokers topic [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "", "The location is number lines", "NUMBER");
    opts.optflag("f", "", "The -f option causes tail to not stop when end of file is reached, but rather to wait for additional data to be appended to the input.  The -f option is ignored if the
                     standard input is a pipe, but not if it is a FIFO.");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            panic!("Error {}", f.to_string())
        }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let brokers: Vec<&str> = matches.free[0].split(",").collect();
    let brokers: Vec<String> = brokers.iter().map(|&x| String::from(x)).collect();
    let config = base::Config {
        brokers: brokers,
        group: String::from("group"),
        topic: matches.free[1].clone(),
        format: base::MessageFormat::JSON,
        fallback_offset: FetchOffset::Latest,
        message_queue_type: base::MessageQueueType::Kafka,
        no_commit: true,
    };
    let mut number: i32 = 0;
    if matches.opt_present("n") {
        number = matches.opt_str("n").unwrap().parse().unwrap();
    }
    tail(config, number);
}
