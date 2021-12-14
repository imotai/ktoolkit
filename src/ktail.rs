//
//
// ktail.rs
// Copyright (C) 2021 ktools Author imotai <codego.me@gmail.com>
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
use std::env;
use std::io::{self, Write};
mod base;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("f", "", "no stop");
    opts.optopt("n", "", "The location is number lines", "NUMBER");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            panic!("Error {}", f.to_string())
        }
    };
    if matches.opt_present("h") {
        base::print_usage(&program, opts);
        return;
    }
    let brokers: Vec<&str> = matches.free[0].split(",").collect();
    let brokers: Vec<String> = brokers.iter().map(|&x| String::from(x)).collect();

    let mut number: i32 = -1;
    if matches.opt_present("n") {
        number = matches.opt_str("n").unwrap().parse().unwrap();
    }
    let broker_config = base::MessageServerConfig {
        brokers,
        server_type: base::MessageQueueServerType::Kafka,
    };
    let topic_config = base::TopicConfig {
        topic: matches.free[1].clone(),
        offset_position: base::ReadPosition::Tail { limit: number },
        format: base::MessageFormat::JSON,
        fetch_max_bytes_read_per_partition: 1000_1000,
    };
    let consumer = base::KtoolsConsumer::new(broker_config, topic_config, 10);
    match consumer {
        Ok(mut c) => c.consume(),
        _ => {
            println!("some errors")
        }
    }
}
