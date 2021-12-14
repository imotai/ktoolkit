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
use kafka::consumer::{Consumer, FetchOffset, MessageSets};
use std::io::{self, Write};
use std::time::Duration;
use std::{env, process};

mod base;

///
/// Kafka HeadConsumer which provide following features
/// 1. consume first n messages
/// 2. consume last n messages
/// 3. consume all messages
///
#[derive(Debug)]
pub struct KtoolsConsumer {
    consumer: Consumer,
    broker_config: base::MessageServerConfig,
    topic_config: base::TopicConfig,
    verbose:i32,
}

impl KtoolsConsumer {
    pub fn new(broker_config: base::MessageServerConfig,
               topic_config: base::TopicConfig,
               verbose:i32) -> Result<KtoolsConsumer, base::KToolsError> {
        let mut cb = Consumer::from_hosts(broker_config.brokers)
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(topic_config.fetch_max_bytes_read_per_partition) // control the max bytes to read
            .with_topic(topic_config.topic);
        let position = topic_config.offset_position;
        match position {
            base::ReadPosition::All | base::ReadPosition::Head => cb.with_fallback_offset(FetchOffset::Earliest),
            base::ReadPosition::Tail => cb.with_fallback_offset(FetchOffset::Latest),
        };
        let consumer = KtoolsConsumer {
            consumer: cb.create().unwrap(),
            broker_config: broker_config.clone(),
            topic_config: topic_config.clone(),
            verbose,
        };
        Ok(consumer)
    }

    pub fn consume(&mut self) {
        let position = self.topic_config.offset_position.clone();
        match position {
            base::ReadPosition::Head {limit} => {
                fetch(limit)
            }
            base::ReadPosition::Tail {limit} => {
                fetch(limit)
            }
            _ => {
                fetch(-1)
            }
        }
    }

    fn fetch(&mut self, limit: i32) {
        let mut count = 0;
        loop {
            let ms = self.consumer.poll();
            match ms {
                Ok=> {
                    let left_limit = limit - count;
                    count += self.display(ms.unwrap(), self.verbose, left_limit);
                }
                Err=> {
                    println!("fail get message with error");
                }
            }
            if count >= limit && limit >= 0 {
                return
            }
        }
    }

    fn display(&self, ms: MessageSets, verbose: i32, limit: i32) -> i32 {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        let mut buf = Vec::with_capacity(1024);
        let mut count = 0;
        for i in ms.iter() {
            for m in i.messages() {
                // ~ clear the output buffer
                unsafe { buf.set_len(0) };
                match verbose {
                    0 => {}
                    _ => {
                        let _ = write!(buf, "{}:{}@{}:", ms.topic(), ms.partition(), m.offset);
                    }
                }
                buf.extend_from_slice(m.value);
                buf.push(b'\n');
                // ~ write to output channel
                stdout.write_all(&buf);
                count += 1;
                if count >= limit && limit >= 0{
                    return count;
                }
            }
        }
        count
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

    let mut number: i32 = -1;
    if matches.opt_present("n") {
        number = matches.opt_str("n").unwrap().parse().unwrap();
    }
    let broker_config = base::MessageServerConfig {
        brokers,
        server_type: base::MessageQueueServerType::Kafka,
    };
    let topic_config = base::TopicConfig {
        topic:matches.free[1].clone(),
        offset_position: base::ReadPosition::Head {
            limit:number,
        },
        format: base::MessageFormat::JSON,
        fetch_max_bytes_read_per_partition: 1000_1000,
    };
    let mut consumer = KtoolsConsumer::new(broker_config, topic_config, 10);
    match consumer {
        Ok(mut c) => {
            c.consume()
        }
        _ => {
            println!("some errors")
        }
    }
}
