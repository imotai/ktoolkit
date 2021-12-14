//
//
// base.rs
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
use std::io::{self, Write};
use getopts::Options;
use kafka::consumer::{Consumer, FetchOffset, MessageSets};

/// The type of message queue server eg kafka, pulsar, activemq, rabbitmq
#[derive(Clone)]
pub enum MessageQueueServerType {
    Kafka,
    Pulsar,
}

/// Define the topic position to read
#[derive(Clone)]
pub enum ReadPosition {
    /// read the head
    Head { limit: i32 },
    /// read the tail
    Tail { limit: i32 },
    /// read all messages
    All,
}

/// the format of message
#[derive(Clone)]
pub enum MessageFormat {
    JSON,
    Text,
}

/// config for message server
#[derive(Clone)]
pub struct MessageServerConfig {
    pub brokers: Vec<String>,
    pub server_type: MessageQueueServerType,
}

/// the config of message queue
#[derive(Clone)]
pub struct TopicConfig {
    pub topic: String,
    pub offset_position: ReadPosition,
    pub format: MessageFormat,
    pub fetch_max_bytes_read_per_partition: i32,
}

pub struct KToolsError {
    pub err: String,
}

/// print usage information for ktail
pub fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} brokers topic [options]", program);
    print!("{}", opts.usage(&brief));
}

///
/// Kafka HeadConsumer which provide following features
/// 1. consume first n messages
/// 2. consume last n messages
/// 3. consume all messages
///
pub struct KtoolsConsumer {
    consumer: Consumer,
    broker_config: MessageServerConfig,
    topic_config: TopicConfig,
    verbose: i32,
}

impl KtoolsConsumer {
    pub fn new(
        broker_config: MessageServerConfig,
        topic_config: TopicConfig,
        verbose: i32,
    ) -> Result<KtoolsConsumer, KToolsError> {
        let mut cb = Consumer::from_hosts(broker_config.brokers.clone())
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(topic_config.fetch_max_bytes_read_per_partition) // control the max bytes to read
            .with_topic(topic_config.topic.clone());

        match topic_config.offset_position {
            ReadPosition::All { .. } | ReadPosition::Head { .. } => {
                cb = cb.with_fallback_offset(FetchOffset::Earliest);
            }
            ReadPosition::Tail { .. } => {
                cb = cb.with_fallback_offset(FetchOffset::Latest);
            }
        };

        Ok(KtoolsConsumer {
            consumer: cb.create().unwrap(),
            broker_config: broker_config,
            topic_config: topic_config,
            verbose,
        })
    }

    pub fn consume(&mut self) {
        let position = self.topic_config.offset_position.clone();
        match position {
            ReadPosition::Head { limit } => self.fetch(limit),
            ReadPosition::Tail { limit } => self.fetch(limit),
            _ => self.fetch(-1),
        }
    }

    fn fetch(&mut self, limit: i32) {
        let mut count = 0;
        loop {
            let ms = self.consumer.poll();
            match ms {
                Ok(m) => {
                    let left_limit = limit - count;
                    count += self.display(m, self.verbose, left_limit);
                }
                _ => {
                    println!("fail get message with error");
                }
            }
            if count >= limit && limit >= 0 {
                return;
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
                        let _ = write!(buf, "{}:{}@{}:", i.topic(), i.partition(), m.offset);
                    }
                }
                buf.extend_from_slice(m.value);
                buf.push(b'\n');
                // ~ write to output channel
                let _ = stdout.write_all(&buf);
                count += 1;
                if count >= limit && limit >= 0 {
                    return count;
                }
            }
        }
        count
    }
}
