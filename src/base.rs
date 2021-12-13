//
//
// config.rs
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

use kafka::consumer::{FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;

/// the type of message queue , eg kafka, pulsar
pub enum MessageQueueType {
    Kafka,
    Pulsar,
}

/// the format of message
pub enum MessageFormat {
    JSON,
    Text,
}

/// the config of message queue
pub struct Config {
    pub message_queue_type: MessageQueueType,
    pub brokers: Vec<String>,
    pub group: String,
    pub topic: String,
    pub no_commit: bool,
    pub fallback_offset: FetchOffset,
    pub format: MessageFormat,
}

