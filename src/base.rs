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

/// The type of message queue server eg kafka, pulsar, activemq, rabbitmq
pub enum MessageQueueServerType {
    Kafka,
    Pulsar,
}

/// Define the topic position to read
pub enum ReadPosition {
    /// read the head
    Head {
        limit:i32
    },
    /// read the tail
    Tail {
        limit:i32
    },
    /// read all messages
    All,
}

/// the format of message
pub enum MessageFormat {
    JSON,
    Text,
}

/// config for message server
#[derive(Copy, Clone)]
pub struct MessageServerConfig {
    pub brokers: Vec<String>,
    pub server_type: MessageQueueServerType,
}

/// the config of message queue
#[derive(Copy, Clone)]
pub struct TopicConfig {
    pub topic: String,
    pub offset_position: ReadPosition,
    pub format: MessageFormat,
    pub fetch_max_bytes_read_per_partition: i32,
}

impl Clone for MessageServerConfig {
    fn clone(&self) -> MessageServerConfig {
        *self
    }
}

impl Clone for TopicConfig {
    fn clone(&self) -> TopicConfig {
        *self
    }
}

pub struct KToolsError {
    pub err : String,
}