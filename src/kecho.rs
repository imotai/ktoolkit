// kecho.rs
// Copyright (C) 2021 zombie <zombie@zombie>
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
use std::time::Duration;
use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};

mod base;
fn send_message(topic:&str, broker: Vec<String>, msg:&str)-> Result<(), KafkaError> {
	let mut producer = Producer::from_hosts(broker)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;
    producer.send(&Record::from_value(topic, msg.as_bytes()))?;
    Ok(())
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            panic!("error {}", f.to_string())
        }
    };
    if matches.opt_present("h") {
        base::print_usage(&program, opts);
        return;
    }
    let brokers: Vec<&str> = matches.free[0].split(",").collect();
    let brokers: Vec<String> = brokers.iter().map(|&x| String::from(x)).collect();
    send_message(&matches.free[1], brokers, &matches.free[2]);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn it_works() {
	}
}
