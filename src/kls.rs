// kls.rs
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
//
//

extern crate getopts;
#[macro_use]
extern crate prettytable;
use std::env;
use getopts::Options;
use kafka::client::KafkaClient;
use prettytable::format;
use prettytable::Table;

mod base;

fn display_topics(broker_config: base::MessageServerConfig) {
    let mut client = KafkaClient::new(broker_config.brokers);
    client.load_metadata_all().unwrap();
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(row!["#", "topics", "partitions"]);
    let mut index: i32 = 1;
    for t in client.topics() {
        table.add_row(row![index, t.name(), t.partitions().len()]);
        index += 1;
    }
    table.printstd();
}

pub fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} brokers [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
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
    let broker_config = base::MessageServerConfig {
        brokers,
        server_type: base::MessageQueueServerType::Kafka,
    };
    display_topics(broker_config);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
