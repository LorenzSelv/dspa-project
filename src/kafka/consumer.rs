use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};

use timely::worker::Worker;
use timely::dataflow::Stream;
use timely::dataflow::scopes::Child;
use timely::communication::allocator::Generic;

extern crate chrono;
use chrono::{Utc,TimeZone};

const MAX_DELAY_SECONDS: u64 = 0; // TODO this should be a command line arg

pub fn string_stream<'a> (
    scope: &mut timely::dataflow::scopes::Child<'a, Worker<Generic>, u64>,
    topic: &'static str
) -> Stream<Child<'a, Worker<Generic>, u64>, String>
{
    let brokers = "localhost:9092";

    // Create Kafka consumer configuration.
    // Feel free to change parameters here.
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("produce.offset.report", "true")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "example") // TODO we wanna change this
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", &brokers);

    // Create a Kafka consumer.
    let consumer : BaseConsumer<EmptyConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
    consumer.subscribe(&[&topic.to_string()]).expect("Failed to subscribe to topic");

    println!("[kafka-consumer] subscribed to topic \"{}\"", topic);

    kafkaesque::source(scope, "KafkaStringSourceStream", consumer, |bytes, capability, output| {

        // If the bytes are utf8, convert to string and send.
        if let Ok(text) = std::str::from_utf8(bytes) {

            let timestamp =
                if text.starts_with("WATERMARK") { // format is WATERMARK|<timestamp>
                    let t = text.split("|").collect::<Vec<_>>()[1].trim();
                    t.parse::<u64>().unwrap()
                } else {
                    output.session(capability).give(text.to_string());
                    let date_str = text.split("|").collect::<Vec<_>>()[2].trim();
                    let date = Utc.datetime_from_str(date_str, "%FT%TZ")
                           .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ"))
                           .expect("failed to parse");

                    date.timestamp() as u64
                };

            let time = *capability.time();
            if timestamp - MAX_DELAY_SECONDS > time {
                capability.downgrade(&(timestamp - MAX_DELAY_SECONDS));
                println!("downgraded to {}", timestamp - MAX_DELAY_SECONDS);
            }

            // Indicate that we are not yet done.
            false
        }
        else { true } // if false keeps on waiting
    })
}
