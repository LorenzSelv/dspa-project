//use timely::dataflow::{Scope, Stream};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};
//use timely::communication::allocator::Generic;

pub fn string_stream<'a> (
    scope: &&mut timely::dataflow::scopes::Child<'a, timely::worker::Worker<timely::communication::allocator::Generic>, u64>,
    topic: &'static str 
) -> timely::dataflow::Stream<timely::dataflow::scopes::Child<'a, timely::worker::Worker<timely::communication::allocator::Generic>, u64>, std::string::String>
{
    let brokers = "localhost:9092";

    // Create Kafka consumer configuration.
    // Feel free to change parameters here.
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("produce.offset.report", "true")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "example")
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
            output
                .session(capability)
                .give(text.to_string());
            // We need some rule to advance timestamps ...
            let time = *capability.time();
            capability.downgrade(&(time + 1));
            // Indicate that we are not yet done.
            false
        }
        else { true } // if false keeps on waiting
    })
}
