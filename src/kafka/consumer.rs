use timely::dataflow::{Scope, Stream};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::TopicPartitionList;

use chrono::{TimeZone, Utc};

use super::source::kafka_source;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("Settings")).unwrap();
        s
    };
    static ref MAX_DELAY_SEC: u64 = SETTINGS.get::<u64>("MAX_DELAY_SEC").unwrap();
    static ref NUM_PARTITIONS: i32 = SETTINGS.get::<i32>("NUM_PARTITIONS").unwrap();
}

/// subscribe to the requested topic and return a stream
/// of Kafka records (strings).
/// Kafka partition are assigned to workers in a round-robin fashion.
pub fn string_stream<'a, G>(
    scope: &mut G,
    topic: &'static str,
    index: usize,
    peers: usize,
) -> Stream<G, String>
where
    G: Scope<Timestamp = u64>,
{
    let brokers = "localhost:9092";

    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("produce.offset.report", "true")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "dspa")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", &brokers);

    // Create a Kafka consumer.
    let consumer: BaseConsumer<EmptyConsumerContext> =
        consumer_config.create().expect("Couldn't create consumer");
    consumer.subscribe(&[&topic.to_string()]).expect("Failed to subscribe to topic");

    // assign kafka partition to workers in a round-robin fashion
    let mut partition_list = TopicPartitionList::new();
    let mut partition = index as i32;
    while partition < *NUM_PARTITIONS {
        partition_list.add_partition(topic, partition);
        partition += peers as i32;
    }
    consumer.assign(&partition_list).expect("error in assigning partition list");

    println!(
        "[kafka-consumer] subscribed to {} partitions of topic \"{}\"",
        partition_list.count(),
        topic
    );

    kafka_source(scope, "KafkaStringSourceStream", consumer, |bytes, capability, output| {
        // If the bytes are utf8, convert to string and send.
        if let Ok(text) = std::str::from_utf8(bytes) {
            let timestamp = if text.starts_with("WATERMARK") {
                // format is WATERMARK|<timestamp>
                // use watermark to downgrade capabilities
                let t = text.split("|").collect::<Vec<_>>()[1].trim();
                t.parse::<u64>().unwrap()
            } else {
                // forward only real events
                output.session(capability).give(text.to_string());
                let date_str = text.split("|").collect::<Vec<_>>()[2].trim();
                let date = Utc
                    .datetime_from_str(date_str, "%FT%TZ")
                    .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ"))
                    .expect("failed to parse");

                date.timestamp() as u64
            };

            let time = *capability.time();
            if timestamp - *MAX_DELAY_SEC > time {
                // downgrade the capability by considering the event timestamp
                // and the maximum bounded delay
                capability.downgrade(&(timestamp - *MAX_DELAY_SEC));
            }

            false
        } else {
            true
        }
    })
}
