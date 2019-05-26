use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::Message;

// mostly copy paste from timely repo
pub fn kafka_source<C, G, D, L>(
    scope: &G,
    name: &str,
    consumer: BaseConsumer<C>,
    logic: L,
) -> Stream<G, D>
where
    C: ConsumerContext + 'static,
    G: Scope,
    D: Data,
    L: Fn(
            &[u8],
            &mut Capability<G::Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> bool
        + 'static,
{
    use timely::dataflow::operators::generic::source;
    source(scope, name, move |capability, info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut cap = Some(capability);

        // define a closure to call repeatedly.
        move |output| {
            // Act only if we retain the capability to send data.
            let mut complete = false;
            if let Some(mut capability) = cap.as_mut() {
                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for [u8] messages.
                // Cease only when Kafka stops returning new data.
                // Could cease earlier, if we had a better policy.
                while let Some(result) = consumer.poll(0) {
                    // If valid data back from Kafka
                    if let Ok(message) = result {
                        // Attempt to interpret bytes as utf8  ...
                        if let Some(payload) = message.payload() {
                            complete = logic(payload, &mut capability, output) || complete;
                        }
                    } else {
                        println!("Kafka error");
                    }
                }
            }

            if complete {
                cap = None;
            }
        }
    })
}
