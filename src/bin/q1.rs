/* TODO
 * - review all kafka options we pass to the BaseConsumer config
 * - handle the case when number of workers and number of partitions is not the same
 * - more TODOs...
 */

extern crate config;
extern crate lazy_static;
extern crate rdkafka;
extern crate serde;
extern crate serde_derive;

use colored::*;

extern crate timely;
use timely::dataflow::operators::{Branch, Broadcast, Concat, Exchange, Inspect, Map};

use std::collections::HashMap;

extern crate dspa;
use dspa::event;
use dspa::event::Event;

use dspa::kafka;

use dspa::operators::active_posts::ActivePosts;
use dspa::operators::active_posts::{dump_stats, Stats};
use dspa::operators::post_trees::PostTrees;

const ACTIVE_WINDOW_SECONDS: u64 = 12 * 3600;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index() as i32;
        let total_workers = worker.peers() as i32;

        worker.dataflow::<u64, _, _>(|scope| {
            let events_stream =
                kafka::consumer::string_stream(scope, "events", index, total_workers)
                    .map(|record: String| event::deserialize(record));

            // reply to comments should be broadcasted
            let (single, broad) = events_stream.branch(|_, event| match event {
                Event::Comment(c) => c.reply_to_comment_id != None,
                _ => false,
            });

            let single = single.exchange(|event| event.target_post_id());
            let broad = broad.broadcast();

            let stat_updates = single.concat(&broad).post_trees(index as usize);

            stat_updates.active_posts(ACTIVE_WINDOW_SECONDS, index as usize).inspect(
                |stats: &HashMap<u64, Stats>| {
                    println!("{}", "inspect".bold().red());
                    dump_stats(stats, 4);
                },
            );
        });
    })
    .expect("Timely computation failed somehow");
}
