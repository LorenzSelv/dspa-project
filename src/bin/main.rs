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

extern crate dspa;
use dspa::event;
use dspa::event::Event;

use dspa::kafka;

use dspa::operators::active_posts::dump_stats;
use dspa::operators::active_posts::ActivePosts;
use dspa::operators::friend_recommendations::dump_recommendations;
use dspa::operators::friend_recommendations::FriendRecommendations;
use dspa::operators::post_trees::PostTrees;

const ACTIVE_WINDOW_SECONDS: u64 = 12 * 3600;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index() as i32;
        let total_workers = worker.peers() as i32;

        // needed for the inspect
        let widx1 = worker.index();
        let widx2 = worker.index();

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

            let (stat_updates, rec_updates) = single.concat(&broad).post_trees(index as usize);

            stat_updates.active_posts(ACTIVE_WINDOW_SECONDS, index as usize).inspect(
                move |stats| {
                    println!(
                        "{} {}",
                        format!("[W{}]", widx1).bold().red(),
                        "stat inspect".bold().red()
                    );
                    dump_stats(stats, 4);
                },
            );

            rec_updates.friend_recommendations(38_u64).inspect(move |recommendations| {
                println!(
                    "{} {}",
                    format!("[W{}]", widx2).bold().blue(),
                    "rec inspect".bold().blue()
                );
                dump_recommendations(recommendations);
            });
        });
    })
    .expect("Timely computation failed somehow");
}
