/* TODO
 * - review all kafka options we pass to the BaseConsumer config
 * - handle the case when number of workers and number of partitions is not the same
 * - more TODOs...
 */
#[macro_use]
extern crate lazy_static;


extern crate config;
extern crate rdkafka;
extern crate serde;
extern crate serde_derive;

use std::collections::HashMap;

use colored::*;

extern crate timely;
use timely::dataflow::operators::{Branch, Broadcast, Concat, Exchange, Inspect, Map};
use timely::dataflow::{Scope, Stream};

extern crate dspa;
use dspa::event;
use dspa::event::Event;

use dspa::kafka;

use dspa::operators::active_posts::ActivePosts;
use dspa::operators::active_posts::{dump_stats, Stats};
use dspa::operators::friend_recommendations::dump_recommendations;
use dspa::operators::friend_recommendations::FriendRecommendations;
use dspa::operators::friend_recommendations::Score;
use dspa::operators::post_trees::PostTrees;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("Settings")).unwrap();
        s
    };
    static ref RECOMMENDATION_CLIENTS: String =
        SETTINGS.get::<String>("RECOMMENDATION_CLIENTS").unwrap();
}

fn inspect_stats(widx: usize, stats: &HashMap<u64, Stats>) {
    println!("{} {}", format!("[W{}]", widx).bold().red(), "stats inspect".bold().red());
    dump_stats(stats, 4);
}

fn inspect_rec(widx: usize, rec: &HashMap<u64, Vec<Score>>) {
    println!("{} {}", format!("[W{}]", widx).bold().blue(), "rec inspect".bold().blue());
    for (pid, rec_single) in rec.iter() {
        dump_recommendations(*pid, rec_single);
    }
}

fn get_event_stream<G>(scope: &mut G, widx: usize, num_workers: usize) -> Stream<G, Event>
where
    G: Scope<Timestamp = u64>,
{
    // read stream from kafka and deserialize string records into events
    let events = kafka::consumer::string_stream(scope, "events", widx, num_workers)
        .map(|record: String| event::deserialize(record));

    // reply to comments should be broadcasted to all workers
    let (single, broad) = events.branch(|_, event| match event {
        Event::Comment(c) => c.reply_to_comment_id != None,
        _ => false,
    });

    let single = single.exchange(|event| event.target_id());
    let broad = broad.broadcast();

    single.concat(&broad)
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let widx = worker.index();
        let num_workers = worker.peers();

        let recommendation_pids: Vec<u64> = RECOMMENDATION_CLIENTS
            .split(',')
            .map(|s| s.trim())
            .map(|s| s.parse().unwrap())
            .collect();

        assert!(recommendation_pids.len() == 10);

        let num_pids:  usize = 10 / num_workers;
        let start_pid: usize = widx * num_pids;
        let end_pid:   usize =  { if widx == (num_workers - 1) { 10 } else { start_pid + num_pids }};

        worker.dataflow::<u64, _, _>(move |scope| {
            let event_stream = get_event_stream(scope, widx, num_workers);

            let (stat_updates, rec_updates) = event_stream.post_trees(widx);

            let widx1 = widx.clone();
            stat_updates
                .active_posts(widx)
                .inspect(move |stats| inspect_stats(widx1, stats));

            let widx2 = widx.clone();
            // TODO pass a list of people instead
            rec_updates
                .friend_recommendations(&recommendation_pids[start_pid..end_pid].to_vec())
                .inspect(move |rec| inspect_rec(widx2, rec));
        });
    })
    .expect("Timely computation failed somehow");
}
