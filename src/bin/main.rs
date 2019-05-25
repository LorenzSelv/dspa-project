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

extern crate clap;

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

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
use dspa::operators::post_freq::PostFrequency;
use dspa::operators::post_trees::PostTrees;
use dspa::operators::unique_words::UniqueWords;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("Settings")).unwrap();
        s
    };
    static ref RECOMMENDATION_PIDS: Vec<u64> = SETTINGS
        .get::<String>("RECOMMENDATION_CLIENTS")
        .unwrap()
        .split(',')
        .map(|s| s.trim())
        .map(|s| s.parse().unwrap())
        .collect();
}

/// round-robin assign the person ids to workers
fn get_my_rec_pids(widx: usize, num_workers: usize) -> Vec<u64> {
    RECOMMENDATION_PIDS
        .iter()
        .enumerate()
        .filter_map(|(i, &pid)| if i % num_workers == widx { Some(pid) } else { None })
        .collect::<Vec<u64>>()
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

fn inspect_spam(widx: usize, spam_pid: &u64) {
    println!(
        "{} {} {}",
        format!("[W{}]", widx).bold().green(),
        "spam inspect".bold().green(),
        spam_pid
    );
}

/// read event stream from kafka and deserialize string records into events
fn get_event_stream<G>(scope: &mut G, widx: usize, num_workers: usize) -> Stream<G, Event>
where
    G: Scope<Timestamp = u64>,
{
    kafka::consumer::string_stream(scope, "events", widx, num_workers)
        .map(|record: String| event::deserialize(record))
}

/// partition events by the post_id they refer to
/// in case of replies, we don't know the root post_id at this stage
/// => broadcast them
fn broadcast_replies<G>(events: &Stream<G, Event>) -> Stream<G, Event>
where
    G: Scope<Timestamp = u64>,
{
    let (single, broad) = events.branch(|_, event| match event {
        Event::Comment(c) => c.reply_to_comment_id != None,
        _ => false,
    });

    let single = single.exchange(|event| event.target_id());
    let broad = broad.broadcast();

    single.concat(&broad)
}

fn main() {
    let matches = clap::App::new("dspa-project")
                        .arg_from_usage("-q --queries=<QUERY-ID>... 'Comma separated list of queries to run (e.g. -q 1,2), default is all queries'")
                        .arg_from_usage("-w --workers=<NUM-WORKERS> 'Comma separated list of queries to run (e.g. -w 2), default is 1'")
                        .get_matches();

    use clap::{value_t, values_t};
    let queries: HashSet<usize> =
        HashSet::from_iter(values_t!(matches, "queries", usize).unwrap_or_else(|e| e.exit()));
    let workers = value_t!(matches, "workers", usize).unwrap_or_else(|e| e.exit());

    let (builder, other) = timely::Configuration::Process(workers).try_build().unwrap();
    timely::execute::execute_from(builder, other, move |worker| {
        let widx = worker.index();
        let num_workers = worker.peers();

        worker.dataflow::<u64, _, _>(|scope| {
            // ===========================================
            // read kakfa stream
            let event_stream = get_event_stream(scope, widx, num_workers);

            event_stream.inspect(move |event: &Event| {
                println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow())
            });

            // compute and store post_trees,
            // emit stats and recommendation updates
            let (stat_updates, rec_updates) = broadcast_replies(&event_stream).post_trees(widx);

            // ===========================================
            // QUERY 1: compute active posts given the stats updates
            if queries.contains(&1) {
                let widx1 = widx.clone();
                stat_updates.active_posts(widx).inspect(move |stats| inspect_stats(widx1, stats));
            }

            // ===========================================
            // QUERY 2: compute recommendations posts given the rec updates
            if queries.contains(&2) {
                let widx2 = widx.clone();
                rec_updates
                    // Updates are currently partitioned by post id. We should either:
                    // 1) re-partition by target_person (the person the event is meaningful to),
                    //    each worker should receive only events that are meaningful for one of
                    //    the people it is responsible for
                    // 2) broadcast all events, they will be ignored by the `friend_rec` operator
                    //    if the target_person is not among the ones it is responsible for
                    // Going with (2) for now
                    .broadcast()
                    .friend_recommendations(&get_my_rec_pids(widx, num_workers))
                    .inspect(move |rec| inspect_rec(widx2, rec));
            }

            // ===========================================
            // QUERY 3: detect spam from the raw event stream (we don't need post_trees for this)
            if queries.contains(&3) {
                // partition the stream by person_id that originated the event
                let events_by_pid = event_stream.exchange(|event| event.person_id());

                // compute post_frequency to detect burst of posts
                let spam1 = events_by_pid.post_frequency(widx);

                // compute unique words metric to detect unusual behavior
                let spam2 = events_by_pid.unique_words(widx);

                // emit person ids marked as spammers
                let widx3 = widx.clone();
                spam1.concat(&spam2).inspect(move |spam| inspect_spam(widx3, spam));
            }
        });
    })
    .expect("Timely computation failed somehow");
}
