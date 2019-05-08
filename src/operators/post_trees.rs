use std::collections::HashMap;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use colored::*;

use crate::event::{Event, ID};

use crate::operators::active_posts::StatUpdate;
use crate::operators::active_posts::StatUpdateType;

pub trait PostTrees<G: Scope> {
    fn post_trees(&self, worker_id: usize) -> Stream<G, StatUpdate>; // TODO
}

impl<G: Scope<Timestamp = u64>> PostTrees<G> for Stream<G, Event> {
    fn post_trees(
        &self,
        worker_id: usize,
        // ) -> Stream<G, (StatUpdate, RecomUpdate)> {
    ) -> Stream<G, StatUpdate> {
        // TODO

        let mut state: PostTreesState = PostTreesState::new(worker_id);

        self.unary(Pipeline, "PostTrees", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    let mut buf = Vec::<Event>::new();
                    data.swap(&mut buf);

                    for event in buf.drain(..) {
                        // println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());

                        let (opt_target_id, opt_root_post_id) = state.update_post_tree(&event);

                        match opt_root_post_id {
                            Some(root_post_id) => {
                                if let ID::Post(pid) = root_post_id {
                                    state.append_stat_update(&event, pid);
                                } else {
                                    panic!("expect ID::Post, got ID::Comment");
                                }

                                // check whether we can pop some stuff out of the ooo map
                                if let Some(eid) = event.id() {
                                    state.process_ooo_events(eid);
                                }
                            }
                            None => {
                                state.push_ooo_event(event, opt_target_id.unwrap());
                            }
                        };

                        // state.dump();
                    }

                    let mut session = output.session(&time);
                    for stat_update in state.pending_stat_updates.drain(..) {
                        session.give(stat_update);
                    }

                    // TODO do only every once in a while .. ?
                    state.clean_ooo_events(*time.time());
                });
            }
        })
    }
}

struct PostTreesState {
    worker_id: usize,
    // event ID --> post ID it refers to (root of the tree)
    root_of: HashMap<ID, ID>,
    // out-of-order events: id of missing event --> event that depends on it
    ooo_events:           HashMap<ID, Vec<Event>>,
    pending_stat_updates: Vec<StatUpdate>,
}

impl PostTreesState {
    fn new(worker_id: usize) -> PostTreesState {
        PostTreesState {
            worker_id:            worker_id,
            root_of:              HashMap::<ID, ID>::new(),
            ooo_events:           HashMap::<ID, Vec<Event>>::new(),
            pending_stat_updates: Vec::<StatUpdate>::new(),
        }
    }

    fn update_post_tree(&mut self, event: &Event) -> (Option<ID>, Option<ID>) {
        match event {
            Event::Post(post) => {
                self.root_of.insert(post.post_id, post.post_id);
                (None, Some(post.post_id))
            }
            Event::Like(like) => {
                // likes are not stored in the tree
                // TODO post might not have been received yet
                (Some(like.post_id), Some(like.post_id)) // can only like a post
            }
            Event::Comment(comment) => {
                let reply_to_id = comment.reply_to_post_id.or(comment.reply_to_comment_id).unwrap();

                if let Some(&root_post_id) = self.root_of.get(&reply_to_id) {
                    self.root_of.insert(comment.comment_id, root_post_id);
                    (Some(reply_to_id), Some(root_post_id))
                } else {
                    (Some(reply_to_id), None)
                }
            }
        }
    }

    /// process events that have `id` as their target,
    /// recursively process the newly inserted ids
    fn process_ooo_events(&mut self, id: ID) {
        if let Some(events) = self.ooo_events.remove(&id) {
            println!("-- {} for id = {:?}", "process_ooo_events".bold().yellow(), id);

            let mut new_ids = Vec::new();
            for event in events {
                let (opt_target_id, opt_root_post_id) = self.update_post_tree(&event);
                assert!(opt_target_id.unwrap() == id, "wtf");
                let root_post_id =
                    opt_root_post_id.expect("[process_ooo_events] root_post_id is None");

                self.append_stat_update(&event, root_post_id.u64());

                if let Some(new_id) = event.id() {
                    new_ids.push(new_id)
                }
            }

            // adding events might unlock other ooo events
            for new_id in new_ids.drain(..) {
                self.process_ooo_events(new_id);
            }
        }
    }

    fn push_ooo_event(&mut self, event: Event, target_id: ID) {
        self.ooo_events.entry(target_id).or_insert(Vec::new()).push(event);
    }

    fn clean_ooo_events(&mut self, timestamp: u64) {
        self.ooo_events = self
            .ooo_events
            .clone()
            .into_iter()
            .filter(|(_, events)| events.iter().all(|event| event.timestamp() > timestamp))
            .collect::<HashMap<_, _>>();
    }

    fn append_stat_update(&mut self, event: &Event, root_post_id: u64) {
        let update_type = match event {
            Event::Post(_) => StatUpdateType::Post,
            Event::Like(_) => StatUpdateType::Like,
            Event::Comment(comment) => {
                if comment.reply_to_post_id != None {
                    StatUpdateType::Comment
                } else {
                    StatUpdateType::Reply
                }
            }
        };

        let update = StatUpdate {
            update_type: update_type,
            post_id:     root_post_id,
            person_id:   event.person_id(),
            timestamp:   event.timestamp(),
        };

        self.pending_stat_updates.push(update);
    }

    fn dump(&self) {
        println!(
            "{}",
            format!(
                "{} {}",
                format!("[W{}]", self.worker_id).bold().blue(),
                "Current state".bold().blue()
            )
        );
        println!("  root_of -- {:?}", self.root_of);
        self.dump_ooo_events(2);
    }

    fn dump_ooo_events(&self, num_spaces: usize) {
        let spaces = " ".repeat(num_spaces);
        println!("{}---- ooo_events", spaces);
        for (post_id, events) in self.ooo_events.iter() {
            println!(
                "{}{:?} -- \n{}  {}",
                spaces,
                post_id,
                spaces,
                events
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(&format!("\n{}  ", spaces))
            );
        }
        println!("{}----", spaces);
    }
}
