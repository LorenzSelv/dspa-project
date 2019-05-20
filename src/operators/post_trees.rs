use std::collections::HashMap;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

use colored::*;

use crate::event::{Event, ID};

use crate::operators::active_posts::StatUpdate;
use crate::operators::active_posts::StatUpdateType;
use crate::operators::friend_recommendations::RecommendationUpdate;

pub trait PostTrees<G: Scope> {
    fn post_trees(
        &self,
        worker_id: usize,
    ) -> (Stream<G, StatUpdate>, Stream<G, RecommendationUpdate>);
}

impl<G: Scope<Timestamp = u64>> PostTrees<G> for Stream<G, Event> {
    fn post_trees(
        &self,
        worker_id: usize,
    ) -> (Stream<G, StatUpdate>, Stream<G, RecommendationUpdate>) {
        let mut state: PostTreesState = PostTreesState::new(worker_id);

        let mut builder = OperatorBuilder::new("PostTrees".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);

        // declare two output streams, one for each downstream operator
        let (mut stat_output, stat_stream) = builder.new_output();
        let (mut rec_output, rec_stream) = builder.new_output();

        builder.build(move |_| {
            let mut buf = Vec::new();
            move |_frontiers| {
                input.for_each(|time, data| {
                    data.swap(&mut buf);

                    // update the post trees
                    for event in buf.drain(..) {
                        println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());
                        let (opt_target_id, opt_root_post_id) = state.update_post_tree(&event);

                        // check if the root post_id has been already received
                        match opt_root_post_id {
                            Some(root_post_id) => {
                                if let ID::Post(pid) = root_post_id {
                                    state.append_output_updates(&event, pid);
                                } else {
                                    panic!("expect ID::Post, got ID::Comment");
                                }

                                // check whether we can pop some stuff out of the ooo map
                                if let Some(_) = event.id() {
                                    state.process_ooo_events(&event);
                                }
                            }
                            None => {
                                state.push_ooo_event(event, opt_target_id.unwrap());
                            }
                        };

                        // state.dump();
                    }

                    let mut stat_handle = stat_output.activate();
                    let mut rec_handle = rec_output.activate();

                    let mut stat_session = stat_handle.session(&time);
                    let mut rec_session = rec_handle.session(&time);

                    for stat_update in state.pending_stat_updates.drain(..) {
                        stat_session.give(stat_update);
                    }

                    for rec_update in state.pending_rec_updates.drain(..) {
                        rec_session.give(rec_update);
                    }

                    // TODO do only every once in a while .. ?
                    state.clean_ooo_events(*time.time());
                });
            }
        });

        // return the two output streams
        (stat_stream, rec_stream)
    }
}

#[derive(Debug)]
struct Node {
    person_id:    u64, // "creator" of the event
    root_post_id: ID,
}

struct PostTreesState {
    worker_id: usize,
    // event ID --> post ID it refers to (root of the tree)
    root_of: HashMap<ID, Node>,
    // out-of-order events: id of missing event --> event that depends on it
    ooo_events: HashMap<ID, Vec<Event>>,
    // updates to be sent on the stat output stream
    pending_stat_updates: Vec<StatUpdate>,
    // updates to be sent on the recommendation output stream
    pending_rec_updates: Vec<RecommendationUpdate>,
}

impl PostTreesState {
    fn new(worker_id: usize) -> PostTreesState {
        PostTreesState {
            worker_id:            worker_id,
            root_of:              HashMap::<ID, Node>::new(),
            ooo_events:           HashMap::<ID, Vec<Event>>::new(),
            pending_stat_updates: Vec::new(),
            pending_rec_updates:  Vec::new(),
        }
    }

    fn update_post_tree(&mut self, event: &Event) -> (Option<ID>, Option<ID>) {
        match event {
            Event::Post(post) => {
                let node = Node { person_id: post.person_id, root_post_id: post.post_id };
                self.root_of.insert(post.post_id, node);
                (None, Some(post.post_id))
            }
            Event::Like(like) => {
                // likes are not stored in the tree
                let post_id = match self.root_of.get(&like.post_id) {
                    Some(_) => Some(like.post_id), // can only like a post
                    None => None,
                };
                (Some(like.post_id), post_id)
            }
            Event::Comment(comment) => {
                let reply_to_id = comment.reply_to_post_id.or(comment.reply_to_comment_id).unwrap();

                if let Some(root_node) = self.root_of.get(&reply_to_id) {
                    let root_post_id = root_node.root_post_id;
                    let node = Node { person_id: comment.person_id, root_post_id: root_post_id };
                    self.root_of.insert(comment.comment_id, node);
                    (Some(reply_to_id), Some(root_post_id))
                } else {
                    (Some(reply_to_id), None)
                }
            }
        }
    }

    /// process events that have `id` as their target,
    /// recursively process the newly inserted ids
    fn process_ooo_events(&mut self, root_event: &Event) {
        let id = root_event.id().unwrap();
        if let Some(events) = self.ooo_events.remove(&id) {
            println!("-- {} for id = {:?}", "process_ooo_events".bold().yellow(), id);

            let mut new_events = Vec::new();
            for event in events {
                let (opt_target_id, opt_root_post_id) = self.update_post_tree(&event);
                assert!(opt_target_id.unwrap() == id, "wtf");
                let root_post_id =
                    opt_root_post_id.expect("[process_ooo_events] root_post_id is None");

                // only use this event if its timestamp is greater or equal to the parent's.
                if event.timestamp() >= root_event.timestamp() {
                    self.append_output_updates(&event, root_post_id.u64());

                    if let Some(_) = event.id() {
                        new_events.push(event);
                    }
                }
            }

            // adding events might unlock other ooo events
            for event in new_events.drain(..) {
                self.process_ooo_events(&event);
            }
        }
    }

    fn push_ooo_event(&mut self, event: Event, target_id: ID) {
        self.ooo_events.entry(target_id).or_insert(Vec::new()).push(event);
    }

    fn clean_ooo_events(&mut self, timestamp: u64) {
        println!("before clean {:?}", self.ooo_events);
        self.ooo_events = self
            .ooo_events
            .clone()
            .into_iter()
            .filter(|(_, events)| events.iter().all(|event| event.timestamp() > timestamp))
            .collect::<HashMap<_, _>>();
        println!("after clean {:?}", self.ooo_events);
    }

    /// generate all output updates for the current event
    fn append_output_updates(&mut self, event: &Event, root_post_id: u64) {
        self.append_stat_update(&event, root_post_id);
        self.append_rec_update(&event, root_post_id);
    }

    /// given an event (and the current state of the post trees),
    /// generate a new stat update and append it to the pending list
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

    /// given an event (and the current state of the post trees),
    /// generate a new recommendation update and append it to the pending list
    fn append_rec_update(&mut self, event: &Event, root_post_id: u64) {
        if let Event::Post(post) = event {
            let update = RecommendationUpdate::Post {
                timestamp: event.timestamp(),
                person_id: event.person_id(),
                forum_id:  post.forum_id,
                tags:      post.tags.clone(),
            };
            self.pending_rec_updates.push(update)
        } else if let Event::Comment(comment_event) = event {
            // TODO emit also normal comments (not only replies)
            if comment_event.reply_to_post_id != None {
                let to_person_id = self.root_of.get(&ID::Post(root_post_id)).unwrap().person_id;
                let update = RecommendationUpdate::Comment {
                    timestamp:      event.timestamp(),
                    from_person_id: event.person_id(),
                    to_person_id:   to_person_id,
                };
                self.pending_rec_updates.push(update)
            }
        } else if let Event::Like(_) = event {
            let to_person_id = self.root_of.get(&ID::Post(root_post_id)).unwrap().person_id;
            let update = RecommendationUpdate::Like {
                timestamp:      event.timestamp(),
                from_person_id: event.person_id(),
                to_person_id:   to_person_id,
            };
            self.pending_rec_updates.push(update)
        }
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
