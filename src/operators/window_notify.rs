use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait Timestamp {
    fn timestamp(&self) -> u64;
}

pub trait WindowNotify<
    G: Scope<Timestamp = u64>,
    D: Data + Timestamp + Debug,
    S: Clone + 'static,
    O: Data,
>
{
    fn window_notify(
        &self,
        window_size: u64,
        op_name: &'static str,
        state: S,
        on_new_input: impl Fn(&mut S, &D, u64) + 'static,
        on_notify: impl Fn(&mut S, u64) -> O + 'static,
    ) -> Stream<G, O>;
}

impl<G: Scope<Timestamp = u64>, D: Data + Timestamp + Debug, S: Clone + 'static, O: Data>
    WindowNotify<G, D, S, O> for Stream<G, D>
{
    fn window_notify(
        &self,
        window_size: u64,
        op_name: &'static str,
        state: S,
        on_new_input: impl Fn(&mut S, &D, u64) + 'static,
        on_notify: impl Fn(&mut S, u64) -> O + 'static,
    ) -> Stream<G, O> {
        let mut first_notification = true;
        let mut next_notification_time = std::u64::MAX;

        let mut cur_state = state;
        let mut next_state = cur_state.clone();

        self.unary_notify(Pipeline, op_name, None, move |input, output, notificator| {
            input.for_each(|time, data| {
                println!("time is {}", *time.time());
                let mut buf = Vec::<D>::new();
                data.swap(&mut buf);

                if first_notification {
                    println!("buf is {:?}", buf);
                    next_notification_time =
                        buf.iter().map(|el| el.timestamp()).min().expect("wtf") + window_size;
                    first_notification = false;
                    // Set up the first notification.
                    println!("next_notification is {}", next_notification_time);
                    println!("current time is {}", *time.time());
                    notificator.notify_at(time.delayed(&next_notification_time));
                    println!("here");
                }

                for el in buf.drain(..) {
                    // use caller-provided function to update the state
                    on_new_input(&mut next_state, &el, next_notification_time);
                    // the event might belong to the next window
                    // update the current state only if it does not
                    if el.timestamp() <= next_notification_time {
                        on_new_input(&mut cur_state, &el, next_notification_time);
                    }
                }
            });

            // Some setup in order to schedule new notifications inside a notification.
            let notified_time = None;
            let ref1 = Rc::new(RefCell::new(notified_time));
            let ref2 = Rc::clone(&ref1);

            notificator.for_each(|time, _, _| {
                let mut borrow = ref1.borrow_mut();
                *borrow = Some(time.clone());

                // use caller-provided function to generate the output
                let out = on_notify(&mut cur_state, *time.time());

                let mut session = output.session(&time);
                session.give(out);

                // we are switching to the next window, update state
                cur_state = next_state.clone();
            });

            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                next_notification_time = *cap.time() + window_size;
                notificator.notify_at(cap.delayed(&next_notification_time));
            }
        })
    }
}
