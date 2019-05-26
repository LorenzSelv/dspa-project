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

/// Generic operator that delivers periodic notifications.
///
/// The interface expect the following:
/// 1) window_size: size of the window in seconds (e.g. 30*60 for every 30 minutes)
/// 2) op_name: name of the operator
/// 3) state:   the initial state of the operator
/// 4) on_new_input: callback function called when a new event is received;
///                  it should update the `state` (passed as mutable)
/// 5) on_notify: callback function called when a notification is delivered;
///               given the state, it should emit the output for that window
///
/// This generic operator serves two main purposes:
/// - implements the windowing logic:
///     after the first events is received, it sets
///     periodic notification every `window_size` starting from
///     the timestamp of the first event
///
/// - handle out-of-order events:
///     the operator internally keeps two copy of the state;
///     one for the current window `cur_state`, and
///     one for the next window `next_state`.
///     When an event is delivered it will update either
///     both states or only the `next_state` if the event
///     belongs to the next window (this could happen as
///     the internal time capability is delayed by the source
///     operator taking into account the maximum bounded delay)
///
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

        // keep two states, current/next window
        let mut cur_state = state;
        let mut next_state = cur_state.clone();

        self.unary_notify(Pipeline, op_name, None, move |input, output, notificator| {
            input.for_each(|time, data| {
                let mut buf = Vec::<D>::new();
                data.swap(&mut buf);

                if first_notification {
                    next_notification_time =
                        buf.iter().map(|el| el.timestamp()).min().expect("wtf") + window_size;
                    first_notification = false;
                    // Set up the first notification
                    notificator.notify_at(time.delayed(&next_notification_time));
                }

                for el in buf.drain(..) {
                    // use caller-provided function to update the next_state
                    on_new_input(&mut next_state, &el, next_notification_time);
                    // the event might belong to the next window
                    // update the current state only if it does not
                    if el.timestamp() <= next_notification_time {
                        on_new_input(&mut cur_state, &el, next_notification_time);
                    }
                }
            });

            // Shut up borrow checker
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

                // we are switching to the next window, flip the state
                cur_state = next_state.clone();
            });

            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                // setup next notification to the previous notification time + window_size
                next_notification_time = *cap.time() + window_size;
                notificator.notify_at(cap.delayed(&next_notification_time));
            }
        })
    }
}
