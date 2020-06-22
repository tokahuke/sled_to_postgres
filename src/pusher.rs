use futures::future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time;

use super::{name_for_tree, ReplicationUpdate, BATCH_SIZE};

pub struct ReplicationPusher {
    tree_name: String,
    queue_sender: yaque::Sender,
    subscriber: sled::Subscriber,
    is_shutdown: Arc<AtomicBool>,
    inactive_period: time::Duration,
}

impl ReplicationPusher {
    pub fn new<P: AsRef<Path>>(
        replication_dir: P,
        tree: &sled::Tree,
        prefix: &[u8],
        is_shutdown: Arc<AtomicBool>,
        inactive_period: time::Duration,
    ) -> Result<ReplicationPusher, crate::Error> {
        let tree_name = name_for_tree(tree, prefix);
        let queue_sender = yaque::Sender::open(replication_dir.as_ref().join(&tree_name))?;

        Ok(ReplicationPusher {
            tree_name,
            queue_sender,
            subscriber: tree.watch_prefix(prefix),
            is_shutdown,
            inactive_period,
        })
    }

    pub async fn push_events(mut self) {
        log::info!("starting to push events for {}", self.tree_name);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut batch_number: usize = 0;

        // Create batch saver thingy:
        let queue_sender = &mut self.queue_sender;
        let tree_name = &self.tree_name;
        let mut save = move |batch: &mut Vec<_>| {
            queue_sender.send_batch(&*batch).expect("queue error");
            batch.clear();
            log::trace!("sent");

            batch_number += 1;

            // Save every now and then...
            if batch_number % 3 == 0 {
                log::trace!("saving queue state for {}", tree_name);
                queue_sender.save().expect("queue error");
                log::trace!("queue state saved for {}", tree_name);
            }
        };

        // Now, to the main course:
        loop {
            let timeout = time::delay_for(self.inactive_period);
            match future::select(&mut self.subscriber, timeout).await {
                // Got timeout:
                future::Either::Right((_, _)) => {
                    if self.is_shutdown.load(Ordering::Relaxed) {
                        log::debug!("is shutdown and has timed out");
                        break;
                    } else {
                        // Let's take the opportunity to sate stuff:
                        log::trace!("has timed out but is not shutdown");
                        save(&mut batch);
                    }
                }
                // Subscriber ended:
                future::Either::Left((None, _)) => {
                    log::info!("subscriber to {} finished", self.tree_name);
                    break;
                }
                // Got event:
                future::Either::Left((Some(event), _)) => {
                    log::trace!("got event for {}", self.tree_name);

                    let update = match event {
                        sled::Event::Insert { key, value } => ReplicationUpdate::Insert {
                            key: key.to_vec(),
                            value: value.to_vec(),
                        },
                        sled::Event::Remove { key } => {
                            ReplicationUpdate::Remove { key: key.to_vec() }
                        }
                    };

                    log::trace!("pushing event");

                    batch.push(bincode::serialize(&Some(update)).expect("can always serialize"));

                    if batch.len() >= BATCH_SIZE {
                        log::trace!("batch full. Sending to queue for {}", self.tree_name);
                        save(&mut batch);
                    }
                }
            }
        }

        log::debug!("pusher broke out from main loop for {}", self.tree_name);

        if !batch.is_empty() {
            log::trace!("batch was not empty. Will send remaining");
            self.queue_sender.send_batch(&*batch).expect("queue error");
            self.queue_sender.save().expect("queue error");
        }

        log::info!("finished pushing events for {}", self.tree_name);
    }
}
