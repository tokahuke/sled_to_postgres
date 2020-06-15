use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::{file_name_for_tree, ReplicationUpdate, BATCH_SIZE};

pub struct ReplicationDumper {
    queue_sender: yaque::Sender,
    tree: sled::Tree,
    prefix: Vec<u8>,
    is_shutdown: Arc<AtomicBool>,
    key_file: PathBuf,
    is_dumped_flag: PathBuf,
}

impl ReplicationDumper {
    pub fn new<P: AsRef<Path>>(
        replication_dir: P,
        tree: sled::Tree,
        prefix: Vec<u8>,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<ReplicationDumper, crate::Error> {
        let queue_sender = yaque::Sender::open(replication_dir.as_ref())?;
        let key_file = replication_dir.as_ref().join("last-key");
        let is_dumped_flag = replication_dir.as_ref().join("is-dumped.flag");

        Ok(ReplicationDumper {
            queue_sender,
            tree,
            prefix,
            is_shutdown,
            key_file,
            is_dumped_flag,
        })
    }

    pub fn was_dumped(&self) -> bool {
        self.is_dumped_flag.exists()
    }

    fn get_last_key(&self) -> Option<Vec<u8>> {
        fs::read(&self.key_file)
            .map_err(|_| log::info!("could not open last key file"))
            .ok()
    }

    fn save_last_key(&self, key: sled::IVec) {
        fs::write(&self.key_file, &*key).expect("could not save last key")
    }

    /// Does dump, returning where it has ended or not (because of shutdown).
    pub fn dump(mut self) {
        // If dumped, do not dump again!
        if self.was_dumped() {
            log::info!(
                "{} already dumped",
                file_name_for_tree(&self.tree, &self.prefix)
            );
            return;
        }

        // Else, let's dump:
        log::info!("starting to dump events");
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut batch_number: usize = 0;

        // Create iterator over prefix:
        let mut iter = self.tree.scan_prefix(&self.prefix);

        // Now, where did I stop?
        if let Some(last_key) = self.get_last_key() {
            log::info!("found last key. Will advance iterator..");
            let last_key: sled::IVec = last_key.into();

            // Advance while not there (TODO it's tricky to use range here, but
            // hopefully not impossible):
            while let Some(maybe_pair) = iter.next() {
                // Return on error.
                let (key, _value) = maybe_pair.expect("db error");

                // Will always hit on equality (>= for clarity):
                if key >= last_key {
                    break;
                }
            }

            log::debug!("iterator advanced");
        }

        // And now, just do a plain old iteration:
        for maybe_pair in iter {
            log::debug!("got pair");
            // Return on error.
            let (key, value) = maybe_pair.expect("db error");

            // Push update on success.
            let update = Some(ReplicationUpdate::Insert {
                key: key.to_vec(),
                value: value.to_vec(),
            });

            batch.push(bincode::serialize(&update).expect("can always serialize"));

            // If at capacity, save!
            if batch.len() >= BATCH_SIZE {
                log::trace!("batch full. Sending to queue");
                self.queue_sender.send_batch(&batch).expect("queue error");
                batch.clear();
                log::trace!("sent");

                batch_number += 1;

                // Save every now and then...
                if batch_number % 3 == 0 {
                    log::trace!("saving queue state");
                    self.queue_sender.save().expect("queue error");
                    log::trace!("queue state saved");
                }

                // Now, let's see if it is time to stop...
                if self.is_shutdown.load(Ordering::Relaxed) {
                    log::info!("dump told to shut down");
                    self.save_last_key(key);

                    // Returning early prevents dump to be capped off.
                    return;
                }
            }
        }

        log::debug!("dumper broke off the main loop");

        // Send the last batch and cap it off.
        batch.push(
            bincode::serialize::<Option<ReplicationUpdate>>(&None).expect("can always serialize"),
        );
        self.queue_sender.send_batch(&batch).expect("queue error");

        // Indicate that dump is over for this dumper:
        fs::File::create(self.is_dumped_flag).expect("failed to create flag file");

        log::info!("finished dumping events");
    }
}
