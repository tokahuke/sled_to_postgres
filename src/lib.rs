//! Replicate your Sled database to Postgres.
//!
//! ## Sample usage
//!
//! This is a sample usage example on a db with a single tree. To replicate more
//! trees, just chain more `Replication::push` calls.
//!
//! ```rust
//! // (`ToSql` is reexported from `tokio_postgres`)
//! use sled_to_postgres::{Replication, ReplicateTree, ToSql};
//!
//! // Open your database:
//! let db = sled::open("data/db").unwrap();
//! // Open your tree.
//! let tree = db.open_tree("a_tree").unwrap();
//!
//! /// Makeshift decode.
//! fn decode(i: &[u8]) -> i32 {
//!     i32::from_be_bytes([i[0], i[1], i[2], i[3]])
//! }
//!
//! // This is how you set up a replication:
//! let setup = Replication::new(
//!         // Put in your credentials.
//!         "host=localhost dbname=a_db user=someone password=idk",
//!         // You will need a location in the disk for temporary data.
//!         "data/replication",
//!     ).push(ReplicateTree {
//!         // This is a replication on the tree `a_tree`.
//!         tree: tree.clone(),
//!         // You may specify a replication only over a given prefix.
//!         prefix: vec![],
//!         // This are the commands for table (and index) creation.
//!         // This needs to be *idempotent* (create _if not exists_).
//!         create_commands: "
//!             create table if not exists a_table (
//!                 x int primary key,
//!                 y int not null
//!             );
//!         ",
//!         // This is the command for one insertion. The replication might need
//!         // to call this repeatedly for the same data.
//!         insert_statement: "
//!             insert into a_table values ($1::int, $2::int)
//!             on conflict (x) do update set x = excluded.x;
//!         ",
//!         // This is how you transform a `(key, value)` into the parameters for
//!         // the above statement.
//!         // ... this is the general and complicated form. You can simplify
//!         // stuff using `params!`.
//!         insert_parameters: |key: &[u8], value: &[u8]| {
//!             vec![
//!                 Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
//!                 Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>,
//!             ]
//!         },
//!         // This is the command for one removal. The replication needs to call
//!         // this repeatedly for the same data.
//!         remove_statement: "delete from a_table where x = $1::int",
//!         // This is how you transform a `key` into the parameters for the above
//!         // statement.
//!         // ... using `params!` makes it more ergonomic.
//!         remove_parameters: |key: &[u8]| params![decode(&*key)],
//!     });
//!     
//! tokio::spawn(async move {
//!     // Do not insert anything before starting the replication. These updates will not be logged.
//!     // tree.insert(&987i32.to_be_bytes(), &654i32.to_be_bytes()).unwrap();
//!     
//!     // Although the current state of the database *will* be dumped with the
//!     // replication when it starts for the first time.
//!
//!     // Start the replication.
//!     let stopper = replication.start().await.unwrap();
//!
//!     // Stopper is a kind of channel:
//!     let (shutdown, handle) = stopper.into();
//!
//!     // Now, insert something in `a_tree`.
//!     tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes()).unwrap();
//!     
//!     // When you are done, trigger shutdown:
//!     // It is understood that _there will be no more db operations after this
//!     // point._
//!     shutdown.trigger();
//!
//!     // ShutdownTrigger doesn't happen immediately. It takes at least 500ms.
//!     // You need not to await this, but it is recommended.
//!     handle.await.unwrap();
//!
//!     // These two last operations could also be accomplished by calling
//!     // `stopper.stop()`.
//! });
//! ```
//!
//! ## Limitations and _caveats_
//!
//! There are some limitations on the current implementation:
//! 1. Do not use foreign key constraints on the replicated tables. Since Sled
//! doesn't have such a concept and updates are inserted in small batches
//! concurrently, the table might not obey this constraint during brief moments.
//! 2. Be careful to start the replications before any updates are done to the
//! database, preferably giving it a head-start of a couple of milliseconds.
//! 3. Be careful to only trigger the end of the replication when you are
//! absolutely sure no more updates are going to be made from that point on.

#[macro_use]
mod util;

mod dumper;
mod error;
mod pusher;
mod sender;
pub mod tuples;

/// Reexport of `tokio_postgres::types::ToSql`:
pub use tokio_postgres::types::ToSql;

pub use crate::error::Error;

use futures::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time;

use crate::dumper::ReplicationDumper;
use crate::pusher::ReplicationPusher;
use crate::sender::{ReplicationSender, ReplicationSenderPool};
use crate::tuples::SqlArrayTuple;

// This is useful for debugging:

const BATCH_SIZE: usize = 256;

/// The canonical identification of a prefix within a tree.
fn name_for_tree(tree: &sled::Tree, prefix: &[u8]) -> String {
    let tree_name = String::from_utf8_lossy(&tree.name()).to_string();
    format!("{}#{}", tree_name, hex::encode(prefix))
}

/// An update on the state of the database. This, contrary to `sled::Event`,
/// serializable.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ReplicationUpdate {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
}

/// Boxed function for transforming from Sled to Postgres for insertion.
type InsertParameters =
    Box<dyn Send + Sync + Fn(&[Vec<u8>], &[Vec<u8>]) -> Vec<Box<dyn ToSql + Send + Sync>>>;
/// Boxed function for transforming from Sled to Postgres for removal.
type RemoveParameters = Box<dyn Send + Sync + Fn(&[Vec<u8>]) -> Vec<Box<dyn ToSql + Send + Sync>>>;

struct ReplicateSpec {
    name: String,
    create_commands: String,
    insert_statement: String,
    remove_statement: String,
    insert_parameters: InsertParameters,
    remove_parameters: RemoveParameters,
}

/// Configuration for replication of a single tree.
pub struct ReplicateTree<'a, Ins, Rm> {
    /// Tree on which to watch for updates.
    pub tree: sled::Tree,
    /// The prefix on which to watch.
    pub prefix: Vec<u8>,
    /// Creation commands for the schema in Postgres. This must be *idempotent*
    /// (not get error if ran twice).
    pub create_commands: &'a str,
    /// Single _prepared_ statement for inserting in Postgres. This must be
    /// *idempotent*, that is, must accommodate repeated updates.
    pub insert_statement: &'a str,
    /// Single _prepared_ statement for removing in Postgres. This must be
    /// *idempotent*, that is, not return error if the supplied key does not
    /// exist.
    pub remove_statement: &'a str,
    /// Conversion from a Sled `(key, value)` pair to Postgres parameters for
    /// inserting.
    pub insert_parameters: Ins,
    /// Conversion from a Sled `key` to Postgres parameters for removal.
    pub remove_parameters: Rm,
}

/// The type controlling replication configuration.
pub struct Replication {
    parameters: String,
    replication_dir: PathBuf,
    trees: Vec<(sled::Tree, Vec<u8>)>,
    replicate_specs: Vec<Arc<ReplicateSpec>>,
    n_connections: usize,
}

impl Replication {
    /// Create a new replication with given connection parameters and
    /// replication directory (for the queues).
    pub fn new<P: AsRef<Path>>(parameters: &str, replication_dir: P) -> Self {
        Replication {
            parameters: parameters.to_owned(),
            replication_dir: replication_dir.as_ref().to_owned(),
            replicate_specs: vec![],
            trees: vec![],
            n_connections: 8, // default.
        }
    }

    /// Sets the number of connections to be used in the replication. If the
    /// initial dump phase in place, it will double the total number of
    /// connections.
    pub fn n_connections(mut self, n_connections: usize) -> Self {
        self.n_connections = n_connections;
        self
    }

    /// Pushes a new replication spec of replication on a tree.
    pub fn push<Ins, InsTuple, Rm, RmTuple>(
        mut self,
        replicate_tree: ReplicateTree<'_, Ins, Rm>,
    ) -> Self
    where
        InsTuple: crate::tuples::SqlTuple,
        RmTuple: crate::tuples::SqlTuple,
        Ins: 'static + Send + Sync + Fn(&[u8], &[u8]) -> InsTuple,
        Rm: 'static + Send + Sync + Fn(&[u8]) -> RmTuple,
    {
        let insert_parameters = replicate_tree.insert_parameters;
        let remove_parameters = replicate_tree.remove_parameters;

        let insert_many = move |keys: &[Vec<u8>], values: &[Vec<u8>]| {
            let mut array_tuple = InsTuple::ArrayTuple::init();
            for (key, value) in keys.iter().zip(values) {
                let tuple = (insert_parameters)(key, value);
                tuple.update(&mut array_tuple)
            }

            array_tuple.into_params()
        };

        let remove_many = move |keys: &[Vec<u8>]| {
            let mut array_tuple = RmTuple::ArrayTuple::init();
            for key in keys {
                let tuple = (remove_parameters)(key);
                tuple.update(&mut array_tuple)
            }

            array_tuple.into_params()
        };

        let spec = ReplicateSpec {
            name: name_for_tree(&replicate_tree.tree, &replicate_tree.prefix),
            create_commands: replicate_tree.create_commands.to_owned(),
            insert_statement: replicate_tree.insert_statement.to_owned(),
            remove_statement: replicate_tree.remove_statement.to_owned(),
            insert_parameters: Box::new(insert_many),
            remove_parameters: Box::new(remove_many),
        };

        self.replicate_specs.push(Arc::new(spec));
        self.trees
            .push((replicate_tree.tree, replicate_tree.prefix));

        self
    }

    /// Directory for queue of a given tree.
    fn dir_for_stream(&self) -> PathBuf {
        self.replication_dir.join("updates")
    }

    /// Directory for queue of a given tree for initial dumping.
    fn dir_for_dump(&self) -> PathBuf {
        self.replication_dir.join("dumps")
    }

    /// Create sender using a given directory name scheme.
    fn make_senders<P: AsRef<Path>>(
        &self,
        base: P,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<ReplicationSenderPool, crate::Error> {
        const DEFAULT_INACTIVE_PERIOD: time::Duration = time::Duration::from_millis(500);
        let pullers = self
            .replicate_specs
            .iter()
            .zip(&self.trees)
            .map(|(spec, (tree, prefix))| {
                ReplicationSender::new(
                    base.as_ref(),
                    tree,
                    prefix,
                    spec.clone(),
                    is_shutdown.clone(),
                    DEFAULT_INACTIVE_PERIOD,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create sender:
        Ok(ReplicationSenderPool::new(
            self.parameters.clone(),
            self.n_connections,
            pullers,
        ))
    }

    fn make_dumpers(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<ReplicationDumper>, crate::Error> {
        self.trees
            .iter()
            .cloned()
            .map(|(tree, prefix)| {
                ReplicationDumper::new(self.dir_for_dump(), tree, prefix, is_shutdown.clone())
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn make_pushers(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<ReplicationPusher>, crate::Error> {
        const DEFAULT_INACTIVE_PERIOD: time::Duration = time::Duration::from_millis(500);

        self.trees
            .iter()
            .map(|(tree, prefix)| {
                ReplicationPusher::new(
                    &self.dir_for_stream(),
                    &tree,
                    &prefix,
                    is_shutdown.clone(),
                    DEFAULT_INACTIVE_PERIOD,
                )
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Tries to recover the underlying queues.
    pub fn recover(&self) -> std::io::Result<()> {
        let dump = self.dir_for_dump();
        let stream = self.dir_for_stream();

        self.trees
            .iter()
            .map(|(tree, prefix)| {
                let tree_name = name_for_tree(tree, prefix);
                let dump = dump.join(&tree_name);
                let stream = stream.join(&tree_name);
                yaque::recovery::unlock_queue(&dump)?;
                yaque::recovery::guess_send_metadata(&dump)?;
                yaque::recovery::unlock_queue(&stream)?;
                yaque::recovery::guess_send_metadata(&stream)?;
                Ok(())
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|_| ())
    }

    /// Sets the dump phase of the replication.
    async fn setup_dump(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<impl Future<Output = ()>, crate::Error> {
        // Create dumpers:
        log::debug!("setting up dumpers");
        let dumpers = self.make_dumpers(is_shutdown.clone())?;

        // Create sender for dump:
        let send_stuff = self
            .make_senders(&self.dir_for_dump(), is_shutdown.clone())?
            .prepare()
            .await?;

        let whole_thing = future::join(
            send_stuff,
            future::join_all(
                dumpers
                    .into_iter()
                    .map(|dumper| tokio::task::spawn_blocking(move || dumper.dump())),
            ),
        )
        .map(|_| ());

        Ok(whole_thing)
    }

    /// Sets the whole system up, returning a future to the completion of the
    /// replication and a channel to _kind of_ trigger the end of the process
    /// of watching for updates.
    pub async fn start(self) -> Result<ReplicationStopper, crate::Error> {
        // Create signaler for shutdown:
        let is_shutdown = Arc::new(AtomicBool::new(false));

        // Create pushers:
        let pushers = self.make_pushers(is_shutdown.clone())?;
        let push_stuff = future::join_all(pushers.into_iter().map(|pusher| pusher.push_updates()));

        // Set up the streaming part:
        let stream_stuff = self
            .make_senders(&self.dir_for_stream(), is_shutdown.clone())?
            .prepare()
            .await?;

        // Set up the conditional dump:
        let maybe_dump = self.setup_dump(is_shutdown.clone()).await?;

        // Create the whole thing!
        let whole_thing = future::join(
            push_stuff,
            maybe_dump.then(move |_| stream_stuff),
            // maybe_dump,
        )
        .map(|_| ());

        Ok(ReplicationStopper {
            handle: tokio::spawn(whole_thing),
            shutdown: ShutdownTrigger { is_shutdown },
        })
    }
}

/// Sends a signal to stop the replication.
#[derive(Debug)]
pub struct ShutdownTrigger {
    is_shutdown: Arc<AtomicBool>,
}

impl Clone for ShutdownTrigger {
    fn clone(&self) -> ShutdownTrigger {
        ShutdownTrigger {
            is_shutdown: self.is_shutdown.clone(),
        }
    }
}

impl ShutdownTrigger {
    /// Makes the replication enter "shutdown mode". After this, if any event
    /// in the pusher side takes more than a predetermined timeout to arrive,
    /// it will be dropped and the pusher will end. This is the way sled works
    /// by now.
    pub fn trigger(&self) {
        self.is_shutdown.fetch_or(true, Ordering::Relaxed);
    }
}

/// A structure that can be used to stop a replication and await on its shutdown.
pub struct ReplicationStopper {
    shutdown: ShutdownTrigger,
    handle: tokio::task::JoinHandle<()>,
}

impl From<ReplicationStopper> for (ShutdownTrigger, tokio::task::JoinHandle<()>) {
    fn from(replication_stopper: ReplicationStopper) -> Self {
        (replication_stopper.shutdown, replication_stopper.handle)
    }
}

impl ReplicationStopper {
    /// Gets a trigger to the replication shutdown. The trigger triggers the
    /// shutdown, but _does not_ await for the replication to complete.
    pub fn get_trigger(&self) -> ShutdownTrigger {
        self.shutdown.clone()
    }

    /// Triggers the shutdown of the replication and awaits for its completion.
    ///
    /// # Panics
    ///
    /// This function panics if the task for the replication has panicked.
    pub async fn stop(self) {
        // Trigger shutdown:
        self.shutdown.trigger();
        self.handle.await.expect("replication panicked")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toy_replication(db: sled::Db) -> Replication {
        fn decode(i: &[u8]) -> i32 {
            i32::from_be_bytes([i[0], i[1], i[2], i[3]])
        }

        Replication::new(
            "host=localhost dbname=a_db user=someone password=idk",
            "data/replication",
        )
        .push(ReplicateTree {
            tree: db.open_tree("a_tree").unwrap(),
            prefix: vec![],
            create_commands: "
                create table if not exists a_table (
                    x int primary key,
                    y int not null
                );
            ",
            insert_statement: "
                insert into a_table
                select * from unnest($1::int[], $2::int[])
                on conflict (x) do update set x = excluded.x;
            ",
            remove_statement: "delete from a_table using unnest($1::int[]) as _ (a) where x = a",
            insert_parameters: |key: &[u8], value: &[u8]| (decode(&*key), decode(&*value)),
            remove_parameters: |key: &[u8]| (decode(&*key),),
        })
    }

    #[tokio::test]
    async fn test_simple() {
        let _ = simple_logger::init_with_level(log::Level::Debug);

        let db = sled::open("data/db").unwrap();
        let tree = db.open_tree("a_tree").unwrap();
        let replication = toy_replication(db.clone());

        tree.insert(&987i32.to_be_bytes(), &654i32.to_be_bytes())
            .unwrap();

        log::info!("setting up");
        let stopper = replication.start().await.unwrap();
        log::info!("spawned");
        tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes())
            .unwrap();
        log::info!("inserted");

        // when you are done, trigger shutdown:
        tokio::time::delay_for(tokio::time::Duration::from_millis(2000)).await;
        log::info!("slept. Waking up");

        stopper.stop().await;
        log::info!("done");
    }
}
