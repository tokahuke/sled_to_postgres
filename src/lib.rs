//! Replicate your Sled database to Postgres.
//!
//! This is a sample usage example:
//! ```rust
//! use sled_to_postgres::{Replication, ReplicateTree};
//! use tokio_postgres::types::ToSql;
//!
//! let db = sled::open("data/db").unwrap();
//! let tree = db.open_tree("a_tree").unwrap();
//!
//! fn decode(i: &[u8]) -> i32 {
//!     i32::from_be_bytes([i[0], i[1], i[2], i[3]])
//! }
//!
//! let setup = Replication::new(
//!         "host=localhost dbname=a_db user=someone password=idk",
//!         "data/replication",
//!     ).push(ReplicateTree {
//!         tree: tree.clone(),
//!         prefix: vec![],
//!         create_commands: "
//!             create table if not exists a_table (
//!                 x int primary key,
//!                 y int not null
//!             );
//!         ",
//!         insert_statement: "
//!             insert into a_table values ($1::int, $2::int)
//!             on conflict (x) do update set x = excluded.x;
//!         ",
//!         remove_statement: "delete from a_table where x = $1::int",
//!         insert_parameters: |key: &[u8], value: &[u8]| vec![
//!             Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
//!             Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>
//!         ],
//!         remove_parameters: |key: &[u8]| vec![
//!             Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>
//!         ],
//!     }).setup();
//!
//! tokio::spawn(async move {
//!     let (replication, shutdown) = setup.await.unwrap();
//!     let handle = tokio::spawn(replication);
//!     tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes()).unwrap();
//!     
//!     // when you are done, trigger shutdown:
//!     shutdown.trigger();
//!     handle.await.unwrap();
//! });
//! ```

mod dumper;
mod pusher;
mod sender;

use failure_derive::Fail;
use futures::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time;
use tokio_postgres::types::ToSql;

use dumper::Dumper;
use pusher::ReplicationPusher;
use sender::{ReplicationPuller, ReplicationSender};

const DEFAULT_INACTIVE_PERIOD: time::Duration = time::Duration::from_millis(500);
const BATCH_SIZE: usize = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ReplicationUpdate {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
}

/// Error kind for `sled_to_postgres`.
#[derive(Debug, Fail)]
pub enum Error {
    /// An underlying error from `sled`.
    #[fail(display = "sled error: {}", _0)]
    Sled(sled::Error),
    /// An underlying IO error.
    #[fail(display = "io error: {}", _0)]
    Io(io::Error),
    /// An underlying error from Postgres.
    #[fail(display = "postgres error: {}", _0)]
    Postgres(tokio_postgres::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Error {
        Error::Sled(error)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Error {
        Error::Postgres(error)
    }
}

/// Boxed function for transforming from Sled to Postgres for insertion.
type InsertParameters =
    Box<dyn Send + Sync + Fn(&[u8], &[u8]) -> Vec<Box<dyn ToSql + Send + Sync>>>;
/// Boxed function for transforming from Sled to Postgres for removal.
type RemoveParameters = Box<dyn Send + Sync + Fn(&[u8]) -> Vec<Box<dyn ToSql + Send + Sync>>>;

struct ReplicateSpec {
    create_commands: String,
    insert_statement: String,
    remove_statement: String,
    insert_parameters: InsertParameters,
    remove_parameters: RemoveParameters,
}

/// Configuration for replication of a single tree.
pub struct ReplicateTree<'a, I, R> {
    /// Tree on which to watch for updates.
    pub tree: sled::Tree,
    /// The prefix on which to watch.
    pub prefix: Vec<u8>,
    /// Creation commands for the schema in Postgres. This must be *idempotent*
    /// (not get error if ran twice).
    pub create_commands: &'a str,
    /// Single _prepared_ statement for inserting in Postgres. This must be
    /// *idempotent*, that is, must accommodate repeated events.
    pub insert_statement: &'a str,
    /// Single _prepared_ statement for removing in Postgres. This must be
    /// *idempotent*, that is, not return error if the supplied key does not
    /// exist.
    pub remove_statement: &'a str,
    /// Conversion from a Sled `(key, value)` pair to Postgres parameters for
    /// inserting.
    pub insert_parameters: I,
    /// Conversion from a Sled `key` to Postgres parameters for removal.
    pub remove_parameters: R,
}

/// The type controlling replication configuration.
pub struct Replication {
    parameters: String,
    replication_dir: PathBuf,
    trees: Vec<(sled::Tree, Vec<u8>)>,
    replicate_specs: Vec<Arc<ReplicateSpec>>,
}

impl Replication {
    /// Create a new replication with given connection parameters and
    /// replication directory (for the queues).
    pub fn new<P: AsRef<Path>>(parameters: &str, replication_dir: P) -> Replication {
        Replication {
            parameters: parameters.to_owned(),
            replication_dir: replication_dir.as_ref().to_owned(),
            replicate_specs: vec![],
            trees: vec![],
        }
    }

    /// Pushes a new replication spec of replication on a tree.
    pub fn push<'a, I, R>(mut self, replicate_tree: ReplicateTree<'a, I, R>) -> Self
    where
        I: 'static + Send + Sync + Fn(&[u8], &[u8]) -> Vec<Box<dyn ToSql + Send + Sync>>,
        R: 'static + Send + Sync + Fn(&[u8]) -> Vec<Box<dyn ToSql + Send + Sync>>,
    {
        let spec = ReplicateSpec {
            create_commands: replicate_tree.create_commands.to_owned(),
            insert_statement: replicate_tree.insert_statement.to_owned(),
            remove_statement: replicate_tree.remove_statement.to_owned(),
            insert_parameters: Box::new(replicate_tree.insert_parameters),
            remove_parameters: Box::new(replicate_tree.remove_parameters),
        };

        self.replicate_specs.push(Arc::new(spec));
        self.trees
            .push((replicate_tree.tree, replicate_tree.prefix));

        self
    }

    fn file_name_for_tree(tree: &sled::Tree, prefix: &[u8]) -> String {
        let tree_name = String::from_utf8_lossy(&tree.name()).to_string();
        if !prefix.is_empty() {
            format!("{}#{}", tree_name, hex::encode(prefix),)
        } else {
            tree_name
        }
    }

    /// Directory for queue of a given tree.
    fn dir_for_tree(&self, tree: &sled::Tree, prefix: &[u8]) -> PathBuf {
        self.replication_dir
            .join("updates")
            .join(&*Replication::file_name_for_tree(tree, prefix))
    }

    /// Directory for queue of a given tree for initial dumping.
    fn dir_for_dump(&self, tree: &sled::Tree, prefix: &[u8]) -> PathBuf {
        self.replication_dir
            .join("dumps")
            .join(&*Replication::file_name_for_tree(tree, prefix))
    }

    // The name of the file to test if the dump phase is over.
    fn path_for_is_dumped(&self) -> PathBuf {
        self.replication_dir.join("is_dumped.flag")
    }

    /// Create sender using a given directory name scheme.
    fn make_sender<F>(&self, dir_name: F) -> Result<ReplicationSender, crate::Error>
    where
        F: Fn(&sled::Tree, &[u8]) -> PathBuf,
    {
        let pullers = self
            .replicate_specs
            .iter()
            .zip(&self.trees)
            .map(|(spec, (tree, prefix))| {
                ReplicationPuller::new(dir_name(tree, prefix), spec.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create sender:
        Ok(ReplicationSender::new(self.parameters.clone(), pullers))
    }

    fn make_dumpers(&self, is_shutdown: Arc<AtomicBool>) -> Result<Vec<Dumper>, crate::Error> {
        self.trees
            .iter()
            .cloned()
            .map(|(tree, prefix)| {
                Dumper::new(
                    self.dir_for_dump(&tree, &prefix),
                    tree,
                    prefix,
                    is_shutdown.clone(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn make_pushers(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<ReplicationPusher>, crate::Error> {
        self.trees
            .iter()
            .map(|(tree, prefix)| {
                ReplicationPusher::new(
                    &self.dir_for_tree(&tree, prefix),
                    &tree,
                    &prefix,
                    is_shutdown.clone(),
                    DEFAULT_INACTIVE_PERIOD,
                )
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Sets the dump phase of th replication.
    async fn setup_dump(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<impl Future<Output = bool>, crate::Error> {
        // Create dumpers:
        log::debug!("setting up dumpers");
        let dumpers = self.make_dumpers(is_shutdown.clone())?;

        // Create sender for dump:
        log::debug!("setting up senders");
        let send_stuff = self
            .make_sender(|tree, prefix| self.dir_for_dump(tree, prefix))?
            .prepare()
            .await?;
        log::debug!("done setting up sender");

        // Here it is `join`: wait for everything to end.
        let whole_thing = future::join(
            future::join_all(
                dumpers
                    .into_iter()
                    .map(|dumper| tokio::task::spawn_blocking(move || dumper.dump())),
            ),
            Box::pin(send_stuff),
        )
        .map(|(was_shutdown, _)| {
            was_shutdown
                .into_iter()
                .any(|flag| flag.expect("task panicked"))
        });

        Ok(whole_thing)
    }

    /// Sets the streaming phase of the replication.
    async fn setup_streaming(
        &self,
        is_shutdown: Arc<AtomicBool>,
    ) -> Result<impl Future<Output = ()>, crate::Error> {
        // Create pushers:
        let pushers = self.make_pushers(is_shutdown.clone())?;

        // Create sender:
        let send_stuff = self
            .make_sender(|tree, prefix| self.dir_for_tree(tree, prefix))?
            .prepare()
            .await?;

        // Here it is `select` because pushers may end and senders will never
        // end in this case.
        let whole_thing = future::select(
            future::join_all(pushers.into_iter().map(|pusher| pusher.push_events())),
            Box::pin(send_stuff),
        )
        .map(|_| ());

        Ok(whole_thing)
    }

    /// Sets the whole system up, returning a future to the completion of the
    /// replication and a channel to _kind of_ trigger the end of the process
    /// of watching for events.
    pub async fn start(self) -> Result<(tokio::task::JoinHandle<()>, Shutdown), crate::Error> {
        // Create signaler for shutdown:
        let is_shutdown = Arc::new(AtomicBool::new(false));

        // Set up the conditional dump:
        let path_for_is_dumped = self.path_for_is_dumped();
        let maybe_dump = if !path_for_is_dumped.exists() {
            Some(self.setup_dump(is_shutdown.clone()).await?)
        } else {
            None
        };

        let dumping_part = async move {
            // Check if you must dump:
            if let Some(dump) = maybe_dump {
                // Shutdown if you have to, else cap it off:
                if dump.await {
                    // Cap it off:
                    File::create(path_for_is_dumped).expect("could not create `is_dumped.flag`");
                }
            }
        };

        // Set up the streaming part:
        let streaming_part = self.setup_streaming(is_shutdown.clone()).await?;

        // Assemble the whole thing:
        let whole_thing = future::join(dumping_part, streaming_part).map(|_| ());

        Ok((tokio::spawn(whole_thing), Shutdown { is_shutdown }))
    }
}

/// Sends a signal to stop the replication.
#[derive(Debug)]
pub struct Shutdown {
    is_shutdown: Arc<AtomicBool>,
}

impl Shutdown {
    /// Makes pusher enter "shutdown mode". After this, if any event takes more
    /// than a predetermined timeout to arrive, it will be dropped and the
    /// pusher will end. This is the way sled works by now.
    pub fn trigger(&self) {
        self.is_shutdown.fetch_or(true, Ordering::Relaxed);
    }
}

#[macro_export]
macro_rules! params {
    ($($item:expr),*) => {
        vec![$(
            Box::new($item) as Box<dyn tokio_postgres::types::ToSql + Send + Sync>,
        )*]
    };
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
                insert into a_table values ($1::int, $2::int)
                on conflict (x) do update set x = excluded.x;
            ",
            remove_statement: "delete from a_table where x = $1::int",
            insert_parameters: |key: &[u8], value: &[u8]| params![decode(&*key), decode(&*value)],
            remove_parameters: |key: &[u8]| params![decode(&*key)],
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
        let (replication, shutdown) = replication.start().await.unwrap();
        log::info!("spawned");
        tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes())
            .unwrap();
        log::info!("inserted");

        // when you are done, trigger shutdown:
        tokio::time::delay_for(tokio::time::Duration::from_millis(1000)).await;
        log::info!("slept. Waking up");

        shutdown.trigger();
        log::info!("triggered");
        replication.await.unwrap();
        log::info!("done");
    }
}
