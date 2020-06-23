use futures::future;
use futures::prelude::*;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{delay_for, Duration};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Statement};

use super::{name_for_tree, ReplicateSpec, ReplicationUpdate};

const UPDATE_EXPONENTIAL_BACKOFF: ExponentialBackoff = ExponentialBackoff {
    millis: 10.0,
    backoff_rate: 2.0,
    retries: 8,
    stabilize: false,
    is_new: true,
};

const CONNECTION_EXPONENTIAL_BACKOFF: ExponentialBackoff = ExponentialBackoff {
    millis: 10.0,
    backoff_rate: 2.0,
    retries: 8,
    stabilize: true,
    is_new: true,
};

#[derive(Clone)]
struct ExponentialBackoff {
    millis: f32,
    backoff_rate: f32,
    retries: usize,
    stabilize: bool,
    is_new: bool,
}

impl ExponentialBackoff {
    async fn tick(&mut self) -> bool {
        if self.is_new {
            self.is_new = false;
            true
        } else if self.retries > 0 {
            delay_for(Duration::from_millis(self.millis as u64)).await;
            self.millis *= self.backoff_rate;
            self.retries -= 1;

            true
        } else if self.stabilize {
            delay_for(Duration::from_millis(self.millis as u64)).await;

            true
        } else {
            false
        }
    }
}

async fn try_connect(parameters: &str) -> Result<Client, crate::Error> {
    let mut builder = TlsConnector::builder();
    builder.danger_accept_invalid_certs(true);
    let connector = builder.build().expect("could not build TLS connector");
    let (client, connection) =
        tokio_postgres::connect(parameters, MakeTlsConnector::new(connector)).await?;

    tokio::spawn(async move {
        connection.await.ok();
    });

    Ok(client)
}

async fn connect(parameters: &str) -> Client {
    let mut backoff = CONNECTION_EXPONENTIAL_BACKOFF.clone();

    loop {
        backoff.tick().await;
        match try_connect(parameters).await {
            Ok(connection) => break connection,
            Err(err) => log::warn!("could not init sql system: {}", err),
        }
    }
}

/// A connection with its prepared statements.
struct SqlSystemInner {
    parameters: String,
    client: Client,
    replicate_specs: Vec<Arc<ReplicateSpec>>,
    insert_statements: Vec<Statement>,
    remove_statements: Vec<Statement>,
}

impl SqlSystemInner {
    async fn init(
        parameters: String,
        replicate_specs: Vec<Arc<ReplicateSpec>>,
    ) -> Result<SqlSystemInner, crate::Error> {
        // Create prepared statements which we will use to upload stuff.
        let client = connect(&parameters).await;
        let insert_statements = future::try_join_all(replicate_specs.iter().map(|spec| {
            log::debug!("{}", spec.insert_statement);
            client.prepare(&spec.insert_statement)
        }))
        .await?;
        let remove_statements = future::try_join_all(replicate_specs.iter().map(|spec| {
            log::debug!("{}", spec.remove_statement);
            client.prepare(&spec.remove_statement)
        }))
        .await?;

        Ok(SqlSystemInner {
            parameters,
            client,
            replicate_specs,
            insert_statements,
            remove_statements,
        })
    }
}

enum UpdateEntry {
    Insert(Vec<u8>),
    Remove,
}

#[derive(Default)]
struct UpdateMap {
    n_updates: usize,
    entries: BTreeMap<Vec<u8>, UpdateEntry>,
}

impl UpdateMap {
    /// This function is cleverer than it seems!
    fn push(&mut self, update: ReplicationUpdate) {
        match update {
            ReplicationUpdate::Insert { key, value } => {
                self.entries.insert(key, UpdateEntry::Insert(value));
            }
            ReplicationUpdate::Remove { key } => {
                self.entries.insert(key, UpdateEntry::Remove);
            }
        }

        self.n_updates += 1;
    }

    fn len(&self) -> usize {
        self.n_updates
    }

    fn into_batches(self) -> [UpdateBatch; 2] {
        let mut insert_keys = vec![];
        let mut insert_values = vec![];
        let mut removes = vec![];

        for (key, update_entry) in self.entries {
            match update_entry {
                UpdateEntry::Insert(value) => {
                    insert_keys.push(key);
                    insert_values.push(value);
                }
                UpdateEntry::Remove => removes.push(key),
            }
        }

        [
            UpdateBatch::Insert(insert_keys, insert_values),
            UpdateBatch::Remove(removes),
        ]
    }
}

enum UpdateBatch {
    Insert(Vec<Vec<u8>>, Vec<Vec<u8>>),
    Remove(Vec<Vec<u8>>),
}

struct SqlSystem {
    inner: RwLock<SqlSystemInner>,
    update_lock: Mutex<()>,
}

impl SqlSystem {
    /// Start a new system.
    async fn init(
        parameters: String,
        replicate_specs: Vec<Arc<ReplicateSpec>>,
    ) -> Result<SqlSystem, crate::Error> {
        Ok(SqlSystem {
            inner: RwLock::new(SqlSystemInner::init(parameters, replicate_specs).await?),
            update_lock: Mutex::new(()),
        })
    }

    async fn trigger_refresh(&self) {
        match self.update_lock.try_lock() {
            // Somebody is already doing that for you.
            Err(_) => {}
            Ok(guard) => {
                let mut inner = self.inner.write().await;
                *inner =
                    SqlSystemInner::init(inner.parameters.clone(), inner.replicate_specs.clone())
                        .await
                        .expect("prepared statements worked the first time");

                drop(guard);
            }
        }
    }

    async fn insert(
        &self,
        spec_number: usize,
        insert_keys: &[Vec<u8>],
        insert_values: &[Vec<u8>],
    ) -> Result<(), crate::Error> {
        let guard = self.inner.read().await;
        let spec = &guard.replicate_specs[spec_number];
        let insert = &guard.insert_statements[spec_number];

        let parameters = (spec.insert_parameters)(insert_keys, insert_values);

        // Need to do like this to please borrow checker:
        let mut parameter_refs = Vec::with_capacity(parameters.len());
        for param in &parameters {
            parameter_refs.push(param.as_ref() as &(dyn ToSql + Sync));
        }

        let x = guard.client.query(insert, &*parameter_refs);

        x.await?;

        Ok(())
    }

    async fn remove(
        &self,
        spec_number: usize,
        remove_keys: &[Vec<u8>],
    ) -> Result<(), crate::Error> {
        let guard = self.inner.read().await;
        let spec = &guard.replicate_specs[spec_number];
        let remove = &guard.remove_statements[spec_number];

        let parameters = (spec.remove_parameters)(remove_keys);

        // Need to do like this to please borrow checker:
        let mut parameter_refs = Vec::with_capacity(parameters.len());
        for param in &parameters {
            parameter_refs.push(param.as_ref() as &(dyn ToSql + Sync));
        }

        let x = guard.client.query(remove, &*parameter_refs);

        x.await?;

        Ok(())
    }

    async fn send(&self, spec_number: usize, batch: &UpdateBatch) -> Result<(), crate::Error> {
        match batch {
            UpdateBatch::Insert(keys, values) => self.insert(spec_number, &keys, &values).await?,
            UpdateBatch::Remove(keys) => self.remove(spec_number, &keys).await?,
        }

        Ok(())
    }
}

pub struct ReplicationSender {
    tree_name: String,
    queue_receiver: yaque::Receiver,
    replicate_spec: Arc<ReplicateSpec>,
    inactive_period: Duration,
}

impl ReplicationSender {
    pub(crate) fn new<P: AsRef<Path>>(
        base: P,
        tree: &sled::Tree,
        prefix: &[u8],
        replicate_spec: Arc<ReplicateSpec>,
        inactive_period: Duration,
    ) -> Result<ReplicationSender, crate::Error> {
        let tree_name = name_for_tree(tree, prefix);
        let queue_receiver = yaque::Receiver::open(base.as_ref().join(&tree_name))?;

        Ok(ReplicationSender {
            tree_name,
            queue_receiver,
            replicate_spec,
            inactive_period,
        })
    }

    async fn send_updates(mut self, spec_number: usize, systems: Arc<Vec<SqlSystem>>) {
        log::info!("puller started to send updates for {}", self.tree_name);
        let mut is_done = false;
        let tree_name = Arc::new(self.tree_name.clone());
        let mut system_iter = systems.iter().cycle();

        // Now, upload stuff while they come in...
        while !is_done {
            // Yes, temporary (you can optimize this one later):
            let mut update_buffer = UpdateMap::default();

            loop {
                // Yes, now that receiving is atomic, I can do this.
                match future::select(
                    Box::pin(self.queue_receiver.recv()),
                    delay_for(self.inactive_period),
                )
                .await
                {
                    // Timed out! Let's send what we have got.
                    future::Either::Right((_, _)) => {
                        break;
                    }
                    // Received something:
                    future::Either::Left((maybe_guard, _)) => {
                        let guard = maybe_guard.expect("queue error");
                        let deserialized =
                            bincode::deserialize(&guard).expect("error deserializing update");

                        if let Some(update) = deserialized {
                            update_buffer.push(update);
                            guard.commit();
                            log::debug!("received event for {}", tree_name);
                        } else {
                            // Never consume the `None` guard.
                            guard.rollback().expect("queue error");
                            is_done = true;
                        }

                        if is_done || update_buffer.len() >= 128 {
                            break;
                        }
                    }
                }
            }

            log::trace!("got batch for {}", self.tree_name);

            let system = system_iter.next().expect("infinite iterator");
            let [insert, remove] = update_buffer.into_batches();

            'insert: loop {
                let mut backoff = UPDATE_EXPONENTIAL_BACKOFF.clone();

                while backoff.tick().await {
                    if let Err(err) = system.send(spec_number, &insert).await {
                        log::warn!("could not update `{}` (retrying): {}", self.tree_name, err);
                    } else {
                        log::trace!("sent event");
                        break 'insert;
                    };
                }

                log::warn!("too many retries to send update. Will trigger reconnect.");
                system.trigger_refresh().await;
            }

            // 'Oops! Duplicated code.
            'remove: loop {
                let mut backoff = UPDATE_EXPONENTIAL_BACKOFF.clone();

                while backoff.tick().await {
                    if let Err(err) = system.send(spec_number, &remove).await {
                        log::warn!("could not update `{}` (retrying): {}", self.tree_name, err);
                    } else {
                        log::trace!("sent event");
                        break 'remove;
                    };
                }

                log::warn!("too many retries to send update. Will trigger reconnect.");
                system.trigger_refresh().await;
            }
        }

        log::info!("finished sending updates for {}", self.tree_name);
    }
}

pub struct ReplicationSenderPool {
    parameters: String,
    n_connections: usize,
    senders: Vec<ReplicationSender>,
}

impl ReplicationSenderPool {
    pub fn new(
        parameters: String,
        n_connections: usize,
        senders: Vec<ReplicationSender>,
    ) -> ReplicationSenderPool {
        ReplicationSenderPool {
            parameters,
            n_connections,
            senders,
        }
    }

    pub async fn prepare(self) -> Result<impl Future<Output = ()>, crate::Error> {
        log::info!("connecting");

        // Ensuring schema:
        log::info!("ensuring schema is initiated");
        let client = connect(&self.parameters).await;
        future::try_join_all(self.senders.iter().map(|puller| {
            // log::info!("{}", puller.replicate_spec.create_commands);
            client.batch_execute(&puller.replicate_spec.create_commands)
        }))
        .await?;

        log::info!("preparing statements");

        // init systems:
        let all_specs = self
            .senders
            .iter()
            .map(|puller| Arc::clone(&puller.replicate_spec))
            .collect::<Vec<_>>();
        let systems = Arc::new(
            future::try_join_all(
                (0..self.n_connections)
                    .map(|_| SqlSystem::init(self.parameters.clone(), all_specs.clone())),
            )
            .await?,
        );

        Ok(async move {
            log::info!("starting to send updates");

            // run updates:
            future::join_all(
                self.senders
                    .into_iter()
                    .enumerate()
                    .map(|(spec_number, puller)| puller.send_updates(spec_number, systems.clone())),
            )
            .await;

            log::info!("done sending updates");
        })
    }
}
