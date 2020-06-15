use futures::prelude::*;
use futures::{future, stream};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{delay_for, Duration};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Statement};

use super::{ReplicateSpec, ReplicationUpdate, BATCH_SIZE};

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
        let insert_statements = future::try_join_all(
            replicate_specs
                .iter()
                .map(|spec| client.prepare(&spec.insert_statement)),
        )
        .await?;
        let remove_statements = future::try_join_all(
            replicate_specs
                .iter()
                .map(|spec| client.prepare(&spec.remove_statement)),
        )
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

    async fn send(
        &self,
        spec_number: usize,
        update: &ReplicationUpdate,
    ) -> Result<(), crate::Error> {
        let guard = self.inner.read().await;
        let spec = &guard.replicate_specs[spec_number];
        let insert = &guard.insert_statements[spec_number];
        let remove = &guard.remove_statements[spec_number];

        match update {
            ReplicationUpdate::Insert { key, value } => {
                let parameters = (spec.insert_parameters)(&key, &value);

                // Need to do like this to please borrow checker:
                let mut parameter_refs = Vec::with_capacity(parameters.len());
                for param in &parameters {
                    parameter_refs.push(param.as_ref() as &(dyn ToSql + Sync));
                }

                let x = guard.client.query(insert, &*parameter_refs);

                x.await?;
            }
            ReplicationUpdate::Remove { key } => {
                let parameters = (spec.remove_parameters)(&key);

                // Need to do like this to please borrow checker:
                let mut parameter_refs = Vec::with_capacity(parameters.len());
                for param in &parameters {
                    parameter_refs.push(param.as_ref() as &(dyn ToSql + Sync));
                }

                guard.client.query(remove, &*parameter_refs).await?;
            }
        }

        Ok(())
    }
}

pub struct ReplicationPuller {
    queue_receiver: yaque::Receiver,
    replicate_spec: Arc<ReplicateSpec>,
}

impl ReplicationPuller {
    pub(crate) fn new<P: AsRef<Path>>(
        replication_dir: P,
        replicate_spec: Arc<ReplicateSpec>,
    ) -> Result<ReplicationPuller, crate::Error> {
        let queue_receiver = yaque::Receiver::open(replication_dir)?;

        Ok(ReplicationPuller {
            queue_receiver,
            replicate_spec,
        })
    }

    async fn send_events(mut self, spec_number: usize, systems: Arc<Vec<SqlSystem>>) {
        log::info!("puller started to send events");
        let mut is_done = false;

        // Now, upload stuff while they come in...
        while !is_done {
            // Yes, temporary (you can optimize this one later):
            let mut update_buffer = Vec::with_capacity(BATCH_SIZE);

            // Now, control until when you want to receive stuff:
            let guard = self
                .queue_receiver
                .recv_while(|serialized| {
                    if update_buffer.len() >= BATCH_SIZE || is_done {
                        return false;
                    }

                    let deserialized =
                        bincode::deserialize(&serialized).expect("error deserializing update");

                    if let Some(update) = deserialized {
                        update_buffer.push(update);
                        log::debug!("received event");
                        true
                    } else {
                        // Never consume the `None` guard.
                        is_done = true;
                        false
                    }
                })
                .await
                .expect("queue error");

            log::trace!("got batch");

            // DANGER! TODO This is wrong. You know why!
            //
            // This executes queries out of order, so if there if an insertion
            // and its corresponding deletion happen to be in the same batch,
            // we could have a race condition in which the line is deleted (a
            // no-op) before insertion, ending up in a line that should not be
            // there.
            //
            // Alternative is to stop being lazy and to run this code first for
            // all insertions in the batch and _then_ for all deletions in the
            // batch.
            stream::iter(update_buffer.into_iter().zip(systems.iter().cycle()))
                .for_each_concurrent(None, |(update, system)| async move {
                    'outer: loop {
                        let mut backoff = UPDATE_EXPONENTIAL_BACKOFF.clone();

                        while backoff.tick().await {
                            if let Err(err) = system.send(spec_number, &update).await {
                                log::warn!("could not update (retrying): {}", err);
                            } else {
                                log::trace!("sent event");
                                break 'outer;
                            };
                        }

                        log::warn!("too many retries to send update. Will trigger reconnect.");
                        system.trigger_refresh().await;
                    }
                })
                .await;

            log::trace!("committing");
            guard.commit();
        }

        log::info!("finished sending events");
    }
}

pub struct ReplicationSender {
    parameters: String,
    n_connections: usize,
    pullers: Vec<ReplicationPuller>,
}

impl ReplicationSender {
    pub fn new(
        parameters: String,
        n_connections: usize,
        pullers: Vec<ReplicationPuller>,
    ) -> ReplicationSender {
        ReplicationSender {
            parameters,
            n_connections,
            pullers,
        }
    }

    pub async fn prepare(self) -> Result<impl Future<Output = ()>, crate::Error> {
        log::info!("connecting");

        // Ensuring schema:
        log::info!("ensuring schema is initiated");
        let client = connect(&self.parameters).await;
        future::try_join_all(
            self.pullers
                .iter()
                .map(|puller| client.batch_execute(&puller.replicate_spec.create_commands)),
        )
        .await?;

        log::info!("preparing statements");

        // init systems:
        let all_specs = self
            .pullers
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
            log::info!("starting to send events");

            // run events:
            future::join_all(
                self.pullers
                    .into_iter()
                    .enumerate()
                    .map(|(spec_number, puller)| puller.send_events(spec_number, systems.clone())),
            )
            .await;

            log::info!("done sending events");
        })
    }
}
