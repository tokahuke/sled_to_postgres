# Replicate your Sled database to Postgres

<a href="https://docs.rs/sled_to_postgres"><img src="https://docs.rs/sled_to_postgres/badge.svg"></a>
<a href="https://crates.io/crates/sled_to_postgres"><img src="https://img.shields.io/crates/v/sled_to_postgres.svg"></a>

**WIP: although this is part of a bigger project, it has not yet been used in production. My use case allows for _some_ small inconsistency. If yours doesn't, _caveat emptor._**

## Introduction

This crate provides replication from Sled (a Key-Value database) to Postgres (a RDBMS). It uses `tokio_postgres` to connect to Postgres and the `Subscriber` API in Sled to watch for updates. Since Sled is agnostic to the type in the database and Postgres is strongly typed, you must provide the serialization/deserialization functions explicitly whe setting the replication up.

Again, this is still a _work in progress_. Do _not_ use this crate (yet) if you need high reliability. 

## Quickstart 

This is a sample usage example for a single tree. For more trees, just chain more `Replication::push` calls. First of all, let's set up the replication itself:

```rust
// (`ToSql` is reexported from `tokio_postgres`)
use sled_to_postgres::{Replication, ReplicateTree, ToSql};

// Open your database:
let db = sled::open("data/db").unwrap();
// Open your tree.
let tree = db.open_tree("a_tree").unwrap();

/// Makeshift decode.
fn decode(i: &[u8]) -> i32 {
    i32::from_be_bytes([i[0], i[1], i[2], i[3]])
}

// This is how you set up a replication:
let setup = Replication::new(
        // Put in your credentials.
        "host=localhost dbname=a_db user=someone password=idk",
        // You will need a location in the disk for temporary data.
        "data/replication",
    ).push(ReplicateTree {
        // This is a replication on the tree `a_tree`.
        tree: tree.clone(),
        // You may specify a replication only over a given prefix.
        prefix: vec![],
        // This are the commands for table (and index) creation.
        // This needs to be *idempotent* (create _if not exists_).
        create_commands: "
            create table if not exists a_table (
                x int primary key,
                y int not null
            );
        ",
        // This is the command for one insertion. The replication might need
        // to call this repeatedly for the same data. 
        insert_statement: "
            insert into a_table values ($1::int, $2::int)
            on conflict (x) do update set x = excluded.x;
        ",
        // This is how you transform a `(key, value)` into the parameters for
        // the above statement.
        // ... this is the general and complicated form. You can simplify 
        // stuff using `params!`.
        insert_parameters: |key: &[u8], value: &[u8]| {
            vec![
                Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
                Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>,
            ]
        },
        // This is the command for one removal. The replication needs to call
        // this repeatedly for the same data. 
        remove_statement: "delete from a_table where x = $1::int",
        // This is how you transform a `key` into the parameters for the above
        // statement.
        // ... using `params!` makes it more ergonomic.
        remove_parameters: |key: &[u8]| params![decode(&*key)],
    });
```

Now, after we have configured the replication, let's put it to run. Since `tokio_postgres` is based on `tokio`, we will use the `tokio` runtime.

```rust
tokio::spawn(async move {
    // Do not insert anything before starting the replication. These events will not be logged.
    // tree.insert(&987i32.to_be_bytes(), &654i32.to_be_bytes()).unwrap();
    
    // Although the current state of the database *will* be dumped with the 
    // replication when it starts for the first time.

    // Start the replication.
    let stopper = replication.start().await.unwrap();

    // Now, insert something in `a_tree`.
    tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes()).unwrap();
    
    // When you are done, trigger shutdown:
    // It is understood that _there will be no more db operations after this
    // point._
    // Shutdown doesn't happen immediately. It takes at least 500ms.
    stopper.stop().await;
});
```

## Limitations and _caveats_

There are some limitations on the current implementation:
1. Do not use foreign key constraints on the replicated tables. Since Sled
doesn't have such a concept and updates are inserted in small batches 
concurrently, the table might not obey this constraint during brief moments.
2. Be careful to start the replications before any updates are done to the
database, preferably giving it a head-start of a couple of milliseconds.
3. Be careful to only trigger the end of the replication when you are
absolutely sure no more updates are going to be made from that point on.

## License

This code is licensed under the Apache 2.0 license. See the attached `license` file for further details.