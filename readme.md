Replicate your Sled database to Postgres.

This is a sample usage example:
```rust
use sled_to_postgres::{Replication, ReplicateTree};
use tokio_postgres::types::ToSql;

// Open yout database:
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
        // This is the command for one insertion. The replication needs to call
        // this repeatedly for the same data. 
        insert_statement: "
            insert into a_table values ($1::int, $2::int)
            on conflict (x) do update set x = excluded.x;
        ",
        // This is how you transform a `(key, value)` into the parameters for
        // the above statement.
        insert_parameters: |key: &[u8], value: &[u8]| vec![
            Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
            Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>
        ],
        // This is the command for one removal. The replication needs to call
        // this repeatedly for the same data. 
        remove_statement: "delete from a_table where x = $1::int",
        // This is how you transform a `key` into the parameters for the above
        // statement.
        remove_parameters: |key: &[u8]| vec![
            Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>
        ],
    });

tokio::spawn(async move {
    // You may insert stuff prior to starting the replication. This will be
    // also sent to Postgres.
    tree.insert(&987i32.to_be_bytes(), &654i32.to_be_bytes()).unwrap();
    
    // Start the replication.
    let (handle, shutdown) = replication.start().await.unwrap();

    // Now, insert something in `a_tree`.
    tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes()).unwrap();
    
    // When you are done, trigger shutdown:
    // It is understood that _there will be no more db operations after this
    // point._
    shutdown.trigger();

    // Shutdown doesn't happen immediately. It takes at least 500ms.
    // You need not to await this, but it is recommended.
    handle.await.unwrap();
});
```
