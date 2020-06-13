Replicate your Sled database to Postgres.

This is a sample usage example:
```rust
use sled_to_postgres::{Replication, ReplicateTree};
use tokio_postgres::types::ToSql;

let db = sled::open("data/db").unwrap();
let tree = db.open_tree("a_tree").unwrap();

fn decode(i: &[u8]) -> i32 {
    i32::from_be_bytes([i[0], i[1], i[2], i[3]])
}

let setup = Replication::new(
        "host=localhost dbname=a_db user=someone password=idk",
        "data/replication",
    ).push(ReplicateTree {
        tree: tree.clone(),
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
        insert_parameters: |key: &[u8], value: &[u8]| vec![
            Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
            Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>
        ],
        remove_parameters: |key: &[u8]| vec![
            Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>
        ],
    }).setup();

tokio::spawn(async move {
    let (replication, shutdown) = setup.await.unwrap();
    let handle = tokio::spawn(replication);
    tree.insert(&123i32.to_be_bytes(), &456i32.to_be_bytes()).unwrap();
    
    // when you are done, trigger shutdown:
    shutdown.trigger();
    handle.await.unwrap();
});
```
