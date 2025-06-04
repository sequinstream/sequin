# Setup Postgres Replication

To prepare your Postgres database for replication with Sequin, follow these steps:

1. Create a replication slot:

   ```sql
   SELECT pg_create_logical_replication_slot('your_slot_name', 'pgoutput');
   ```

2. Create a publication for the tables you want to replicate:

   ```sql
   CREATE PUBLICATION your_publication_name FOR TABLE schema.table1, schema.table2 WITH (publish_via_partition_root = true);
   ```

   You can add more tables by separating them with commas. To publish all tables in a schema:

   ```sql
   CREATE PUBLICATION your_publication_name FOR TABLES IN SCHEMA your_schema WITH (publish_via_partition_root = true);
   ```

3. Set the replica identity for each table. There are two main options:

   a. Default (only primary key is replicated for updates/deletes):

   ```sql
   ALTER TABLE your_schema.your_table REPLICA IDENTITY DEFAULT;
   ```

   b. Full (entire old row is replicated for updates/deletes):

   ```sql
   ALTER TABLE your_schema.your_table REPLICA IDENTITY FULL;
   ```

   The "full" option provides more detailed change information but may increase replication
   overhead.

Remember to adjust your Postgres configuration to allow replication connections. Also, ensure that the `wal_level` parameter in your PostgreSQL configuration is set to 'logical'. This setting enables logical decoding, which is necessary for logical replication.
