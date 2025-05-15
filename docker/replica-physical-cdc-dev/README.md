# PostgreSQL Physical Streaming Replication Setup

This directory contains a Docker Compose setup for PostgreSQL physical streaming replication with a primary and a hot standby replica instance.

## Configuration

- Primary PostgreSQL runs on port `7432`
- Replica PostgreSQL runs on port `7452`
- Admin credentials for both instances (for direct connections):
  - Username: `postgres`
  - Password: `postgres`
  - Database: `postgres`
- Replication user credentials (used for replication stream):
  - Username: `postgres`
  - Password: `postgres`

## Connection Strings

### Primary Database
`postgresql://postgres:postgres@localhost:7432/postgres`

### Replica Database (Read-Only)
`postgresql://postgres:postgres@localhost:7452/postgres`

## Table Structure (Example)

The `test_table` created on the primary will be replicated to the replica:
```sql
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

## Replication Configuration

- Type: Physical Streaming Replication
- Primary WAL level: `replica`
- Replica mode: `hot_standby` (allows read queries)
- Replication slot name: `replica_physical_slot` (on primary)

## Setup

1.  Ensure `setup-replica.sh` is executable:
    ```bash
    chmod +x setup-replica.sh
    ```
2.  Start the containers:
    ```bash
    docker-compose up -d --build # Use --build if you change scripts
    ```
3.  Wait for both containers to be healthy. The replica might take a bit longer to initialize from base backup.
    Check with `docker-compose ps` and `docker-compose logs -f postgres-replica`.

## Testing the Replication

### 1. Insert Data into Primary

Connect to the primary database and insert some test data:
```bash
# Connect to primary
docker exec -it postgres-primary psql -U postgres

# Once in psql, insert some test data
INSERT INTO test_table (name) VALUES ('physical_test1');
INSERT INTO test_table (name) VALUES ('physical_test2');
COMMIT; -- Ensure data is flushed and sent
```

### 2. Verify Data on Replica

Connect to the replica database (it's read-only) and check if the data was replicated:
```bash
# Connect to replica
docker exec -it postgres-replica psql -U postgres

# Once in psql, verify the data
SELECT * FROM test_table;
```
You should see the data inserted on the primary.

### 3. Monitor Replication Status

**On the Primary:**
Check connected standbys and replication slot status:
```bash
docker exec -it postgres-primary psql -U postgres -c "SELECT * FROM pg_stat_replication;"
docker exec -it postgres-primary psql -U postgres -c "SELECT slot_name, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'replica_physical_slot';"
```

**On the Replica:**
Check if it's in recovery mode and WAL replay status:
```bash
docker exec -it postgres-replica psql -U postgres -c "SELECT pg_is_in_recovery();"
# Expected output: t (true)

docker exec -it postgres-replica psql -U postgres -c "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_last_xact_replay_timestamp();"
```
The LSNs should advance, and `pg_last_xact_replay_timestamp` should update after transactions on the primary.

## Cleanup

To stop and remove the containers, networks, and volumes:
```bash
docker-compose down -v
```

## Troubleshooting

1.  **Replica Fails to Start or Connect:**
    *   Check `docker-compose logs postgres-replica`. Look for errors from `pg_basebackup` or connection issues to the primary.
    *   Ensure `postgres-primary` is healthy first (`docker-compose ps`).
    *   Verify `pg_hba.conf` on the primary allows the `postgres` user from the replica's IP (using `all` as in the script is a broad allow).
    *   Check `docker-compose logs postgres-primary` for connection attempt logs.

2.  **Data Not Replicating:**
    *   Verify replication status on primary (`pg_stat_replication`). Is the replica connected?
    *   Check the replication slot status on primary (`pg_replication_slots`). Is it active?
    *   Check replica logs for errors related to WAL replay.

3.  **`pg_basebackup` fails:**
    *   Ensure the `postgres-replica-data` volume is empty if re-running setup. Use `docker-compose down -v` to clear volumes.
    *   Verify `postgres` user exists on primary with correct password and `REPLICATION` privilege.
    *   Verify the replication slot `replica_physical_slot` exists on primary.
