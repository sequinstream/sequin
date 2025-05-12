# PostgreSQL Logical Replication Setup

This directory contains a Docker Compose setup for PostgreSQL logical replication with a primary and replica instance.

## Configuration

- Primary PostgreSQL runs on port `7432`
- Replica PostgreSQL runs on port `7452`
- Both instances use the default credentials:
  - Username: `postgres`
  - Password: `postgres`
  - Database: `postgres`
- Replication user credentials:
  - Username: `replicator`
  - Password: `replicator_password`

## Connection Strings

### Primary Database
```
postgresql://postgres:postgres@localhost:7432/postgres
```

### Replica Database
```
postgresql://postgres:postgres@localhost:7452/postgres
```

## Table Structure

The example uses a `test_table` with the following structure:
```sql
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

## Replication Configuration

- Publication name: `sequin_pub`
- Subscription name: `sequin_sub`
- Replication slot name: `sequin_slot`

## Setup

1. Start the containers:
```bash
docker-compose up -d
```

2. Wait for both containers to be healthy (you can check with `docker-compose ps`)

## Testing the Replication

### 1. Insert Data into Primary

Connect to the primary database and insert some test data:

```bash
# Connect to primary
docker exec -it postgres-primary psql -U postgres

# Once in psql, insert some test data
INSERT INTO test_table (name) VALUES ('test1');
INSERT INTO test_table (name) VALUES ('test2');
INSERT INTO test_table (name) VALUES ('test3');
```

### 2. Verify Data on Replica

Connect to the replica database and check if the data was replicated:

```bash
# Connect to replica
docker exec -it postgres-replica psql -U postgres

# Once in psql, verify the data
SELECT * FROM test_table;
```

### 3. Monitor Replication Status

To check the replication status on the primary:

```bash
docker exec -it postgres-primary psql -U postgres -c "SELECT * FROM pg_publication;"
docker exec -it postgres-primary psql -U postgres -c "SELECT * FROM pg_replication_slots;"
```

To check the subscription status on the replica:

```bash
docker exec -it postgres-replica psql -U postgres -c "SELECT * FROM pg_subscription;"
```

## Cleanup

To stop and remove the containers and volumes:

```bash
docker-compose down -v
```

## Troubleshooting

If replication is not working:

1. Check if both containers are running:
```bash
docker-compose ps
```

2. Check the logs:
```bash
docker-compose logs postgres-primary
docker-compose logs postgres-replica
```

3. Verify the replication user and permissions:
```bash
docker exec -it postgres-primary psql -U postgres -c "\du"
```

4. Check if the publication and subscription are active:
```bash
# On primary
docker exec -it postgres-primary psql -U postgres -c "SELECT * FROM pg_publication;"

# On replica
docker exec -it postgres-replica psql -U postgres -c "SELECT * FROM pg_subscription;"
``` 