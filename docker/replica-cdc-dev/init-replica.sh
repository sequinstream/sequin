#!/bin/bash
set -e

# Wait for primary to be ready
until PGPASSWORD=postgres psql -h postgres-primary -U postgres -c '\q'; do
  echo "Waiting for primary to be ready..."
  sleep 1
done

# Create the subscription
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Drop table if it already exists (for idempotency)
    DROP TABLE IF EXISTS test_table;

    -- Create the table with the same structure as the primary
    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );    

    -- Drop existing subscription if it exists
    DROP SUBSCRIPTION IF EXISTS sequin_sub;

    -- Create subscription using the new publication name
    CREATE SUBSCRIPTION sequin_sub
    CONNECTION 'host=postgres-primary port=5432 user=replicator password=replicator_password dbname=postgres'
    PUBLICATION sequin_pub
    WITH (copy_data = true);
EOSQL

# Wait for initial data copy to complete
echo "Waiting for initial data copy to complete..."
until PGPASSWORD=postgres psql -h postgres-primary -U postgres -c "SELECT count(*) FROM test_table" | grep -q "[0-9]"; do
  echo "Waiting for data to be copied..."
  sleep 2
done 