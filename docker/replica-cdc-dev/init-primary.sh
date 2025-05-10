#!/bin/bash
set -e

# Create replication user and set up replication
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create replication user with proper permissions
    CREATE USER replicator WITH REPLICATION LOGIN ENCRYPTED PASSWORD 'replicator_password' SUPERUSER;
    
    -- Create test table
    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- Grant permissions to replicator user
    GRANT USAGE ON SCHEMA public TO replicator;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replicator;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO replicator;

    -- Drop existing publication if it exists
    DROP PUBLICATION IF EXISTS sequin_pub;

    -- Create publication with specific settings
    CREATE PUBLICATION sequin_pub FOR TABLE test_table;
    
    -- Drop existing replication slot if it exists
    SELECT pg_drop_replication_slot('sequin_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sequin_slot');
    
    -- Create replication slot
    SELECT pg_create_logical_replication_slot('sequin_slot', 'pgoutput');
EOSQL

# Configure pg_hba.conf to allow replication connections
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
echo "host all replicator all md5" >> "$PGDATA/pg_hba.conf" 