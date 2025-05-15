#!/bin/bash
set -e

PG_HBA_CONF="$PGDATA/pg_hba.conf"
AUTH_METHOD="md5"

echo "INFO: Modifying $PG_HBA_CONF in init-primary.sh"

HBA_POSTGRES_USER_LINE="host all \"$POSTGRES_USER\" all $AUTH_METHOD"
if ! grep -Fxq "$HBA_POSTGRES_USER_LINE" "$PG_HBA_CONF"; then
    echo "$HBA_POSTGRES_USER_LINE" >> "$PG_HBA_CONF"
    echo "INFO: Added to $PG_HBA_CONF: $HBA_POSTGRES_USER_LINE"
else
    echo "INFO: $PG_HBA_CONF already contains: $HBA_POSTGRES_USER_LINE"
fi

# Ensure 'postgres' user can connect for replication from any IP
HBA_REPLICATOR_LINE="host replication postgres all $AUTH_METHOD"
if ! grep -Fxq "$HBA_REPLICATOR_LINE" "$PG_HBA_CONF"; then
    echo "$HBA_REPLICATOR_LINE" >> "$PG_HBA_CONF"
    echo "INFO: Added to $PG_HBA_CONF: $HBA_REPLICATOR_LINE"
else
    echo "INFO: $PG_HBA_CONF already contains: $HBA_REPLICATOR_LINE"
fi


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'postgres') THEN
        CREATE USER postgres WITH REPLICATION LOGIN ENCRYPTED PASSWORD 'postgres';
    ELSE
        -- Ensure password and REPLICATION attribute are set if user exists
        ALTER USER postgres WITH REPLICATION LOGIN ENCRYPTED PASSWORD 'postgres';
    END IF;
END
\$\$;
 CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
 -- Create physical replication slot if it doesn't exist
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica_physical_slot' AND slot_type = 'physical') THEN
        PERFORM pg_create_physical_replication_slot('replica_physical_slot');
    END IF;
END
\$\$;
EOSQL

echo "INFO: init-primary.sh finished."
