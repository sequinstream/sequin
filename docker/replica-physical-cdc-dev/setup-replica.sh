#!/bin/bash
set -e

# Only run setup if PGDATA is empty (first time for this volume)
if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
  echo "Replica data directory ($PGDATA) is empty. Initializing standby from primary..."

  # Wait for primary to be ready for connections
  # Use the main postgres user/pass for this check against the primary.
  until PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres-primary -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
    echo "Waiting for primary (postgres-primary) to be ready..."
    sleep 2
  done
  echo "Primary is ready."

  echo "Performing pg_basebackup..."
  # Use the 'postgres' user and its password for pg_basebackup.
  # -D $PGDATA : target directory
  # -h postgres-primary : primary host
  # -U postgres : replication user
  # -Fp : format plain (not tar)
  # -Xs : stream WAL content while backup is taken
  # -P  : show progress
  # -R  : create recovery configuration (standby.signal and adds to postgresql.auto.conf)
  # --slot=replica_physical_slot : use the slot created on the primary
  PGPASSWORD='postgres' pg_basebackup \
    -h postgres-primary \
    -U postgres \
    -D "$PGDATA" \
    -Fp \
    -Xs \
    -P \
    -R \
    --slot='replica_physical_slot'

  echo "pg_basebackup completed."

  # pg_basebackup with -R should correctly set permissions for $PGDATA.
  # If needed, ensure postgresql.auto.conf contains hot_standby = on,
  # but it's also passed via 'command:' in docker-compose.yaml.
  # echo "hot_standby = on" >> "$PGDATA/postgresql.auto.conf"

else
  echo "Replica data directory ($PGDATA) is not empty. Assuming already configured or restored."
fi

# Execute the original command passed to this script (e.g., "postgres -c hot_standby=on")
# This will invoke the original docker-entrypoint.sh from the postgres image,
# which will then start the PostgreSQL server.
echo "Executing command: $@"
exec /usr/local/bin/docker-entrypoint.sh "$@"
