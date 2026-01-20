defmodule Sequin.Repo.Migrations.DropConsumerRecordsAndMessageKind do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema], "public")
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema], "public")

  def up do
    # Check if any consumers are using message_kind = 'record' or if consumer_records has messages
    execute """
    DO $$
    DECLARE
      record_consumer_exists boolean;
      consumer_record_messages_exist boolean;
    BEGIN
      -- Check for consumers with message_kind = 'record' (only if column exists)
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '#{@config_schema}'
        AND table_name = 'sink_consumers'
        AND column_name = 'message_kind'
      ) THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM #{@config_schema}.sink_consumers WHERE message_kind = ''record'' LIMIT 1)' INTO record_consumer_exists;

        IF record_consumer_exists THEN
          RAISE EXCEPTION 'Migration blocked: This version of Sequin no longer supports record-type consumers (message_kind = ''record''). Please change all your consumers to use event-type (message_kind = ''event'') before upgrading Sequin. You can do this in the Sequin UI or API by updating each consumer''s message_kind setting.';
        END IF;
      END IF;

      -- Check for any messages in consumer_records table
      IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '#{@stream_schema}'
        AND table_name = 'consumer_records'
      ) THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM #{@stream_schema}.consumer_records LIMIT 1)' INTO consumer_record_messages_exist;

        IF consumer_record_messages_exist THEN
          RAISE EXCEPTION 'Migration blocked: This version of Sequin no longer supports consumer records. You have existing messages in the consumer_records table. Please change all your consumers to use event-type (message_kind = ''event'') and wait for all record messages to be processed before upgrading Sequin.';
        END IF;
      END IF;
    END
    $$;
    """

    # Drop message_kind column from sink_consumers if the table and column exist
    execute """
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '#{@config_schema}'
        AND table_name = 'sink_consumers'
        AND column_name = 'message_kind'
      ) THEN
        ALTER TABLE #{@config_schema}.sink_consumers DROP COLUMN message_kind;
      END IF;
    END
    $$;
    """

    # Drop consumer_records table and all its partitions (CASCADE handles partitions)
    execute "DROP TABLE IF EXISTS #{@stream_schema}.consumer_records CASCADE"

    # Drop the consumer_message_kind enum type if it exists
    execute "DROP TYPE IF EXISTS #{@config_schema}.consumer_message_kind"
  end

  def down do
    # Recreate the consumer_message_kind enum type
    execute """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'consumer_message_kind') THEN
        CREATE TYPE #{@config_schema}.consumer_message_kind AS ENUM ('event', 'record');
      END IF;
    END
    $$;
    """

    # Add message_kind column back to sink_consumers with default 'event'
    execute """
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '#{@config_schema}'
        AND table_name = 'sink_consumers'
      ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '#{@config_schema}'
        AND table_name = 'sink_consumers'
        AND column_name = 'message_kind'
      ) THEN
        ALTER TABLE #{@config_schema}.sink_consumers ADD COLUMN message_kind #{@config_schema}.consumer_message_kind NOT NULL DEFAULT 'event';
      END IF;
    END
    $$;
    """

    # Recreate the consumer_records table as a partitioned table
    execute """
    CREATE TABLE IF NOT EXISTS #{@stream_schema}.consumer_records (
      id uuid NOT NULL,
      consumer_id uuid NOT NULL,
      commit_lsn bigint NOT NULL,
      commit_idx integer NOT NULL DEFAULT 0,
      record_pks text[] NOT NULL,
      group_id text,
      table_oid integer NOT NULL,
      deliver_count integer NOT NULL DEFAULT 0,
      last_delivered_at timestamp(0) without time zone,
      not_visible_until timestamp(0) without time zone,
      data jsonb,
      replication_message_trace_id uuid,
      ack_id uuid NOT NULL,
      state text NOT NULL DEFAULT 'available',
      encoded_data_size_bytes integer,
      payload_size_bytes integer,
      ingested_at timestamp(0) without time zone,
      commit_timestamp timestamp(0) without time zone,
      inserted_at timestamp(0) without time zone NOT NULL,
      updated_at timestamp(0) without time zone NOT NULL,
      CONSTRAINT consumer_records_pkey PRIMARY KEY (consumer_id, id)
    ) PARTITION BY LIST (consumer_id)
    """
  end
end
