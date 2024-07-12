defmodule Sequin.Repo.Migrations.CreateStreamTables do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "select 1;"

    execute "create schema if not exists #{@stream_schema}",
            "drop schema if exists #{@stream_schema}"

    execute "create schema if not exists #{@config_schema}",
            "drop schema if exists #{@config_schema}"

    create table(:accounts, prefix: @config_schema) do
      timestamps()
    end

    create table(:streams, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :slug, :text, null: false

      timestamps()
    end

    # We will need to alter this unique index to account for the database in which the stream lives
    # This should also probably include account_id
    create unique_index(:streams, [:account_id, :slug], prefix: @config_schema)
    # Required for composite foreign keys pointing to this table
    create unique_index(:streams, [:id, :account_id], prefix: @config_schema)

    execute "create sequence #{@stream_schema}.messages_seq",
            "drop sequence if exists #{@stream_schema}.messages_seq"

    execute """
            CREATE OR REPLACE FUNCTION subject_from_tokens(VARIADIC text[])
            RETURNS text AS $$
            DECLARE
            result text := '';
            i integer;
            BEGIN
            FOR i IN 1..array_length($1, 1) LOOP
            IF $1[i] IS NOT NULL AND $1[i] != '' THEN
                IF result != '' THEN
                    result := result || '.';
                END IF;
                result := result || $1[i];
            END IF;
            END LOOP;
            RETURN result;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
            """,
            """
            drop function if exists subject_from_tokens;
            """

    create table(:messages,
             primary_key: false,
             prefix: @stream_schema,
             options: "PARTITION BY LIST (stream_id)"
           ) do
      add :stream_id, :uuid, null: false, primary_key: true

      # Generated column which concats tokens
      add :subject, :text,
        null: false,
        primary_key: true,
        generated: """
        ALWAYS AS (
          subject_from_tokens(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10, token11, token12, token13, token14, token15, token16)
        ) STORED
        """

      add :seq, :bigint,
        null: false,
        default: fragment("nextval('#{@stream_schema}.messages_seq')")

      add :data, :text, null: false
      add :data_hash, :text, null: false

      add :token1, :text, null: false
      add :token2, :text
      add :token3, :text
      add :token4, :text
      add :token5, :text
      add :token6, :text
      add :token7, :text
      add :token8, :text
      add :token9, :text
      add :token10, :text
      add :token11, :text
      add :token12, :text
      add :token13, :text
      add :token14, :text
      add :token15, :text
      add :token16, :text

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:messages, [:stream_id, :seq], prefix: @stream_schema)
    create unique_index(:messages, [:stream_id, :subject], prefix: @stream_schema)

    create index(:messages, [:stream_id, :token1], prefix: @stream_schema)

    create index(:messages, [:stream_id, :token2],
             prefix: @stream_schema,
             where: "token2 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token3],
             prefix: @stream_schema,
             where: "token3 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token4],
             prefix: @stream_schema,
             where: "token4 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token5],
             prefix: @stream_schema,
             where: "token5 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token6],
             prefix: @stream_schema,
             where: "token6 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token7],
             prefix: @stream_schema,
             where: "token7 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token8],
             prefix: @stream_schema,
             where: "token8 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token9],
             prefix: @stream_schema,
             where: "token9 IS NOT NULL"
           )

    execute """
            CREATE OR REPLACE FUNCTION #{@stream_schema}.validate_message_subject(subject text)
            RETURNS boolean
            LANGUAGE plpgsql
             IMMUTABLE
            AS $function$
            DECLARE
                parts TEXT[];
                part TEXT;
            BEGIN
                -- Check if subject is not empty and doesn't start with a period
                IF subject IS NULL OR subject = '' OR subject LIKE '.%' THEN
                    RETURN FALSE;
                END IF;

                -- Split the subject into parts
                parts := string_to_array(subject, '.');

                -- Check if there's at least one part
                IF array_length(parts, 1) = 0 THEN
                    RETURN FALSE;
                END IF;

                -- Check each part
                FOREACH part IN ARRAY parts
                LOOP
                    -- Check if part is empty
                    IF part = '' THEN
                        RETURN FALSE;
                    END IF;

                    -- Check for disallowed characters
                    IF part ~ '[.* >]' THEN
                        RETURN FALSE;
                    END IF;

                    -- Check for non-ASCII characters
                    IF part ~ '[^ -~]' THEN
                        RETURN FALSE;
                    END IF;
                END LOOP;

                -- All checks passed
                RETURN TRUE;
            END;
            $function$
            """,
            """
            drop function if exists #{@stream_schema}.validate_message_subject;
            """

    execute "alter table #{@stream_schema}.messages add constraint validate_message_subject check (#{@stream_schema}.validate_message_subject(subject));",
            "alter table #{@stream_schema}.messages drop constraint validate_message_subject;"

    create index(:messages, [:stream_id, :token10],
             prefix: @stream_schema,
             where: "token10 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token11],
             prefix: @stream_schema,
             where: "token11 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token12],
             prefix: @stream_schema,
             where: "token12 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token13],
             prefix: @stream_schema,
             where: "token13 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token14],
             prefix: @stream_schema,
             where: "token14 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token15],
             prefix: @stream_schema,
             where: "token15 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token16],
             prefix: @stream_schema,
             where: "token16 IS NOT NULL"
           )

    ## TODO: Add indexes to subject space

    execute "create type #{@stream_schema}.consumer_message_state as enum ('acked', 'available', 'delivered', 'pending_redelivery');",
            "drop type if exists #{@stream_schema}.consumer_message_state"

    create table(:consumers, prefix: @config_schema) do
      # Using a composite foreign key for stream_id
      add :account_id, :uuid, null: false

      add :stream_id,
          references(:streams,
            on_delete: :delete_all,
            with: [account_id: :account_id],
            match: :full,
            prefix: @config_schema
          ),
          null: false

      add :slug, :text, null: false
      add :filter_subject_pattern, :text, null: false
      add :backfill_completed_at, :utc_datetime_usec

      add :ack_wait_ms, :integer, null: false, default: 30_000
      add :max_ack_pending, :integer, null: false, default: 10_000
      add :max_deliver, :integer, null: true
      add :max_waiting, :integer, null: false, default: 100

      timestamps()
    end

    create index(:consumers, [:stream_id], prefix: @config_schema)
    create unique_index(:consumers, [:stream_id, :slug], prefix: @config_schema)

    create table(:consumer_messages,
             prefix: @stream_schema,
             primary_key: false,
             options: "PARTITION BY LIST (consumer_id)"
           ) do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :message_subject, :text, null: false, primary_key: true
      add :message_seq, :bigint, null: false

      add :ack_id, :uuid, null: false, default: fragment("uuid_generate_v4()")

      add :state, :"#{@stream_schema}.consumer_message_state", null: false
      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:consumer_messages, [:consumer_id, :message_subject],
             prefix: @stream_schema
           )

    create unique_index(:consumer_messages, [:consumer_id, :ack_id], prefix: @stream_schema)

    create index(:consumer_messages, [:message_subject], prefix: @stream_schema)
    create index(:consumer_messages, [:consumer_id], prefix: @stream_schema)

    create index(
             :consumer_messages,
             [
               :consumer_id,
               :state,
               :not_visible_until,
               :last_delivered_at
             ],
             prefix: @stream_schema
           )

    create table(:postgres_databases, prefix: @config_schema) do
      add :database, :string, null: false
      add :hostname, :string, null: false
      add :password, :binary, null: false
      add :pool_size, :integer, default: 10, null: false
      add :port, :integer, null: false
      add :queue_interval, :integer, default: 50, null: false
      add :queue_target, :integer, default: 100, null: false
      add :slug, :string, null: false
      add :ssl, :boolean, default: false, null: false
      add :username, :string, null: false

      add :account_id, references(:accounts, prefix: @config_schema), null: false

      timestamps()
    end

    # This is for the FKs from postgres_replication to this table
    create unique_index(:postgres_databases, [:id, :account_id], prefix: @config_schema)

    execute "create type #{@config_schema}.replication_status as enum ('active', 'disabled');",
            "drop type if exists #{@config_schema}.replication_status"

    create table(:postgres_replications, prefix: @config_schema) do
      add :publication_name, :string, null: false
      add :slot_name, :string, null: false
      add :status, :"#{@config_schema}.replication_status", null: false

      add :account_id, references(:accounts, type: :uuid, prefix: @config_schema), null: false

      add :postgres_database_id,
          references(:postgres_databases, with: [account_id: :account_id], prefix: @config_schema),
          null: false

      add :stream_id,
          references(:streams, with: [account_id: :account_id], prefix: @config_schema),
          null: false

      timestamps()
    end

    create unique_index(:postgres_replications, [:slot_name, :postgres_database_id],
             prefix: @config_schema
           )

    create index(:postgres_replications, [:account_id], prefix: @config_schema)
    create index(:postgres_replications, [:postgres_database_id], prefix: @config_schema)
    create index(:postgres_replications, [:stream_id], prefix: @config_schema)

    create table(:api_keys, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :value, :binary, null: false
      add :name, :string

      timestamps()
    end

    create index(:api_keys, [:account_id], prefix: @config_schema)
    create unique_index(:api_keys, [:value], prefix: @config_schema)
  end
end
