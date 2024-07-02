defmodule Sequin.Repo.Migrations.CreateStreamTables do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "select 1;"

    create table(:accounts) do
      timestamps()
    end

    create table(:streams) do
      add :account_id, references(:accounts, on_delete: :delete_all), null: false
      add :slug, :text, null: false

      timestamps()
    end

    create unique_index(:streams, [:slug])
    # Required for composite foreign keys pointing to this table
    create unique_index(:streams, [:id, :account_id])

    execute "create schema if not exists streams", "drop schema if exists streams"

    execute "create sequence streams.messages_seq",
            "drop sequence if exists streams.messages_seq"

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
             prefix: "streams",
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

      add :seq, :bigint, null: false, default: fragment("nextval('streams.messages_seq')")

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

    create unique_index(:messages, [:stream_id, :seq], prefix: "streams")
    create unique_index(:messages, [:stream_id, :subject], prefix: "streams")

    create index(:messages, [:stream_id, :token1], prefix: "streams")
    create index(:messages, [:stream_id, :token2], prefix: "streams", where: "token2 IS NOT NULL")
    create index(:messages, [:stream_id, :token3], prefix: "streams", where: "token3 IS NOT NULL")
    create index(:messages, [:stream_id, :token4], prefix: "streams", where: "token4 IS NOT NULL")
    create index(:messages, [:stream_id, :token5], prefix: "streams", where: "token5 IS NOT NULL")
    create index(:messages, [:stream_id, :token6], prefix: "streams", where: "token6 IS NOT NULL")
    create index(:messages, [:stream_id, :token7], prefix: "streams", where: "token7 IS NOT NULL")
    create index(:messages, [:stream_id, :token8], prefix: "streams", where: "token8 IS NOT NULL")
    create index(:messages, [:stream_id, :token9], prefix: "streams", where: "token9 IS NOT NULL")

    execute """
            CREATE OR REPLACE FUNCTION streams.validate_message_subject(subject text)
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
            drop function if exists streams.validate_message_subject;
            """

    execute "alter table streams.messages add constraint validate_message_subject check (streams.validate_message_subject(subject));",
            "alter table streams.messages drop constraint validate_message_subject;"

    create index(:messages, [:stream_id, :token10],
             prefix: "streams",
             where: "token10 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token11],
             prefix: "streams",
             where: "token11 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token12],
             prefix: "streams",
             where: "token12 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token13],
             prefix: "streams",
             where: "token13 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token14],
             prefix: "streams",
             where: "token14 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token15],
             prefix: "streams",
             where: "token15 IS NOT NULL"
           )

    create index(:messages, [:stream_id, :token16],
             prefix: "streams",
             where: "token16 IS NOT NULL"
           )

    ## TODO: Add indexes to subject space

    execute "create type streams.consumer_message_state as enum ('delivered', 'available', 'pending_redelivery');",
            "drop type if exists streams.consumer_message_state"

    create table(:consumers) do
      # Using a composite foreign key for stream_id
      add :account_id, :uuid, null: false

      add :stream_id,
          references(:streams,
            on_delete: :delete_all,
            with: [account_id: :account_id],
            match: :full
          ),
          null: false

      add :slug, :text, null: false
      add :filter_subject, :text, null: false

      add :ack_wait_ms, :integer, null: false, default: 30_000
      add :max_ack_pending, :integer, null: false, default: 10_000
      add :max_deliver, :integer, null: true
      add :max_waiting, :integer, null: false, default: 100

      timestamps()
    end

    create index(:consumers, [:stream_id])
    create unique_index(:consumers, [:stream_id, :slug])

    create table(:consumer_messages,
             prefix: "streams",
             primary_key: false,
             options: "PARTITION BY LIST (consumer_id)"
           ) do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :message_subject, :text, null: false, primary_key: true
      add :message_seq, :bigint, null: false

      add :ack_id, :uuid, null: false, default: fragment("uuid_generate_v4()")

      add :state, :"streams.consumer_message_state", null: false
      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:consumer_messages, [:consumer_id, :message_subject], prefix: "streams")
    create unique_index(:consumer_messages, [:consumer_id, :ack_id], prefix: "streams")

    create index(:consumer_messages, [:message_subject], prefix: "streams")
    create index(:consumer_messages, [:consumer_id], prefix: "streams")

    create index(
             :consumer_messages,
             [
               :consumer_id,
               :state,
               :not_visible_until,
               :last_delivered_at
             ],
             prefix: "streams"
           )
  end
end
