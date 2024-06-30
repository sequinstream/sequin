defmodule Sequin.Repo.Migrations.CreateStreamTables do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "select 1;"

    create table(:accounts) do
      timestamps()
    end

    execute "create sequence if not exists stream_idx_seq start with 1 increment by 1",
            "drop sequence if exists stream_idx_seq"

    create table(:streams) do
      add :idx, :bigint, default: fragment("nextval('stream_idx_seq')"), null: false
      add :account_id, references(:accounts, on_delete: :delete_all), null: false

      timestamps()
    end

    create unique_index(:streams, [:idx])
    # Required for composite foreign keys pointing to this table
    create unique_index(:streams, [:id, :account_id])

    execute "create schema if not exists streams", "drop schema if exists streams"

    create table(:messages,
             primary_key: false,
             prefix: "streams",
             options: "PARTITION BY LIST (stream_id)"
           ) do
      add :key, :text, null: false, primary_key: true
      add :stream_id, :uuid, null: false, primary_key: true
      add :data_hash, :text, null: false
      add :data, :text, null: false
      add :seq, :bigint, null: true

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:messages, [:stream_id, :seq], prefix: "streams")

    execute "create sequence streams.messages_seq owned by streams.messages.seq;",
            "drop sequence if exists streams.messages_seq"

    create index(:messages, [:stream_id, :updated_at],
             prefix: "streams",
             where: "seq IS NULL"
           )

    # The following trigger+func ensures that `seq` is set to null on updates, if it's not already.
    # This is to prevent mistakes in situations where `messages` is updated directly, bypassing `AssignMessageSeqWorker`.

    execute """
            create or replace function streams.reset_seq_on_update()
            returns trigger as $$
            begin
                new.seq := null;
                return new;
            end;
            $$ language plpgsql;
            """,
            "drop function if exists streams.reset_seq_on_update()"

    execute """
            create trigger trg_reset_seq_on_update
            before update on streams.messages
            for each row
            when (old.seq is not null and new.seq is not null)
            execute function streams.reset_seq_on_update();
            """,
            "drop trigger if exists trg_reset_seq_on_update on streams.messages"

    execute "create type streams.outstanding_message_state as enum ('delivered', 'available', 'pending_redelivery');",
            "drop type if exists streams.outstanding_message_state"

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

      add :ack_wait_ms, :integer, null: false, default: 30_000
      add :max_ack_pending, :integer, null: false, default: 10_000
      add :max_deliver, :integer, null: true
      add :max_waiting, :integer, null: false, default: 100

      timestamps()
    end

    create index(:consumers, [:stream_id])

    create table(:consumer_states, prefix: "streams", primary_key: false) do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :message_seq_cursor, :bigint, null: false, default: 0
      add :count_pulled_into_outstanding, :bigint, null: false, default: 0

      timestamps()
    end

    create table(:consumer_pending_messages_count, prefix: "streams") do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :count, :integer, null: false
    end

    create unique_index(:consumer_pending_messages_count, [:consumer_id], prefix: "streams")

    create table(:outstanding_messages, prefix: "streams") do
      add :consumer_id, :uuid, null: false
      add :message_seq, :bigint, null: false
      add :message_key, :text, null: false
      add :message_stream_id, :uuid, null: false
      add :state, :"streams.outstanding_message_state", null: false
      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:outstanding_messages, [:consumer_id, :message_key], prefix: "streams")
    create index(:outstanding_messages, [:message_key], prefix: "streams")
    create index(:outstanding_messages, [:consumer_id], prefix: "streams")

    create index(
             :outstanding_messages,
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
