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
      add :seq, :bigint, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:messages, [:stream_id, :seq], prefix: "streams")

    execute "create sequence streams.messages_seq owned by streams.messages.seq;",
            "drop sequence if exists streams.messages_seq"

    execute "create type streams.message_state as enum ('delivered', 'available', 'pending_redelivery');",
            "drop type if exists streams.message_state"

    create table(:consumers) do
      add :stream_id, references(:streams, on_delete: :delete_all), null: false
      add :account_id, references(:accounts, on_delete: :delete_all), null: false

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

      timestamps()
    end

    create table(:outstanding_messages, prefix: "streams") do
      add :consumer_id, :text, null: false
      add :message_seq, :bigint, null: false
      add :message_key, :text, null: false
      add :message_stream_id, :uuid, null: false
      add :state, :"streams.message_state", null: false
      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    execute """
            alter table streams.outstanding_messages
            add constraint fk_outstanding_messages_messages
            foreign key (message_key, message_stream_id)
            references streams.messages(key, stream_id)
            """,
            "alter table streams.outstanding_messages drop constraint if exists fk_outstanding_messages_messages"

    create unique_index(:outstanding_messages, [:consumer_id, :message_key], prefix: "streams")
    create index(:outstanding_messages, [:message_key], prefix: "streams")

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

    create table(:outstanding_messages_count, prefix: "streams") do
      add :consumer_id, :text, null: false
      add :count, :integer, null: false
    end

    create unique_index(:outstanding_messages_count, [:consumer_id], prefix: "streams")
  end
end
