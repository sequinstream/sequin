defmodule Sequin.Repo.Migrations.CreateInitial do
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

    create table(:postgres_databases, prefix: @config_schema) do
      add :database, :string, null: false
      add :hostname, :string, null: false
      add :password, :binary, null: false
      add :pool_size, :integer, default: 10, null: false
      add :port, :integer, null: false
      add :queue_interval, :integer, default: 50, null: false
      add :queue_target, :integer, default: 100, null: false
      add :name, :string, null: false
      add :ssl, :boolean, default: false, null: false
      add :username, :string, null: false

      add :account_id, references(:accounts, prefix: @config_schema), null: false

      timestamps()
    end

    # This is for the FKs from postgres_replication to this table
    create unique_index(:postgres_databases, [:id, :account_id], prefix: @config_schema)

    create unique_index(:postgres_databases, [:account_id, :name], prefix: @config_schema)

    create table(:postgres_replication_slots, prefix: @config_schema) do
      add :publication_name, :string, null: false
      add :slot_name, :string, null: false

      add :account_id, references(:accounts, type: :uuid, prefix: @config_schema), null: false

      add :postgres_database_id,
          references(:postgres_databases, with: [account_id: :account_id], prefix: @config_schema),
          null: false

      timestamps()
    end

    create unique_index(:postgres_replication_slots, [:slot_name, :postgres_database_id],
             prefix: @config_schema
           )

    create index(:postgres_replication_slots, [:account_id], prefix: @config_schema)
    create index(:postgres_replication_slots, [:postgres_database_id], prefix: @config_schema)

    # This is for the FKs from stream_tables to this table
    create unique_index(:postgres_replication_slots, [:id, :account_id], prefix: @config_schema)

    create table(:http_endpoints, prefix: @config_schema) do
      add :name, :string, null: true
      add :base_url, :string, null: false
      add :headers, :map, default: %{}

      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      timestamps()
    end

    # Required for composite foreign keys pointing to this table
    create unique_index(:http_endpoints, [:id, :account_id], prefix: @config_schema)

    execute "CREATE TYPE #{@config_schema}.consumer_status AS ENUM ('active', 'disabled');"

    create table(:http_push_consumers, prefix: @config_schema) do
      # Using a composite foreign key for stream_id
      add :account_id, :uuid, null: false

      add :http_endpoint_id,
          references(:http_endpoints, with: [account_id: :account_id], prefix: @config_schema),
          null: false

      add :name, :text, null: false
      add :backfill_completed_at, :utc_datetime_usec

      add :ack_wait_ms, :integer, null: false, default: 30_000
      add :max_ack_pending, :integer, null: false, default: 10_000
      add :max_deliver, :integer, null: true
      add :max_waiting, :integer, null: false, default: 100
      # TODO: Will remove in favor of separate models
      add :message_kind, :text, null: false, default: "record"
      add :status, :"#{@config_schema}.consumer_status", null: false, default: "active"

      timestamps()
    end

    create table(:http_pull_consumers, prefix: @config_schema) do
      # Using a composite foreign key for stream_id
      add :account_id, :uuid, null: false

      add :name, :text, null: false
      add :backfill_completed_at, :utc_datetime_usec

      add :ack_wait_ms, :integer, null: false, default: 30_000
      add :max_ack_pending, :integer, null: false, default: 10_000
      add :max_deliver, :integer, null: true
      add :max_waiting, :integer, null: false, default: 100
      # TODO: Will remove in favor of separate models
      add :message_kind, :text, null: false, default: "record"
      add :status, :"#{@config_schema}.consumer_status", null: false, default: "active"

      timestamps()
    end

    execute "create type #{@stream_schema}.consumer_record_state as enum ('acked', 'available', 'delivered', 'pending_redelivery');",
            "drop type if exists #{@stream_schema}.consumer_record_state"

    create table(:consumer_messages,
             prefix: @stream_schema,
             primary_key: false,
             options: "PARTITION BY LIST (consumer_id)"
           ) do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :message_key, :text, null: false, primary_key: true
      add :message_seq, :bigint, null: false

      add :ack_id, :uuid, null: false, default: fragment("uuid_generate_v4()")

      add :state, :"#{@stream_schema}.consumer_record_state", null: false
      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:consumer_messages, [:consumer_id, :ack_id], prefix: @stream_schema)

    create index(:consumer_messages, [:message_key], prefix: @stream_schema)
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

    create table(:consumer_events,
             prefix: @stream_schema,
             primary_key: false,
             options: "PARTITION BY LIST (consumer_id)"
           ) do
      add :consumer_id, :uuid, null: false, primary_key: true
      add :id, :serial, null: false, primary_key: true
      add :commit_lsn, :bigint, null: false
      add :record_pks, :jsonb, null: false
      add :table_oid, :integer, null: false

      add :data, :jsonb, null: false

      add :ack_id, :uuid, null: false, default: fragment("uuid_generate_v4()")

      add :not_visible_until, :utc_datetime_usec
      add :deliver_count, :integer, null: false, default: 0
      add :last_delivered_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create index(:consumer_events, [:consumer_id, :record_pks, :table_oid],
             prefix: @stream_schema
           )

    create unique_index(:consumer_events, [:consumer_id, :ack_id], prefix: @stream_schema)

    create index(:consumer_events, [:consumer_id], prefix: @stream_schema)

    create index(
             :consumer_events,
             [
               :consumer_id,
               :not_visible_until,
               :last_delivered_at
             ],
             prefix: @stream_schema
           )

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
