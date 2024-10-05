defmodule Sequin.Repo.Migrations.CreateWalEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    create table(:wal_events,
             prefix: @stream_schema,
             primary_key: false
           ) do
      add :id, :serial, null: false, primary_key: true
      add :source_replication_slot_id, :uuid, null: false, primary_key: true
      add :source_table_oid, :integer, null: false, primary_key: true
      add :commit_lsn, :bigint, null: false
      add :record_pks, {:array, :text}, null: false
      add :data, :jsonb, null: false
      add :replication_message_trace_id, :uuid, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:wal_events, [:source_replication_slot_id, :source_table_oid, :commit_lsn],
             prefix: @stream_schema
           )

    create index(:wal_events, [:source_replication_slot_id, :record_pks, :source_table_oid],
             prefix: @stream_schema
           )

    create index(:wal_events, [:source_replication_slot_id], prefix: @stream_schema)
  end
end
