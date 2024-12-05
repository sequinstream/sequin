defmodule Sequin.Repo.Migrations.AddSeqToConsumerStreamTables do
  use Ecto.Migration
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_events, prefix: @stream_schema) do
      add :seq, :bigint
    end

    create unique_index(:consumer_events, [:consumer_id, :seq], prefix: @stream_schema)

    alter table(:consumer_records, prefix: @stream_schema) do
      add :seq, :bigint
    end

    create unique_index(:consumer_records, [:consumer_id, :seq], prefix: @stream_schema)

    alter table(:wal_events, prefix: @stream_schema) do
      add :seq, :bigint
    end

    create unique_index(:wal_events, [:wal_pipeline_id, :seq], prefix: @stream_schema)
  end
end
