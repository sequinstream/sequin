defmodule Sequin.Repo.Migrations.AddCommitIdxToAndRemoveSeqFromConsumerEventsAndRecords do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_events, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute "UPDATE #{@stream_schema}.consumer_events SET commit_idx = seq - commit_lsn;", ""

    alter table(:consumer_events, prefix: @stream_schema) do
      modify :commit_idx, :integer, null: false
      remove :seq
    end

    alter table(:consumer_records, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute "UPDATE #{@stream_schema}.consumer_records SET commit_idx = seq - commit_lsn;", ""

    alter table(:consumer_records, prefix: @stream_schema) do
      modify :commit_idx, :integer, null: false
      remove :seq
    end

    alter table(:wal_events, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute "UPDATE #{@stream_schema}.wal_events SET commit_idx = commit_lsn - commit_idx;", ""

    alter table(:wal_events, prefix: @stream_schema) do
      modify :commit_idx, :integer, null: false
      remove :seq
    end

    create(
      unique_index(:wal_events, [:wal_pipeline_id, :commit_lsn, :commit_idx],
        prefix: @stream_schema
      )
    )
  end
end
