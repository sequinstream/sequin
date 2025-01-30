defmodule Sequin.Repo.Migrations.AddCommitIdxToAndRemoveSeqFromConsumerEventsAndRecords do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_events, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute """
              update #{@stream_schema}.consumer_events
              set commit_idx = case
                when seq - commit_lsn < 0 then 0
                when seq - commit_lsn > 2147483647 then 0
                else seq - commit_lsn
              end;
            """,
            ""

    alter table(:consumer_events, prefix: @stream_schema) do
      modify :commit_idx, :integer, null: false
      remove :seq
    end

    alter table(:consumer_records, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute """
              update #{@stream_schema}.consumer_records
              set commit_idx = case
                when seq - commit_lsn < 0 then 0
                when seq - commit_lsn > 2147483647 then 0
                else seq - commit_lsn
              end;
            """,
            ""

    alter table(:consumer_records, prefix: @stream_schema) do
      modify :commit_idx, :integer, null: false
      remove :seq
    end

    alter table(:wal_events, prefix: @stream_schema) do
      add :commit_idx, :integer
    end

    execute """
              update #{@stream_schema}.wal_events
              set commit_idx = case
                when commit_lsn - commit_idx < 0 then 0
                when commit_lsn - commit_idx > 2147483647 then 0
                else commit_lsn - commit_idx
              end;
            """,
            ""

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
