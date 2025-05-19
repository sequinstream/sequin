defmodule Sequin.Repo.Migrations.AddIndexToConsumerEventsForStreaming do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    create index(:consumer_events, [:consumer_id, :commit_lsn, :commit_idx],
             prefix: @stream_schema
           )

    create index(:consumer_records, [:consumer_id, :commit_lsn, :commit_idx],
             prefix: @stream_schema
           )

    drop index(:consumer_events, [:consumer_id], prefix: @stream_schema)
  end
end
