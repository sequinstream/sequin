defmodule Sequin.Repo.Migrations.AddTraceIdToConsumerEventsAndRecords do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_events, prefix: @stream_schema) do
      add :replication_message_trace_id, :uuid
    end

    alter table(:consumer_records, prefix: @stream_schema) do
      add :replication_message_trace_id, :uuid
    end
  end
end
