defmodule Sequin.Repo.Migrations.AlterConsumerEventsAndRecordsSetTraceIdNotNull do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def up do
    alter table(:consumer_events, prefix: @stream_schema) do
      modify :replication_message_trace_id, :uuid, null: false
    end

    alter table(:consumer_records, prefix: @stream_schema) do
      modify :replication_message_trace_id, :uuid, null: false
    end
  end

  def down do
    alter table(:consumer_events, prefix: @stream_schema) do
      modify :replication_message_trace_id, :uuid, null: true
    end

    alter table(:consumer_records, prefix: @stream_schema) do
      modify :replication_message_trace_id, :uuid, null: true
    end
  end
end
