defmodule Sequin.Transforms.Message do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer

  def to_external(%SinkConsumer{legacy_transform: :none}, %ConsumerEvent{} = event) do
    %{
      record: event.data.record,
      changes: event.data.changes,
      action: to_string(event.data.action),
      metadata: %{
        database_name: event.data.metadata.database_name,
        table_schema: event.data.metadata.table_schema,
        table_name: event.data.metadata.table_name,
        consumer: event.data.metadata.consumer,
        commit_timestamp: event.data.metadata.commit_timestamp,
        commit_lsn: event.data.metadata.commit_lsn,
        transaction_annotations: event.data.metadata.transaction_annotations
      }
    }
  end

  def to_external(%SinkConsumer{legacy_transform: :record_only}, %ConsumerEvent{} = event) do
    event.data.record
  end

  def to_external(%SinkConsumer{legacy_transform: :none}, %ConsumerRecord{} = record) do
    %{
      record: record.data.record,
      metadata: %{
        database_name: record.data.metadata.database_name,
        table_schema: record.data.metadata.table_schema,
        table_name: record.data.metadata.table_name,
        consumer: record.data.metadata.consumer,
        commit_lsn: record.data.metadata.commit_lsn
      }
    }
  end

  def to_external(%SinkConsumer{legacy_transform: :record_only}, %ConsumerRecord{} = record) do
    record.data.record
  end
end
