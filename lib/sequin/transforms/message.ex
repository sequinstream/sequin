defmodule Sequin.Transforms.Message do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform

  def to_external(%SinkConsumer{transform: nil, legacy_transform: :none}, %ConsumerEvent{} = event) do
    %{
      record: event.data.record,
      changes: event.data.changes,
      action: to_string(event.data.action),
      metadata: Map.from_struct(event.data.metadata)
    }
  end

  def to_external(%SinkConsumer{transform: nil, legacy_transform: :record_only}, %ConsumerEvent{} = event) do
    event.data.record
  end

  def to_external(%SinkConsumer{transform: nil, legacy_transform: :none}, %ConsumerRecord{} = record) do
    %{
      record: record.data.record,
      metadata: Map.from_struct(record.data.metadata)
    }
  end

  def to_external(%SinkConsumer{transform: nil, legacy_transform: :record_only}, %ConsumerRecord{} = record) do
    record.data.record
  end

  def to_external(%SinkConsumer{transform: %Transform{transform: %PathTransform{path: path}}}, %ConsumerEvent{} = event) do
    keys = String.split(path, ".")

    event.data
    |> Sequin.Map.from_struct_deep()
    |> Sequin.Map.deep_stringify_keys()
    |> get_in(keys)
  end

  def to_external(%SinkConsumer{transform: %Transform{transform: %PathTransform{path: path}}}, %ConsumerRecord{} = record) do
    keys = String.split(path, ".")

    record.data
    |> Sequin.Map.from_struct_deep()
    |> Sequin.Map.deep_stringify_keys()
    |> get_in(keys)
  end
end
