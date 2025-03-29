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
    traverse_path(event.data, keys)
  end

  def to_external(%SinkConsumer{transform: %Transform{transform: %PathTransform{path: path}}}, %ConsumerRecord{} = record) do
    keys = String.split(path, ".")
    traverse_path(record.data, keys)
  end

  defp traverse_path(data, [key | rest]) when is_struct(data) do
    case struct_field(data, key) do
      nil -> nil
      value when is_struct(value) -> traverse_path(value, rest)
      value when is_map(value) -> traverse_path(value, rest)
      value when rest == [] -> value
      _ -> nil
    end
  end

  defp traverse_path(data, [key | rest]) when is_map(data) do
    case Map.get(data, key) do
      nil -> nil
      value when is_struct(value) -> traverse_path(value, rest)
      value when is_map(value) -> traverse_path(value, rest)
      value when rest == [] -> value
      _ -> nil
    end
  end

  defp traverse_path(value, []) do
    value
  end

  defp struct_field(struct, field) when is_struct(struct) do
    Map.get(struct, String.to_existing_atom(field))
  rescue
    _ -> nil
  end
end
