defmodule Sequin.Transforms.Message do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.FunctionTransform
  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Transforms.MiniElixir

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

  def to_external(
        %SinkConsumer{transform: %Transform{id: id, transform: %FunctionTransform{}} = transform},
        %ConsumerEvent{data: data}
      ) do
    # Presence or abscence of ID is intended to indicate whether we are evaluating for test messages
    if id do
      MiniElixir.run_compiled(transform, data)
    else
      MiniElixir.run_interpreted(transform, data)
    end
  end

  def to_external(
        %SinkConsumer{transform: %Transform{id: id, transform: %FunctionTransform{}} = transform},
        %ConsumerRecord{data: data}
      ) do
    # Presence or abscence of ID is intended to indicate whether we are evaluating for test messages
    if id do
      MiniElixir.run_compiled(transform, data)
    else
      MiniElixir.run_interpreted(transform, data)
    end
  end

  def to_external(%SinkConsumer{transform: %Transform{transform: %PathTransform{path: path}}}, %ConsumerEvent{} = event) do
    keys = String.split(path || "", ".")
    traverse_path(event.data, keys)
  end

  def to_external(%SinkConsumer{transform: %Transform{transform: %PathTransform{path: path}}}, %ConsumerRecord{} = record) do
    keys = String.split(path || "", ".")
    traverse_path(record.data, keys)
  end

  # Carve out known structs that we can traverse
  defp traverse_path(%ConsumerEventData{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerEventData.Metadata{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData.Metadata{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerEventData.Metadata.Sink{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData.Metadata.Sink{} = data, keys), do: mapify_struct_and_traverse(data, keys)

  # Base case
  defp traverse_path(value, []), do: value

  # Traverse a map
  defp traverse_path(data, [key | rest]) when is_map(data) do
    case Map.get(data, key) do
      nil -> nil
      value when is_struct(value) -> traverse_path(value, rest)
      value when is_map(value) -> traverse_path(value, rest)
      value -> traverse_path(value, rest)
    end
  end

  # Traverse a list - we don't support this
  defp traverse_path(data, _keys) when is_list(data), do: nil

  # Traverse a struct
  defp mapify_struct_and_traverse(struct, keys) do
    struct
    |> Map.from_struct()
    |> Sequin.Map.stringify_keys()
    |> traverse_path(keys)
  end
end
