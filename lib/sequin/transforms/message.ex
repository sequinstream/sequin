defmodule Sequin.Transforms.Message do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.Trace

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

  def to_external(%SinkConsumer{transform: %Function{id: nil}}, _), do: raise("Transform function lacks id")

  def to_external(%SinkConsumer{id: consumer_id, transform: %Function{function: %TransformFunction{}} = function}, %c{
        data: data
      })
      when c in [ConsumerEvent, ConsumerRecord] do
    result = MiniElixir.run_compiled(function, data)

    Trace.info(consumer_id, %Trace.Event{
      message: "Executed transform function #{function.name}",
      extra: %{
        input: data,
        output: result
      }
    })

    result
  end

  def to_external(%SinkConsumer{transform: %Function{function: %PathFunction{} = function}}, %c{data: data})
      when c in [ConsumerEvent, ConsumerRecord] do
    PathFunction.apply(function, data)
  end
end
