defmodule Sequin.Runtime.PostgresPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.PostgresSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Postgres
  alias Sequin.Transforms.Message

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    [default: [concurrency: 10, batch_size: 1, batch_timeout: 1]]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %PostgresSink{} = sink} = consumer} = context

    records =
      Enum.map(messages, fn %{data: data} ->
        Message.to_external(consumer, data)
      end)

    case Postgres.insert_records(sink, records) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end
end
