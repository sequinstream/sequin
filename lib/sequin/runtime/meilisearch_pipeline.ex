defmodule Sequin.Runtime.MeilisearchPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Sinks.Meilisearch.Client
  alias Sequin.Transforms.Message

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def batchers_config(consumer) do
    concurrency = min(System.schedulers_online() * 2, 50)

    [
      default: [
        concurrency: concurrency,
        batch_size: consumer.sink.batch_size,
        batch_timeout: 1000
      ],
      delete: [
        concurrency: concurrency,
        batch_size: consumer.sink.batch_size,
        batch_timeout: 1000
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    batcher =
      case message.data.data.action do
        :delete -> :delete
        _ -> :default
      end

    %Routing.Consumers.Meilisearch{index_name: index_name} =
      Routing.route_message(context.consumer, message.data)

    message =
      message
      |> Broadway.Message.put_batcher(batcher)
      |> Broadway.Message.put_batch_key(index_name)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink, transform: transform} = consumer,
      test_pid: test_pid
    } = context

    index_name = batch_info.batch_key

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: message} ->
        consumer
        |> Message.to_external(message)
        |> attempt_to_fill_primary_key(transform, sink.primary_key)
      end)

    case Client.import_documents(sink, index_name, records) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Imported documents to \"#{index_name}\" index"
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to import to \"#{index_name}\" index",
          error: error
        })

        {:error, error}
    end
  end

  @impl SinkPipeline
  def handle_batch(:delete, messages, batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      test_pid: test_pid
    } = context

    index_name = batch_info.batch_key

    setup_allowances(test_pid)

    document_ids =
      Enum.flat_map(messages, fn %{data: message} -> message.record_pks end)

    case Client.delete_documents(sink, index_name, document_ids) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Deleted documents from \"#{index_name}\" index",
          extra: %{document_ids: document_ids}
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete documents from \"#{index_name}\" index",
          error: error,
          extra: %{document_ids: document_ids}
        })

        {:error, error}
    end
  end

  # Helper functions

  # If the consumer does not have a transform, attempt to fill in missing primary key
  # by extracting it from record, but only for simple (non-dotted) primary keys
  defp attempt_to_fill_primary_key(message, nil, primary_key) do
    if String.contains?(primary_key, ".") do
      # If primary key contains a dot, it's a nested primary key - leave message untouched
      message
    else
      # For simple primary keys, attempt to extract from record if not already present
      case get_in(message, [:record, primary_key]) do
        nil -> message
        value -> Map.put(message, primary_key, value)
      end
    end
  end

  defp attempt_to_fill_primary_key(message, _transform, _primary_key), do: message

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
