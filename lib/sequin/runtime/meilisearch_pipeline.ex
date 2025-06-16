defmodule Sequin.Runtime.MeilisearchPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Meilisearch.Client

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    meilisearch_client = Client.new(MeilisearchSink.client_params(consumer.sink))
    Map.put(context, :meilisearch_client, meilisearch_client)
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

    {:ok, Broadway.Message.put_batcher(message, batcher), context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      meilisearch_client: client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    records = Enum.map(messages, fn %{data: data} -> data.data.record end)
    jsonl = encode_as_jsonl(records)

    case Client.import_documents(consumer, client, sink.index_name, jsonl) do
      {:ok} -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  @impl SinkPipeline
  def handle_batch(:delete, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      meilisearch_client: client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    document_ids =
      messages
      |> Enum.map(fn %{data: data} -> data.data.record end)
      |> Enum.map(&ensure_id_string/1)
      |> Enum.map(& &1["id"])
      |> Enum.filter(&is_binary/1)

    case Client.delete_documents(consumer, client, sink.index_name, document_ids) do
      {:ok, _} -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  # Helper functions

  defp encode_as_jsonl(records) do
    Enum.map_join(records, "\n", &Jason.encode!/1)
  end

  defp ensure_id_string(%{"id" => id} = m) when is_binary(id), do: m
  defp ensure_id_string(%{"id" => id} = m), do: %{m | "id" => to_string(id)}
  defp ensure_id_string(m), do: m

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
