defmodule Sequin.Runtime.ElasticsearchPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Elasticsearch.Client
  alias Sequin.Transforms

  require Logger

  @impl SinkPipeline
  def init(context, opts) do
    req_opts = Keyword.get(opts, :req_opts, [])
    Map.put(context, :req_opts, req_opts)
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{sink: %ElasticsearchSink{batch_size: batch_size}}) do
    concurrency = min(System.schedulers_online() * 2, 80)

    [
      default: [
        concurrency: concurrency,
        batch_size: batch_size,
        batch_timeout: :timer.seconds(1)
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.Elasticsearch{index_name: index_name} =
      Routing.route_message(consumer, message.data)

    message = Broadway.Message.put_batch_key(message, index_name)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{consumer: consumer, test_pid: test_pid, req_opts: req_opts} = context
    index_name = batch_info.batch_key
    sink = consumer.sink

    setup_allowances(test_pid)

    # Create proper bulk API format with action line + data line for each document
    # Map each consumer event action to appropriate Elasticsearch bulk operation
    # Each row, including the last row, must end with a newline
    ndjson =
      Enum.map(messages, fn %Broadway.Message{data: consumer_message} ->
        id = Enum.join(consumer_message.record_pks, "-")

        case consumer_message.data.action do
          :delete ->
            [Jason.encode!(%{delete: %{_index: index_name, _id: id}}), "\n"]

          action when action in [:insert, :update, :read] ->
            action = Jason.encode!(%{index: %{_index: index_name, _id: id}})
            data = Jason.encode!(Transforms.Message.to_external(consumer, consumer_message))
            [action, "\n", data, "\n"]
        end
      end)

    case Client.import_documents(sink, index_name, ndjson, req_opts) do
      {:ok, results} ->
        messages =
          messages
          |> Enum.zip(results)
          |> Enum.map(fn {message, result} ->
            case result do
              %{status: :ok} ->
                message

              %{status: :error, error_message: error_message} ->
                Broadway.Message.failed(message, error_message)
            end
          end)

        {:ok, messages, context}

      {:error, error} ->
        reason =
          Error.service(
            service: :elasticsearch,
            code: "index_error",
            message: "Elasticsearch bulk index failed",
            details: %{error: error}
          )

        {:error, reason}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    # Mox.allow(Sequin.Sinks.ElasticsearchMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
