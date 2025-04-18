defmodule Sequin.Runtime.TypesensePipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Error
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Typesense.Client

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    typesense_client = Client.new(TypesenseSink.client_params(consumer.sink))
    Map.put(context, :typesense_client, typesense_client)
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    concurrency = min(System.schedulers_online() * 2, 50)

    [
      default: [
        concurrency: concurrency,
        batch_size: 40,
        batch_timeout: 1000
      ]
    ]
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      typesense_client: client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    external_messages =
      Enum.map(messages, fn %{data: data} ->
        Sequin.Transforms.Message.to_external(consumer, data)
      end)

    case length(external_messages) do
      1 ->
        # Use single document API for a single message
        [message] = external_messages

        case Client.index_document(client, sink.collection_name, message) do
          {:ok, _response} ->
            {:ok, messages, context}

          {:error, error} ->
            {:error, error}
        end

      _ ->
        # Use batch import API with JSONL for multiple messages
        jsonl = encode_as_jsonl(external_messages)

        case Client.import_documents(client, sink.collection_name, jsonl, action: sink.import_action) do
          {:ok, response} ->
            case find_errors(response) do
              [] ->
                {:ok, messages, context}

              errors ->
                {:error, Error.service(service: :typesense, message: "Failed to import documents", details: errors)}
            end

          {:error, error} ->
            {:error, error}
        end
    end
  end

  defp encode_as_jsonl(messages) do
    Enum.map_join(messages, "\n", &Jason.encode!/1)
  end

  defp find_errors(response) do
    Enum.filter(response, fn item -> Map.get(item, "success") == false end)
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    # Req.Test.allow(Sequin.Typesense.Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
