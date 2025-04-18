defmodule Sequin.Runtime.TypesensePipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.TypesenseSink
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
        consumer
        |> Sequin.Transforms.Message.to_external(data)
        |> ensure_id_string()
      end)

    case external_messages do
      [] ->
        :noop

      [message] ->
        case Client.index_document(client, sink.collection_name, message, action: sink.import_action) do
          {:ok, _response} ->
            {:ok, messages, context}

          {:error, error} ->
            {:error, error}
        end

      _ ->
        jsonl = encode_as_jsonl(external_messages)

        case Client.import_documents(client, sink.collection_name, jsonl, action: sink.import_action) do
          {:error, _} = e -> e
          {:ok, _} -> {:ok, messages, context}
        end
    end
  end

  defp encode_as_jsonl(messages) do
    Enum.map_join(messages, "\n", &Jason.encode!/1)
  end

  defp ensure_id_string(%{"id" => id} = m) when is_binary(id), do: m
  defp ensure_id_string(%{"id" => id} = m), do: %{m | "id" => to_string(id)}
  defp ensure_id_string(m), do: m

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    # Req.Test.allow(Sequin.Typesense.Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
