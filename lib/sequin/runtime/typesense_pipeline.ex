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
        batch_size: 1,
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

    message =
      Broadway.Message.put_batcher(message, batcher)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{
      consumer: consumer = %SinkConsumer{sink: sink},
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
        {:ok, messages, context}

      [message] ->
        case Client.index_document(client, sink.collection_name, message) do
          {:ok, _response} ->
            {:ok, messages, context}

          {:error, error} ->
            {:error, error}
        end

      _ ->
        jsonl = encode_as_jsonl(external_messages)

        case Client.import_documents(client, sink.collection_name, jsonl) do
          {:error, error} ->
            {:error, error}

          {:ok, results} ->
            messages =
              Enum.zip_with(messages, results, fn message, result ->
                case result do
                  :ok -> message
                  {:error, error_message} -> Broadway.Message.failed(message, error_message)
                end
              end)

            {:ok, messages, context}
        end
    end
  end

  @impl SinkPipeline
  def handle_batch(:delete, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink},
      typesense_client: client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    case messages do
      [%{data: dd}] ->
        if document_id = dd.data.record["id"] do
          case Client.delete_document(client, sink.collection_name, document_id) do
            {:ok, _response} ->
              {:ok, messages, context}

            {:error, error} ->
              {:error, error}
          end
        else
          {:error, "Missing document ID for deletion"}
        end

      [] ->
        {:ok, messages, context}

      _ ->
        {:error, :misconfigured_batcher}
    end
  end

  # Helper functions

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
