defmodule Sequin.Runtime.TypesensePipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Typesense.Client
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, opts) do
    %{consumer: consumer} = context

    req_opts = Keyword.get(opts, :req_opts, [])
    client_params = TypesenseSink.client_params(consumer.sink)
    client_opts = Keyword.put(client_params, :req_opts, req_opts)

    Map.put(context, :typesense_client, Client.new(client_opts))
  end

  @impl SinkPipeline
  def batchers_config(consumer) do
    concurrency = min(System.schedulers_online() * 2, 50)

    [
      default: [
        concurrency: concurrency,
        batch_size: consumer.sink.batch_size,
        batch_timeout: 10
      ],
      delete: [
        concurrency: concurrency,
        batch_size: 1,
        batch_timeout: 10
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %Routing.Consumers.Typesense{action: action, collection_name: collection_name} =
      Routing.route_message(context.consumer, message.data)

    batcher =
      case action do
        :index -> :default
        :delete -> :delete
      end

    message =
      message
      |> Broadway.Message.put_batcher(batcher)
      |> Broadway.Message.put_batch_key(collection_name)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{
      consumer: consumer = %SinkConsumer{sink: _sink},
      typesense_client: client,
      test_pid: test_pid
    } = context

    collection_name = batch_info.batch_key

    setup_allowances(test_pid)

    external_messages =
      Enum.map(messages, fn %{data: data} ->
        Message.to_external(consumer, data)
      end)

    case external_messages do
      [] ->
        {:ok, messages, context}

      [message] ->
        case Client.index_document(consumer, client, collection_name, message) do
          {:ok, _response} ->
            {:ok, messages, context}

          {:error, error} ->
            {:error, error}
        end

      _ ->
        jsonl = encode_as_jsonl(external_messages)

        case Client.import_documents(consumer, client, collection_name, jsonl) do
          {:ok, results} ->
            messages =
              Enum.zip_with(messages, results, fn message, result ->
                case result do
                  :ok -> message
                  {:error, error_message} -> Broadway.Message.failed(message, error_message)
                end
              end)

            {:ok, messages, context}

          {:error, error} ->
            {:error, error}
        end
    end
  end

  @impl SinkPipeline
  def handle_batch(:delete, messages, batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: _sink} = consumer,
      typesense_client: client,
      test_pid: test_pid
    } = context

    collection_name = batch_info.batch_key

    setup_allowances(test_pid)

    external_messages =
      Enum.map(messages, fn %{data: data} ->
        Message.to_external(consumer, data)
      end)

    case external_messages do
      [external_message] ->
        document_id = get_stringified_id(external_message)

        if document_id do
          case Client.delete_document(consumer, client, collection_name, document_id) do
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

  defp get_stringified_id(message) do
    case Map.get(message, "id") || Map.get(message, :id) do
      nil -> nil
      id when is_binary(id) -> id
      id -> to_string(id)
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    # Req.Test.allow(Sequin.Typesense.Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
