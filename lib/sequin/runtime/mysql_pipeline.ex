defmodule Sequin.Runtime.MysqlPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Sinks.Mysql.Client
  alias Sequin.Transforms.Message

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def batchers_config(consumer) do
    concurrency = min(System.schedulers_online() * 2, 20)

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
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: message} ->
        consumer
        |> Message.to_external(message)
        |> ensure_string_keys()
      end)

    case Client.upsert_records(sink, records) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Upserted records to MySQL table \"#{sink.table_name}\""
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to upsert records to MySQL table \"#{sink.table_name}\"",
          error: error
        })

        {:error, error}
    end
  end

  @impl SinkPipeline
  def handle_batch(:delete, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    record_pks =
      Enum.flat_map(messages, fn %{data: message} -> message.record_pks end)

    case Client.delete_records(sink, record_pks) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Deleted records from MySQL table \"#{sink.table_name}\"",
          extra: %{record_pks: record_pks}
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete records from MySQL table \"#{sink.table_name}\"",
          error: error,
          extra: %{record_pks: record_pks}
        })

        {:error, error}
    end
  end

  # Helper functions

  # Ensure all keys in the record are strings for MySQL column compatibility
  defp ensure_string_keys(record) when is_map(record) do
    Map.new(record, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp ensure_string_keys(record), do: record

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
