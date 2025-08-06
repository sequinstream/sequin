defmodule Sequin.Runtime.MysqlPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Sinks.Mysql
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
    %{consumer: consumer} = context

    %Routing.Consumers.Mysql{table_name: table_name} = Routing.route_message(consumer, message.data)

    batcher =
      case message.data.data.action do
        :delete -> :delete
        _ -> :default
      end

    message = Broadway.Message.put_batch_key(message, table_name)
    {:ok, Broadway.Message.put_batcher(message, batcher), context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    table_name = Map.get(batch_info, :batch_key, sink.table_name)

    records =
      Enum.map(messages, fn %{data: message} ->
        consumer
        |> Message.to_external(message)
        |> ensure_string_keys()
      end)

    routed_sink = %{sink | table_name: table_name}

    case Mysql.upsert_records(routed_sink, records) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Upserted records to MySQL table \"#{table_name}\""
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to upsert records to MySQL table \"#{table_name}\"",
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

    setup_allowances(test_pid)

    table_name = Map.get(batch_info, :batch_key, sink.table_name)

    record_pks =
      Enum.flat_map(messages, fn %{data: message} -> message.record_pks end)

    routed_sink = %{sink | table_name: table_name}

    case Mysql.delete_records(routed_sink, record_pks) do
      {:ok} ->
        Trace.info(consumer.id, %Trace.Event{
          message: "Deleted records from MySQL table \"#{table_name}\"",
          extra: %{record_pks: record_pks}
        })

        {:ok, messages, context}

      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to delete records from MySQL table \"#{table_name}\"",
          error: error,
          extra: %{record_pks: record_pks}
        })

        {:error, error}
    end
  end

  defp ensure_string_keys(record) when is_map(record) do
    Map.new(record, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp ensure_string_keys(record), do: record

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.MysqlMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end
