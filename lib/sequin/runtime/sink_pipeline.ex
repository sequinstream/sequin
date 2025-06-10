defmodule Sequin.Runtime.SinkPipeline do
  @moduledoc """
  Generic pipeline implementation that delegates to a specific sink pipeline behaviour.


  Also defines a behaviour for implementing a message pipeline that processes
  and delivers messages to various sinks (Kafka, HTTP, SQS, etc.).
  """

  use Broadway

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Prometheus
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.Trace

  require Logger

  @type context :: map()
  @doc """
  Initializes the pipeline context.

  Called during pipeline startup to prepare the initial context. Because this is invoked during `start_link`, this should not perform any expensive/blocking operations.
  """
  @callback init(context :: context(), opts :: keyword()) :: context()

  @doc """
  Handles an individual message, optionally preparing it for batching.

  Return the message as-is to use default batching, or add batch information to enable batching.
  """
  @callback handle_message(message :: Message.t(), context :: context()) ::
              {:ok, Message.t(), context()} | {:error, Error.t()}

  @doc """
  Handles a batch of messages for delivery to the sink.
  """
  @callback handle_batch(
              batch_name :: atom(),
              messages :: [Message.t()],
              batch_info :: term(),
              context :: context()
            ) :: {:ok, [Message.t()], context()} | {:error, Error.t()}

  @doc """
  Returns the processor configuration to be merged with Broadway config.

  The returned keywords are merged into the `:processors` configuration.
  """
  @callback processors_config(consumer :: SinkConsumer.t()) :: keyword()

  @doc """
  Returns the batcher configuration to be merged with Broadway config.

  The returned keywords are merged into the `:batchers` configuration.
  Return an empty list if batching is not needed.
  """
  @callback batchers_config(consumer :: SinkConsumer.t()) :: keyword()

  @doc """
  Returns the key/value pairs which control the pipeline after routing.

  Receives the result of running the routing transform.
  Should return the effective values of all relevant keys.
  It's intended that the return value become the batch key.
  """
  @callback apply_routing(self :: SinkConsumer.t(), rinfo :: map()) :: map()

  @optional_callbacks [
    processors_config: 1,
    batchers_config: 1,
    handle_message: 2,
    apply_routing: 2
  ]

  @doc """
  Starts a new pipeline process.

  Required options:
  * `:consumer` - The SinkConsumer struct
  * `:pipeline_mod` - The module implementing SinkPipelineBehaviour

  Optional options:
  * `:producer` - The producer module (defaults to ConsumerProducer)
  * `:test_pid` - PID for test setup
  """
  @spec start_link([opt]) :: Broadway.on_start()
        when opt:
               {:consumer, SinkConsumer.t()}
               | {:pipeline_mod, module()}
               | {:producer, module()}
               | {:test_pid, pid()}
  def start_link(opts) do
    %SinkConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer_id)
      |> Consumers.get_sink_consumer!()
      |> preload_consumer()
      # Ensure db is not on there
      |> Ecto.reset_fields([:postgres_database])

    slot_message_store_mod = Keyword.get(opts, :slot_message_store_mod, Sequin.Runtime.SlotMessageStore)
    producer = Keyword.get(opts, :producer, Sequin.Runtime.ConsumerProducer)
    pipeline_mod = Keyword.get(opts, :pipeline_mod, pipeline_mod_for_consumer(consumer))
    test_pid = Keyword.get(opts, :test_pid)

    context = %{
      pipeline_mod: pipeline_mod,
      pipeline_mod_opts: Keyword.take(opts, [:req_opts, :features]),
      consumer_id: consumer.id,
      account_id: consumer.account_id,
      slot_message_store_mod: slot_message_store_mod,
      test_pid: test_pid
    }

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer_id: consumer.id, test_pid: test_pid]}
      ],
      processors: processors_config(pipeline_mod, consumer),
      batchers: batchers_config(pipeline_mod, consumer),
      context: context
    )
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:consumers, {__MODULE__, consumer_id}}}
  end

  def producer(consumer_id) do
    consumer_id
    |> via_tuple()
    |> Broadway.producer_names()
    |> List.first()
  end

  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  def handle_message(_, message, %{pipeline_mod: pipeline_mod} = context) do
    setup_allowances(context[:test_pid])

    Logger.metadata(
      account_id: context.account_id,
      consumer_id: context.consumer_id
    )

    context = context(context)
    message = format_timestamps(message, context.consumer)

    # Have to ensure module is loaded to trust function_exported?
    Code.ensure_loaded?(pipeline_mod)

    case filter_message(message, context.consumer) do
      {:ok, true} ->
        if function_exported?(pipeline_mod, :handle_message, 2) do
          case pipeline_mod.handle_message(message, context) do
            {:ok, message, next_context} ->
              update_context(context, next_context)
              message

            {:error, error} ->
              Message.failed(message, error)
          end
        else
          message
        end

      {:ok, false} ->
        Message.put_batcher(message, :filtered_messages)

      {:error, error} ->
        Message.failed(message, error)
    end
  end

  @impl Broadway
  def handle_batch(:filtered_messages, messages, _batch_info, context) do
    if test_pid = context[:test_pid] do
      send(test_pid, {__MODULE__, :filtered_messages, messages})
    end

    messages
  end

  @impl Broadway
  def handle_batch(batch_name, messages, batch_info, %{pipeline_mod: pipeline_mod} = context) do
    setup_allowances(context[:test_pid])

    Logger.metadata(
      account_id: context.account_id,
      consumer_id: context.consumer_id
    )

    context = context(context)

    case reject_delivered_messages(context, messages) do
      {[], already_delivered} ->
        already_delivered

      {to_deliver, already_delivered} ->
        Prometheus.increment_message_deliver_attempt(context.consumer.id, context.consumer.name, length(to_deliver))
        delivered = deliver_messages(pipeline_mod, batch_name, to_deliver, batch_info, context)

        Trace.info(context.consumer.id, %Trace.Event{
          message: "Delivered messages",
          extra: %{
            messages: Enum.map(delivered, & &1.data.data)
          }
        })

        delivered ++ already_delivered
    end
  end

  defp filter_message(message, %SinkConsumer{} = consumer) do
    {:ok, Consumers.matches_filter?(consumer, message.data)}
  rescue
    error ->
      Health.put_event(consumer, %Event{slug: :messages_filtered, status: :fail, error: error})
      {:error, error}
  end

  defp deliver_messages(pipeline_mod, batch_name, to_deliver, batch_info, context) do
    now = Sequin.utc_now()

    to_deliver
    |> Stream.map(fn
      %Broadway.Message{data: %ConsumerEvent{ingested_at: ingested_at}} -> ingested_at
      %Broadway.Message{data: %ConsumerRecord{ingested_at: ingested_at}} -> ingested_at
    end)
    |> Stream.filter(& &1)
    |> Stream.map(&DateTime.diff(now, &1, :microsecond))
    |> Enum.each(&Prometheus.observe_internal_latency(context.consumer.id, context.consumer.name, &1))

    case :timer.tc(pipeline_mod, :handle_batch, [batch_name, to_deliver, batch_info, context], :microsecond) do
      {t, {:ok, delivered, next_context}} ->
        Prometheus.increment_message_deliver_success(context.consumer.id, context.consumer.name, length(delivered))
        Prometheus.observe_delivery_latency(context.consumer.id, context.consumer.name, :ok, t)

        update_context(context, next_context)
        delivered

      {t, {:error, error}} ->
        Prometheus.increment_message_deliver_failure(context.consumer.id, context.consumer.name, length(to_deliver))
        Prometheus.observe_delivery_latency(context.consumer.id, context.consumer.name, :error, t)

        Enum.map(to_deliver, fn message ->
          Message.failed(message, error)
        end)
    end
  rescue
    error ->
      Prometheus.increment_message_deliver_failure(context.consumer.id, context.consumer.name, length(to_deliver))

      reraise error, __STACKTRACE__
  end

  # Give processes a way to modify their context, which allows them to use it as a k/v store
  defp context(context) do
    case Process.get(:runtime_context) do
      nil ->
        init_runtime_context(context)

      runtime_ctx ->
        Map.merge(context, runtime_ctx)
    end
  end

  defp init_runtime_context(context) do
    setup_allowances(context[:test_pid])

    # Add jitter for thundering herd
    unless Application.get_env(:sequin, :env) == :test do
      :timer.sleep(:rand.uniform(50))
    end

    consumer =
      context
      |> Map.fetch!(:consumer_id)
      |> Consumers.get_sink_consumer!()
      |> preload_consumer()

    context =
      context
      |> Map.put(:consumer, consumer)
      |> context.pipeline_mod.init(context.pipeline_mod_opts)

    Process.put(:runtime_context, context)
    context
  end

  defp update_context(context, next_context) when context == next_context, do: :ok

  defp update_context(_context, next_context) do
    Process.put(:runtime_context, next_context)
  end

  defp processors_config(pipeline_mod, consumer) do
    default = [
      default: [
        concurrency: System.schedulers_online(),
        max_demand: 10,
        min_demand: 5
      ]
    ]

    # Have to ensure module is loaded to trust function_exported?
    Code.ensure_loaded?(pipeline_mod)

    if function_exported?(pipeline_mod, :processors_config, 1) do
      Keyword.merge(default, pipeline_mod.processors_config(consumer))
    else
      default
    end
  end

  defp batchers_config(pipeline_mod, consumer) do
    concurrency =
      if consumer.id == "c90178b2-a419-49fe-b5b3-92950153d6f2" do
        4
      else
        min(System.schedulers_online() * 2, 80)
      end

    batch_timeout = consumer.batch_timeout_ms || 50

    default = [
      default: [
        concurrency: concurrency,
        batch_size: consumer.batch_size,
        batch_timeout: batch_timeout
      ],
      filtered_messages: [
        concurrency: 1,
        batch_size: 1
      ]
    ]

    # Have to ensure module is loaded to trust function_exported?
    Code.ensure_loaded?(pipeline_mod)

    config =
      if function_exported?(pipeline_mod, :batchers_config, 1) do
        Keyword.merge(default, pipeline_mod.batchers_config(consumer))
      else
        default
      end

    default_workers = Application.get_env(:sequin, __MODULE__, [])[:default_workers_per_sink]

    if default_workers do
      put_in(config, [:default, :concurrency], default_workers)
    else
      config
    end
  end

  def apply_routing(consumer, rinfo) do
    pipeline_mod = pipeline_mod_for_consumer(consumer)

    # Have to ensure module is loaded to trust function_exported?
    Code.ensure_loaded?(pipeline_mod)

    if function_exported?(pipeline_mod, :apply_routing, 2) do
      # Have to use apply to avoid warnings from elixir about undefined impls of optional callback
      apply(pipeline_mod, :apply_routing, [consumer, rinfo])
    end
  end

  def pipeline_mod_for_consumer(%SinkConsumer{} = consumer) do
    case consumer.type do
      :azure_event_hub -> Sequin.Runtime.AzureEventHubPipeline
      :elasticsearch -> Sequin.Runtime.ElasticsearchPipeline
      :gcp_pubsub -> Sequin.Runtime.GcpPubsubPipeline
      :http_push -> Sequin.Runtime.HttpPushPipeline
      :kafka -> Sequin.Runtime.KafkaPipeline
      :kinesis -> Sequin.Runtime.KinesisPipeline
      :nats -> Sequin.Runtime.NatsPipeline
      :rabbitmq -> Sequin.Runtime.RabbitMqPipeline
      :redis_stream -> Sequin.Runtime.RedisStreamPipeline
      :redis_string -> Sequin.Runtime.RedisStringPipeline
      :sequin_stream -> Sequin.Runtime.SequinStreamPipeline
      :sqs -> Sequin.Runtime.SqsPipeline
      :typesense -> Sequin.Runtime.TypesensePipeline
      :sns -> Sequin.Runtime.SnsPipeline
    end
  end

  @spec ack({SinkConsumer.t(), pid(), slot_message_store_mod :: atom()}, list(Message.t()), list(Message.t())) :: :ok
  def ack({%SinkConsumer{} = consumer, test_pid, slot_message_store_mod}, successful, failed) do
    ack_successful(consumer, slot_message_store_mod, successful)
    ack_failed(consumer, slot_message_store_mod, failed)

    if test_pid do
      successful_ack_ids = Enum.map(successful, & &1.data.ack_id)
      failed_ack_ids = Enum.map(failed, & &1.data.ack_id)
      send(test_pid, {__MODULE__, :ack_finished, successful_ack_ids, failed_ack_ids})
    end

    :ok
  end

  defp ack_successful(_consumer, _sms, []), do: :ok

  defp ack_successful(consumer, slot_message_store_mod, successful) do
    {filtered, delivered} = Enum.split_with(successful, &(&1.batcher == :filtered_messages))

    delivered_messages = Enum.map(delivered, & &1.data)
    filtered_messages = Enum.map(filtered, & &1.data)
    all_messages = delivered_messages ++ filtered_messages
    # Mark wal_cursors as delivered
    wal_cursors =
      Enum.map(all_messages, fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    :ok = MessageLedgers.wal_cursors_delivered(consumer.id, wal_cursors)
    :ok = MessageLedgers.wal_cursors_reached_checkpoint(consumer.id, "consumer_producer.ack", wal_cursors)

    case slot_message_store_mod.messages_succeeded(consumer, all_messages) do
      {:ok, _count} -> :ok
      {:error, error} -> raise error
    end

    # Update consumer stats + create ack messages
    {:ok, _count} = Consumers.after_messages_acked(consumer, delivered_messages)

    :ok
  end

  defp ack_failed(_consumer, _sms, []), do: :ok

  defp ack_failed(%SinkConsumer{} = consumer, slot_message_store_mod, failed) do
    failed_message_metadatas =
      failed
      |> Stream.map(&Consumers.advance_delivery_state_for_failure/1)
      |> Enum.map(&%{&1 | data: nil})

    Enum.each(failed, fn %Message{} = message ->
      error =
        case message.status do
          {:failed, error} when is_exception(error) ->
            error

          {:failed, error} ->
            Error.service(service: "sink_pipeline", code: :unknown_error, message: error)

          {:error, error, _stacktrace} when is_exception(error) ->
            Error.service(service: "sink_pipeline", code: :unknown_error, message: Exception.message(error))

          {:error, error} ->
            Error.service(service: "sink_pipeline", code: :unknown_error, message: error)

          {:exit, error} ->
            Error.service(service: "sink_pipeline", code: :unknown_error, message: inspect(error, pretty: true))
        end

      error_message = Exception.message(error)

      Logger.warning("Failed to deliver messages to sink: #{error_message}")
      [%{ack_id: failed_ack_id} | _] = failed_message_metadatas

      Health.put_event(consumer, %Health.Event{
        slug: :messages_delivered,
        status: :fail,
        error: error,
        extra: %{ack_id: failed_ack_id}
      })

      Sequin.Logs.log_for_consumer_message(
        :error,
        consumer.account_id,
        consumer.id,
        [message.data.replication_message_trace_id],
        "Failed to send message: #{error_message}"
      )
    end)

    :ok = slot_message_store_mod.messages_failed(consumer, failed_message_metadatas)
  end

  defp reject_delivered_messages(context, messages) do
    consumer = context.consumer
    slot_message_store_mod = context.slot_message_store_mod
    consumer_messages = Enum.map(messages, & &1.data)

    wal_cursors_to_deliver =
      consumer_messages
      |> Stream.reject(fn
        # We don't enforce idempotency for read actions
        %ConsumerEvent{data: %ConsumerEventData{action: :read}} -> true
        %ConsumerRecord{data: %ConsumerRecordData{action: :read}} -> true
        # We only recently added :action to ConsumerRecordData, so we need to ignore
        # any messages that don't have it for backwards compatibility
        %ConsumerRecord{data: %ConsumerRecordData{action: nil}} -> true
        _ -> false
      end)
      |> Enum.map(fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    {:ok, delivered_wal_cursors} =
      MessageLedgers.filter_delivered_wal_cursors(consumer.id, wal_cursors_to_deliver)

    :ok = MessageLedgers.wal_cursors_delivered(consumer.id, delivered_wal_cursors)

    delivered_cursor_set = MapSet.new(delivered_wal_cursors)

    {already_delivered, to_deliver} =
      Enum.split_with(messages, fn message ->
        MapSet.member?(delivered_cursor_set, %{commit_lsn: message.data.commit_lsn, commit_idx: message.data.commit_idx})
      end)

    if already_delivered == [] do
      {to_deliver, []}
    else
      Logger.info(
        "[SinkPipeline] Rejected messages for idempotency",
        rejected_message_count: length(already_delivered),
        commits: delivered_wal_cursors,
        message_count: length(already_delivered)
      )

      slot_message_store_mod.messages_already_succeeded(consumer, Enum.map(already_delivered, & &1.data))

      {to_deliver, already_delivered}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Runtime.SinkPipelineMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.ApplicationMock, test_pid, self())
    Sandbox.allow(Sequin.Repo, test_pid, self())
  end

  defp preload_consumer(consumer) do
    Repo.lazy_preload(consumer, [:sequence, :transform, :routing, :filter])
  end

  # Formats timestamps according to the consumer's timestamp_format setting
  defp format_timestamps(%{data: %struct{} = event_or_record} = message, %SinkConsumer{} = consumer)
       when struct in [ConsumerEvent, ConsumerRecord] do
    %{message | data: %{event_or_record | data: format_timestamps(event_or_record.data, consumer)}}
  end

  defp format_timestamps(%struct{} = event_or_record_data, %SinkConsumer{} = consumer)
       when struct in [ConsumerEventData, ConsumerRecordData] do
    case consumer.timestamp_format do
      :unix_microsecond ->
        record = format_timestamps_to_unix(event_or_record_data.record, :microsecond)
        changes = format_timestamps_to_unix(event_or_record_data.changes, :microsecond)
        %{event_or_record_data | record: record, changes: changes}

      # Keep as ISO8601 (default)
      _iso8601 ->
        event_or_record_data
    end
  end

  # Generic function to convert timestamps in any nested data structure
  defp format_timestamps_to_unix(data, unit) do
    Sequin.Enum.transform_deeply(data, fn
      %DateTime{} = dt -> DateTime.to_unix(dt, unit)
      %NaiveDateTime{} = dt -> dt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(unit)
      other -> other
    end)
  end
end
