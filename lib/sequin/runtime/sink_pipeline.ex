defmodule Sequin.Runtime.SinkPipeline do
  @moduledoc """
  Generic pipeline implementation that delegates to a specific sink pipeline behaviour.


  Also defines a behaviour for implementing a message pipeline that processes
  and delivers messages to various sinks (Kafka, HTTP, SQS, etc.).
  """

  use Broadway

  alias Broadway.Message
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers

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

  @optional_callbacks [
    processors_config: 1,
    batchers_config: 1,
    handle_message: 2
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
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload([:sequence, :postgres_database])

    slot_message_store_mod = Keyword.get(opts, :slot_message_store_mod, Sequin.Runtime.SlotMessageStore)
    producer = Keyword.get(opts, :producer, Sequin.Runtime.ConsumerProducer)
    pipeline_mod = Keyword.get(opts, :pipeline_mod, pipeline_mod_for_consumer(consumer))
    test_pid = Keyword.get(opts, :test_pid)

    context = %{
      pipeline_mod: pipeline_mod,
      consumer: consumer,
      slot_message_store_mod: slot_message_store_mod,
      test_pid: test_pid
    }

    context = pipeline_mod.init(context, opts)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid]}
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
    Logger.metadata(
      account_id: context.consumer.account_id,
      consumer_id: context.consumer.id
    )

    context = context(context)

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
  end

  @impl Broadway
  def handle_batch(batch_name, messages, batch_info, %{pipeline_mod: pipeline_mod} = context) do
    Logger.metadata(
      account_id: context.consumer.account_id,
      consumer_id: context.consumer.id
    )

    context = context(context)

    messages = reject_delivered_messages(context, messages)

    case pipeline_mod.handle_batch(batch_name, messages, batch_info, context) do
      {:ok, messages, next_context} ->
        update_context(context, next_context)
        messages

      {:error, error} ->
        Enum.map(messages, fn message ->
          Message.failed(message, error)
        end)
    end
  end

  # Give processes a way to modify their context, which allows them to use it as a k/v store
  defp context(context) do
    ctx = Process.get(:runtime_context, %{})
    Map.merge(context, ctx)
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

    if function_exported?(pipeline_mod, :processors_config, 1) do
      Keyword.merge(default, pipeline_mod.processors_config(consumer))
    else
      default
    end
  end

  defp batchers_config(pipeline_mod, consumer) do
    concurrency = min(System.schedulers_online() * 2, 80)

    default = [
      default: [
        concurrency: concurrency,
        batch_size: consumer.batch_size,
        batch_timeout: 50
      ]
    ]

    if function_exported?(pipeline_mod, :batchers_config, 1) do
      Keyword.merge(default, pipeline_mod.batchers_config(consumer))
    else
      default
    end
  end

  defp pipeline_mod_for_consumer(%SinkConsumer{} = consumer) do
    case consumer.type do
      :azure_event_hub -> Sequin.Runtime.AzureEventHubPipeline
      :gcp_pubsub -> Sequin.Runtime.GcpPubsubPipeline
      :http_push -> Sequin.Runtime.HttpPushPipeline
      :kafka -> Sequin.Runtime.KafkaPipeline
      :nats -> Sequin.Runtime.NatsPipeline
      :rabbitmq -> Sequin.Runtime.RabbitMqPipeline
      :redis -> Sequin.Runtime.RedisPipeline
      :sequin_stream -> Sequin.Runtime.SequinStreamPipeline
      :sqs -> Sequin.Runtime.SqsPipeline
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
    successful_messages = Enum.map(successful, & &1.data)
    successful_ack_ids = Enum.map(successful_messages, & &1.ack_id)

    # Mark wal_cursors as delivered
    wal_cursors =
      Enum.map(successful_messages, fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    :ok = MessageLedgers.wal_cursors_delivered(consumer.id, wal_cursors)
    :ok = MessageLedgers.wal_cursors_reached_checkpoint(consumer.id, "consumer_producer.ack", wal_cursors)

    case slot_message_store_mod.messages_succeeded(consumer.id, successful_ack_ids) do
      {:ok, _count} -> :ok
      {:error, error} -> raise error
    end

    # Update consumer stats + create ack messages
    {:ok, _count} = Consumers.after_messages_acked(consumer, successful_messages)

    Health.put_event(consumer, %Health.Event{slug: :messages_delivered, status: :success})

    :ok
  end

  defp ack_failed(_consumer, _sms, []), do: :ok

  defp ack_failed(%SinkConsumer{} = consumer, slot_message_store_mod, failed) do
    failed_message_datas = Enum.map(failed, & &1.data)

    failed_message_metadatas =
      failed_message_datas
      |> Stream.map(&Consumers.advance_delivery_state_for_failure/1)
      |> Enum.map(&%{&1 | data: nil})

    Enum.map(failed, fn %Message{} = message ->
      {:failed, error} = message.status

      error =
        if is_exception(error) do
          error
        else
          Error.service(service: "sink_pipeline", code: :unknown_error, message: error)
        end

      error_message = Exception.message(error)

      Logger.warning("Failed to deliver messages to sink: #{error_message}")
      Health.put_event(consumer, %Health.Event{slug: :messages_delivered, status: :fail, error: error})

      Sequin.Logs.log_for_consumer_message(
        :error,
        consumer.account_id,
        consumer.id,
        [message.data.replication_message_trace_id],
        "Failed to send message: #{error_message}"
      )
    end)

    :ok = slot_message_store_mod.messages_failed(consumer.id, failed_message_metadatas)
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

    {delivered_messages, filtered_messages} =
      Enum.split_with(messages, fn message ->
        MapSet.member?(delivered_cursor_set, %{commit_lsn: message.data.commit_lsn, commit_idx: message.data.commit_idx})
      end)

    if delivered_messages == [] do
      filtered_messages
    else
      Logger.info(
        "[SinkPipeline] Rejected messages for idempotency",
        rejected_message_count: length(delivered_messages),
        commits: delivered_wal_cursors,
        message_count: length(filtered_messages)
      )

      slot_message_store_mod.messages_already_succeeded(consumer, Enum.map(delivered_messages, & &1.ack_id))

      filtered_messages
    end
  end
end
