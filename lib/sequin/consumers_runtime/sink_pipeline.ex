defmodule Sequin.ConsumersRuntime.SinkPipeline do
  @moduledoc """
  Behaviour for Sequin pipeline implementations.

  This behaviour defines the contract for implementing a message pipeline that processes
  and delivers messages to various sinks (Kafka, HTTP, SQS, etc.).
  """

  alias Broadway.Message
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.Error
  alias Sequin.Health

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
              Message.t() | {:error, Error.t()}

  @doc """
  Handles a batch of messages for delivery to the sink.
  """
  @callback handle_batch(
              batch_name :: atom(),
              messages :: [Message.t()],
              batch_info :: term(),
              context :: context()
            ) :: [Message.t()] | {:error, Error.t()}

  @doc """
  Updates the context with runtime-specific information.

  Called when a handle_message/handle_batch starts to set up runtime context like partition counts. Uses the process k/v store to cache runtime context values.
  """
  @callback runtime_context(context :: context()) :: context()

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
    runtime_context: 1
  ]

  @spec on_success(context :: context(), messages :: [Message.t()]) :: [Message.t()]
  def on_success(context, messages) do
    consumer = Map.fetch!(context, :consumer)
    :ok = ConsumerProducer.pre_ack_delivered_messages(consumer, messages)
    Health.put_event(consumer, %Health.Event{slug: :messages_delivered, status: :success})

    messages
  end

  @spec on_failure(context :: context(), error :: Error.t(), messages :: [Message.t()]) :: [Message.t()]
  def on_failure(context, error, messages) do
    consumer = Map.fetch!(context, :consumer)
    Logger.warning("Failed to deliver messages to sink: #{Exception.message(error)}")
    Health.put_event(consumer, %Health.Event{slug: :messages_delivered, status: :fail, error: error})

    message_datas = Enum.flat_map(messages, & &1.data)
    trace_ids = Enum.map(message_datas, & &1.replication_message_trace_id)

    Sequin.Logs.log_for_consumer_message(
      :error,
      consumer.account_id,
      consumer.id,
      trace_ids,
      "Failed to push message: #{Exception.message(error)}"
    )

    Enum.map(messages, &Broadway.Message.failed(&1, error))
  end
end
