defmodule Sequin.ConsumersRuntime.SinkBroadway do
  @moduledoc """
  Generic pipeline implementation that delegates to a specific sink pipeline behaviour.
  """
  use Broadway

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Repo

  require Logger

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

    producer = Keyword.get(opts, :producer, Sequin.ConsumersRuntime.ConsumerProducer)
    pipeline_mod = Keyword.get(opts, :pipeline_mod, pipeline_mod_for_consumer(consumer))
    test_pid = Keyword.get(opts, :test_pid)

    context = %{
      pipeline_mod: pipeline_mod,
      consumer: consumer,
      test_pid: test_pid
    }

    context = pipeline_mod.init(context, opts)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid, batch_size: consumer.batch_size]}
      ],
      processors: processors_config(pipeline_mod, consumer),
      batchers: batchers_config(pipeline_mod, consumer),
      context: context
    )
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:consumers, {__MODULE__, consumer_id}}}
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

    context = update_runtime_context(context, pipeline_mod)
    pipeline_mod.handle_message(message, context)
  end

  @impl Broadway
  def handle_batch(batch_name, messages, batch_info, %{pipeline_mod: pipeline_mod} = context) do
    Logger.metadata(
      account_id: context.consumer.account_id,
      consumer_id: context.consumer.id
    )

    context = update_runtime_context(context, pipeline_mod)
    pipeline_mod.handle_batch(batch_name, messages, batch_info, context)
  end

  defp update_runtime_context(context, pipeline_mod) do
    if function_exported?(pipeline_mod, :runtime_context, 1) do
      runtime_context = Process.get(:runtime_context)
      context = Map.merge(context, runtime_context)
      next_runtime_context = pipeline_mod.runtime_context(context)
      Process.put(:runtime_context, next_runtime_context)

      Map.merge(context, next_runtime_context)
    else
      context
    end
  end

  defp processors_config(pipeline_mod, consumer) do
    default = [
      default: [
        concurrency: 1,
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
    default = [
      default: [
        concurrency: 1,
        batch_size: 1,
        batch_timeout: 0
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
      :azure_event_hub -> Sequin.ConsumersRuntime.AzureEventHubPipeline
      :gcp_pubsub -> Sequin.ConsumersRuntime.GcpPubsubPipeline
      :http_push -> Sequin.ConsumersRuntime.HttpPushPipeline
      :kafka -> Sequin.ConsumersRuntime.KafkaPipeline
      :nats -> Sequin.ConsumersRuntime.NatsPipeline
      :rabbitmq -> Sequin.ConsumersRuntime.RabbitMqPipeline
      :redis -> Sequin.ConsumersRuntime.RedisPipeline
      :sequin_stream -> Sequin.ConsumersRuntime.SequinStreamPipeline
      :sqs -> Sequin.ConsumersRuntime.SqsPipeline
    end
  end
end
