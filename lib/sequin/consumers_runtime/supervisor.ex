defmodule Sequin.ConsumersRuntime.Supervisor do
  @moduledoc false
  use Supervisor

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime
  alias Sequin.ConsumersRuntime.AzureEventHubPipeline
  alias Sequin.ConsumersRuntime.GcpPubsubPipeline
  alias Sequin.ConsumersRuntime.HttpPushPipeline
  alias Sequin.ConsumersRuntime.KafkaPipeline
  alias Sequin.ConsumersRuntime.NatsPipeline
  alias Sequin.ConsumersRuntime.RabbitMqPipeline
  alias Sequin.ConsumersRuntime.RedisPipeline
  alias Sequin.ConsumersRuntime.SqsPipeline
  alias Sequin.DatabasesRuntime.SlotSupervisor
  alias Sequin.Repo

  require Logger

  @sinks_to_pipelines %{
    http_push: HttpPushPipeline,
    sqs: SqsPipeline,
    redis: RedisPipeline,
    kafka: KafkaPipeline,
    gcp_pubsub: GcpPubsubPipeline,
    nats: NatsPipeline,
    rabbitmq: RabbitMqPipeline,
    azure_event_hub: AzureEventHubPipeline
  }

  def sinks_to_pipelines, do: @sinks_to_pipelines

  defp dynamic_supervisor, do: {:via, :syn, {:consumers, ConsumersRuntime.DynamicSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:consumers, __MODULE__}})
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_for_sink_consumer(supervisor \\ dynamic_supervisor(), consumer_or_id, opts \\ [])

  def start_for_sink_consumer(supervisor, %SinkConsumer{} = consumer, opts) do
    consumer = Repo.preload(consumer, [:sequence, :postgres_database], force: true)

    SlotSupervisor.start_message_store!(consumer)

    if consumer.type == :sequin_stream do
      :ok
    else
      default_opts = [consumer: consumer]
      consumer_features = Consumers.consumer_features(consumer)

      {features, opts} = Keyword.pop(opts, :features, [])
      features = Keyword.merge(consumer_features, features)

      opts =
        default_opts
        |> Keyword.merge(opts)
        |> Keyword.put(:features, features)

      Sequin.DynamicSupervisor.start_child(supervisor, {pipeline(consumer), opts})
    end
  end

  def start_for_sink_consumer(supervisor, id, opts) do
    case Consumers.get_consumer(id) do
      {:ok, consumer} -> start_for_sink_consumer(supervisor, consumer, opts)
      error -> error
    end
  end

  def stop_for_sink_consumer(supervisor \\ dynamic_supervisor(), consumer_or_id)

  def stop_for_sink_consumer(supervisor, %SinkConsumer{id: id}) do
    Enum.each(
      Map.values(@sinks_to_pipelines),
      &Sequin.DynamicSupervisor.stop_child(supervisor, &1.via_tuple(id))
    )

    :ok
  end

  def stop_for_sink_consumer(supervisor, id) do
    Enum.each(
      Map.values(@sinks_to_pipelines),
      &Sequin.DynamicSupervisor.stop_child(supervisor, &1.via_tuple(id))
    )

    :ok
  end

  def restart_for_sink_consumer(supervisor \\ dynamic_supervisor(), consumer_or_id) do
    stop_for_sink_consumer(supervisor, consumer_or_id)
    start_for_sink_consumer(supervisor, consumer_or_id)
  end

  defp pipeline(%SinkConsumer{} = consumer) do
    Map.fetch!(@sinks_to_pipelines, consumer.type)
  end

  defp children do
    [
      Sequin.ConsumersRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: dynamic_supervisor())
    ]
  end
end
