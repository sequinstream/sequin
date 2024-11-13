defmodule Sequin.ConsumersRuntime.Supervisor do
  @moduledoc false
  use Supervisor

  alias Sequin.Consumers
  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.ConsumersRuntime
  alias Sequin.ConsumersRuntime.HttpPushPipeline
  alias Sequin.ConsumersRuntime.SqsPipeline

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_for_destination_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id, opts \\ [])

  def start_for_destination_consumer(supervisor, %DestinationConsumer{} = consumer, opts) do
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

  def start_for_destination_consumer(supervisor, id, opts) do
    case Consumers.get_consumer(id) do
      {:ok, consumer} -> start_for_destination_consumer(supervisor, consumer, opts)
      error -> error
    end
  end

  def stop_for_destination_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id)

  def stop_for_destination_consumer(supervisor, %DestinationConsumer{id: id}) do
    stop_for_destination_consumer(supervisor, id)
  end

  def stop_for_destination_consumer(supervisor, id) do
    Enum.each([HttpPushPipeline, SqsPipeline], &Sequin.DynamicSupervisor.stop_child(supervisor, &1.via_tuple(id)))
    :ok
  end

  def restart_for_destination_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id) do
    stop_for_destination_consumer(supervisor, consumer_or_id)
    start_for_destination_consumer(supervisor, consumer_or_id)
  end

  defp pipeline(%DestinationConsumer{} = consumer) do
    case consumer.type do
      :http_push -> HttpPushPipeline
      :sqs -> SqsPipeline
    end
  end

  defp children do
    [
      Sequin.ConsumersRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: ConsumersRuntime.DynamicSupervisor)
    ]
  end
end
