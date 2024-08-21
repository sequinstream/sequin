defmodule Sequin.ConsumersRuntime.Supervisor do
  @moduledoc false
  use Supervisor

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.ConsumersRuntime
  alias Sequin.ConsumersRuntime.HttpPushPipeline

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_for_push_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id, opts \\ [])

  def start_for_push_consumer(supervisor, %HttpPushConsumer{} = consumer, opts) do
    default_opts = [consumer: consumer]
    opts = Keyword.merge(default_opts, opts)

    Sequin.DynamicSupervisor.start_child(supervisor, {HttpPushPipeline, opts})
  end

  def start_for_push_consumer(supervisor, id, opts) do
    case Consumers.get_consumer(id) do
      {:ok, consumer} -> start_for_push_consumer(supervisor, consumer, opts)
      error -> error
    end
  end

  def stop_for_push_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id)

  def stop_for_push_consumer(supervisor, %HttpPushConsumer{id: id}) do
    stop_for_push_consumer(supervisor, id)
  end

  def stop_for_push_consumer(supervisor, id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, HttpPushPipeline.via_tuple(id))
    :ok
  end

  def restart_for_push_consumer(supervisor \\ ConsumersRuntime.DynamicSupervisor, consumer_or_id) do
    stop_for_push_consumer(supervisor, consumer_or_id)
    start_for_push_consumer(supervisor, consumer_or_id)
  end

  defp children do
    [
      Sequin.ConsumersRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: ConsumersRuntime.DynamicSupervisor)
    ]
  end
end
