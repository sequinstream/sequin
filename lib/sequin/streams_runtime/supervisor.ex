defmodule Sequin.StreamsRuntime.Supervisor do
  @moduledoc """
  """
  use Supervisor

  alias Sequin.StreamsRuntime.AssignMessageSeq
  alias Sequin.StreamsRuntime.ConsumerSupervisor
  alias Sequin.StreamsRuntime.PopulateOutstandingMessages
  alias Sequin.StreamsRuntime.StreamSupervisor

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_for_consumer(supervisor \\ ConsumerSupervisor, consumer_id) do
    # Start another Supervisor if we go beyond one child
    Sequin.DynamicSupervisor.start_child(supervisor, {PopulateOutstandingMessages, [consumer_id: consumer_id]})
  end

  def stop_for_consumer(supervisor \\ ConsumerSupervisor, consumer_id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, PopulateOutstandingMessages.via_tuple(consumer_id))
  end

  def restart_for_consumer(supervisor \\ ConsumerSupervisor, consumer_id) do
    stop_for_consumer(supervisor, consumer_id)
    start_for_consumer(supervisor, consumer_id)
  end

  def start_for_stream(supervisor \\ StreamSupervisor, stream_id) do
    # Start another Supervisor if we go beyond one child
    Sequin.DynamicSupervisor.start_child(supervisor, {AssignMessageSeq, [stream_id: stream_id]})
  end

  def stop_for_stream(supervisor \\ StreamSupervisor, stream_id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, AssignMessageSeq.via_tuple(stream_id))
  end

  def restart_for_stream(supervisor \\ StreamSupervisor, stream_id) do
    stop_for_stream(supervisor, stream_id)
    start_for_stream(supervisor, stream_id)
  end

  defp children do
    [
      Sequin.StreamsRuntime.Starter,
      {Sequin.DynamicSupervisor, name: Sequin.StreamsRuntime.ConsumerSupervisor},
      {Sequin.DynamicSupervisor, name: Sequin.StreamsRuntime.StreamSupervisor}
    ]
  end
end
