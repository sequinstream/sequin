defmodule Sequin.Runtime.SlotMessageStoreSupervisor do
  @moduledoc """
  A supervisor for partitioned slot message stores.
  """

  use Supervisor

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SlotMessageStore

  def child_spec(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)

    restart =
      if Application.get_env(:sequin, :env) == :test do
        Keyword.get(opts, :restart, :permanent)
      else
        :permanent
      end

    %{
      id: via_tuple(consumer_id),
      start: {__MODULE__, :start_link, [opts]},
      restart: restart
    }
  end

  def start_link(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    Supervisor.start_link(__MODULE__, opts, name: via_tuple(consumer_id))
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:replication, {__MODULE__, consumer_id}}}
  end

  def init(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)

    if test_pid = opts[:test_pid] do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    %SinkConsumer{partition_count: partition_count} = Consumers.get_sink_consumer!(consumer_id)

    children =
      Enum.map(0..(partition_count - 1), fn idx ->
        opts = Keyword.put(opts, :partition, idx)

        {SlotMessageStore, opts}
      end)

    Supervisor.init(children, strategy: :one_for_all)
  end
end
