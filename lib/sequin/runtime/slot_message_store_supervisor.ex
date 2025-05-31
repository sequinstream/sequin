defmodule Sequin.Runtime.SlotMessageStoreSupervisor do
  @moduledoc """
  A supervisor for partitioned slot message stores.
  """

  use Supervisor

  alias Sequin.Runtime.SlotMessageStore

  def child_spec(opts) do
    consumer = Keyword.fetch!(opts, :consumer)

    restart =
      if Application.get_env(:sequin, :env) == :test do
        Keyword.get(opts, :restart, :permanent)
      else
        :permanent
      end

    %{
      id: via_tuple(consumer.id),
      start: {__MODULE__, :start_link, [opts]},
      restart: restart
    }
  end

  def start_link(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    Supervisor.start_link(__MODULE__, opts, name: via_tuple(consumer.id))
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:replication, {__MODULE__, consumer_id}}}
  end

  def init(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    opts = Keyword.put(opts, :consumer_id, consumer.id)
    partition_count = consumer.partition_count

    children =
      Enum.map(0..(partition_count - 1), fn idx ->
        opts = Keyword.put(opts, :partition, idx)

        {SlotMessageStore, opts}
      end)

    Supervisor.init(children, strategy: :one_for_all)
  end
end
