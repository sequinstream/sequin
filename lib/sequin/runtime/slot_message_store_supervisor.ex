defmodule Sequin.Runtime.SlotMessageStoreSupervisor do
  @moduledoc """
  A supervisor for partitioned slot message stores.
  """

  use Supervisor

  alias Sequin.Runtime.SlotMessageStore

  def partition_count, do: 3

  def child_spec(opts) do
    consumer = Keyword.fetch!(opts, :consumer)

    %{
      id: via_tuple(consumer.id),
      start: {__MODULE__, :start_link, [opts]}
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
    opts = Keyword.put(opts, :consumer_id, opts[:consumer].id)

    children =
      Enum.map(0..(partition_count() - 1), fn idx ->
        opts = Keyword.put(opts, :partition, idx)

        {SlotMessageStore, opts}
      end)

    Supervisor.init(children, strategy: :one_for_all)
  end

  def partitions do
    Enum.to_list(0..(partition_count() - 1))
  end
end
