defmodule Sequin.Streams.Supervisors.ConsumerSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Sequin.Streams.PopulateOutstandingMessagesServer

  def start_link(init_args) do
    {name, init_args} = Keyword.pop(init_args, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, init_args, name: name)
  end

  @impl DynamicSupervisor
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_for_consumer(supervisor \\ __MODULE__, consumer_id) do
    # If consumers require more than one GenServer in the future, start a Supervisor under this
    # DynamicSupervisor
    DynamicSupervisor.start_child(supervisor, {PopulateOutstandingMessagesServer, consumer_id: consumer_id})
  end

  def restart_for_consumer(supervisor \\ __MODULE__, consumer_id) do
    case stop_for_consumer(supervisor, consumer_id) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end

    case start_for_consumer(supervisor, consumer_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  def stop_for_consumer(supervisor \\ __MODULE__, consumer_id) do
    case GenServer.whereis(PopulateOutstandingMessagesServer.via_tuple(consumer_id)) do
      nil -> :ok
      pid -> DynamicSupervisor.terminate_child(supervisor, pid)
    end
  end
end
