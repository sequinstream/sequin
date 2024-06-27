defmodule Sequin.ProcessRegistry do
  @moduledoc false
  def start_link do
    Registry.start_link(keys: :unique, name: __MODULE__)
  end

  # def lookup(name) do
  #   Registry.lookup(__MODULE__, name)
  # end

  def whereis_name(name) do
    Registry.whereis_name({__MODULE__, name})
  end

  def via_tuple(key) do
    {:via, Registry, {__MODULE__, key}}
  end

  def child_spec(_) do
    Supervisor.child_spec(
      Registry,
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    )
  end
end
