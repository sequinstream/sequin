defmodule Sequin.Tracer.Starter do
  @moduledoc false
  use GenServer

  alias Sequin.Accounts
  alias Sequin.Tracer.DynamicSupervisor

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    schedule_start(1000)
    {:ok, state}
  end

  @impl true
  def handle_info(:start, state) do
    start_tracers_for_all_accounts()
    schedule_start()
    {:noreply, state}
  end

  defp start_tracers_for_all_accounts do
    Enum.each(Accounts.list_accounts(), &DynamicSupervisor.start_for_account(&1.id))
  end

  defp schedule_start(timeout \\ :timer.seconds(30)) do
    Process.send_after(self(), :start, timeout)
  end
end
