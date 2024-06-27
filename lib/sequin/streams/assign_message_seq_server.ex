defmodule Sequin.Streams.AssignMessageSeqServer do
  @moduledoc """
  This GenServer updates messages in the message table that are missing a `seq`. It updates them
  with a `seq`.

  We perform this work here inside of a single-threaded process to avoid race conditions
  associated with assigning an auto-incrementing value to rows in Postgres.

  The GenServer uses a lock in Postgres to assure that only one transaction is updating
  the `seq` column at a time.
  """
  use GenServer

  alias __MODULE__
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Streams

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :interval_ms, non_neg_integer()
      field :test_pid, pid()
    end
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, AssignMessageSeqServer)
    GenServer.start_link(AssignMessageSeqServer, opts, name: name)
  end

  @impl GenServer
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, 1)
    test_pid = Keyword.get(opts, :test_pid, nil)

    if test_pid do
      :ok = Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    state = %State{interval_ms: interval, test_pid: test_pid}
    Process.send_after(self(), :poll, 0)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    case Streams.assign_message_seqs_with_lock() do
      {:ok, _} ->
        :ok

      {:error, :locked} ->
        Logger.warning("[AssignMessageSeqServer] Tried to assign message seqs, but another process already has the lock")
        :ok
    end

    if state.test_pid do
      send(state.test_pid, {AssignMessageSeqServer, :assign_done})
    end

    schedule_poll(state)

    {:noreply, state}
  rescue
    error ->
      unless env() == :prod do
        reraise error, __STACKTRACE__
      end

      Logger.error("[AssignMessageSeqServer] Error assigning message seqs", error: error)
      schedule_poll(state)

      {:noreply, state}
  end

  defp schedule_poll(%State{} = state) do
    Process.send_after(self(), :poll, state.interval_ms)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end
