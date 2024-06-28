defmodule Sequin.StreamsRuntime.PopulateOutstandingMessages do
  @moduledoc """
  This GenServer pulls messages from the `messages` table into `outstanding_messages` for a given consumer.

  We perform this work here inside of a single-threaded process to avoid race conditions
  introduced by multiple workers attempting to pull messages into outstanding_messages.
  """
  use GenServer

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Streams

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumer_id, Ecto.UUID.t()
      field :interval_ms, non_neg_integer()
      field :test_pid, pid() | nil
    end
  end

  def child_spec(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)

    %{
      id: via_tuple(consumer_id),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def start_link(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(consumer_id))
  end

  def via_tuple(consumer_id) do
    Sequin.ProcessRegistry.via_tuple({__MODULE__, consumer_id})
  end

  @impl GenServer
  def init(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    interval = Keyword.get(opts, :interval_ms, 100)
    test_pid = Keyword.get(opts, :test_pid)

    if test_pid do
      :ok = Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    state = %State{
      consumer_id: consumer_id,
      interval_ms: interval,
      test_pid: test_pid
    }

    Process.send_after(self(), :poll, 0)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    case Streams.populate_outstanding_messages_with_lock(state.consumer_id) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error("[PopulateOutstandingMessages] Error populating outstanding messages",
          error: reason,
          consumer_id: state.consumer_id
        )
    end

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :populate_done})
    end

    schedule_poll(state)
    {:noreply, state}
  rescue
    error ->
      unless env() == :prod do
        reraise error, __STACKTRACE__
      end

      Logger.error("[PopulateOutstandingMessages] Error populating outstanding messages",
        error: error,
        consumer_id: state.consumer_id
      )

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
