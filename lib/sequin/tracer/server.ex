defmodule Sequin.Tracer.Server do
  @moduledoc false
  use GenServer

  import Sequin.Consumers.Guards, only: [is_event_or_record: 1]

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.Message
  alias Sequin.Tracer.State

  require Logger

  # Client API

  def child_spec(account_id) do
    %{
      id: {__MODULE__, account_id},
      start: {__MODULE__, :start_link, [account_id]},
      restart: :permanent,
      shutdown: 5000,
      type: :worker
    }
  end

  def start_link(account_id) do
    GenServer.start_link(__MODULE__, account_id, name: via_tuple(account_id))
  end

  def update_heartbeat(account_id) do
    GenServer.cast(via_tuple(account_id), :update_heartbeat)
  end

  def message_replicated(%PostgresDatabase{account_id: account_id} = db, %Message{} = message) do
    GenServer.cast(via_tuple(account_id), {:replicated, db, message})
  end

  def message_filtered(consumer, %Message{} = message) do
    GenServer.cast(via_tuple(consumer.account_id), {:filtered, consumer, message})
  end

  def messages_ingested(_consumer, []), do: :ok

  def messages_ingested(consumer, event_or_records) do
    GenServer.cast(via_tuple(consumer.account_id), {:ingested, consumer, event_or_records})
  end

  def messages_received(_consumer, []), do: :ok

  def messages_received(consumer, [first | _rest] = event_or_records) when is_event_or_record(first) do
    GenServer.cast(via_tuple(consumer.account_id), {:received, consumer, event_or_records})
  end

  def messages_acked(_consumer, []), do: :ok

  def messages_acked(consumer, ack_ids) do
    GenServer.cast(via_tuple(consumer.account_id), {:acked, consumer, ack_ids})
  end

  def get_state(account_id) do
    case GenServer.whereis(via_tuple(account_id)) do
      nil -> {:error, :not_started}
      pid -> GenServer.call(pid, :get_state)
    end
  end

  if Mix.env() == :dev do
    def reset_state(account_id) do
      GenServer.call(via_tuple(account_id), :reset_state)
    end
  end

  # Server Callbacks

  @heartbeat_interval :timer.seconds(5)
  @max_idle_time :timer.minutes(1)

  def init(account_id) do
    Logger.metadata(account_id: account_id)
    Logger.info("Starting Tracer.Server for account #{account_id}")
    schedule_heartbeat_check()
    {:ok, {State.new(account_id), DateTime.utc_now()}}
  end

  def handle_cast({:replicated, %PostgresDatabase{} = db, %Message{} = message}, {state, last_heartbeat}) do
    Logger.info("Replicated message: #{inspect(message)}")
    {:noreply, {State.message_replicated(state, db, message), last_heartbeat}}
  end

  def handle_cast({:filtered, consumer, %Message{} = message}, {state, last_heartbeat}) do
    Logger.info("Filtered message: #{inspect(message)}")
    {:noreply, {State.message_filtered(state, consumer, message), last_heartbeat}}
  end

  def handle_cast({:ingested, consumer, event_or_records}, {state, last_heartbeat}) do
    Logger.info("Ingested messages: #{inspect(event_or_records)}")
    {:noreply, {State.messages_ingested(state, consumer.id, event_or_records), last_heartbeat}}
  end

  def handle_cast({:received, consumer, event_or_records}, {state, last_heartbeat}) do
    Logger.info("Received messages: #{inspect(event_or_records)}")
    {:noreply, {State.messages_received(state, consumer.id, event_or_records), last_heartbeat}}
  end

  def handle_cast({:acked, consumer, ack_ids}, {state, last_heartbeat}) do
    Logger.info("Acked messages: #{inspect(ack_ids)}")
    {:noreply, {State.messages_acked(state, consumer.id, ack_ids), last_heartbeat}}
  end

  def handle_cast(:update_heartbeat, {state, _last_heartbeat}) do
    {:noreply, {state, DateTime.utc_now()}}
  end

  def handle_call(:get_state, _from, {state, last_heartbeat}) do
    {:reply, state, {state, last_heartbeat}}
  end

  if Mix.env() == :dev do
    def handle_call(:reset_state, _from, {state, _last_heartbeat}) do
      {:reply, :ok, {State.reset(state), DateTime.utc_now()}}
    end
  end

  def handle_info(:check_heartbeat, {state, last_heartbeat}) do
    if should_terminate?(last_heartbeat) do
      Logger.info("Tracer.Server stopping due to idle timeout")
      {:stop, :normal, {state, last_heartbeat}}
    else
      schedule_heartbeat_check()
      {:noreply, {state, last_heartbeat}}
    end
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error("Tracer.Server exited: #{inspect(reason)}")
    {:stop, reason, state}
  end

  # Helper Functions

  defp via_tuple(account_id) do
    Sequin.Registry.via_tuple({__MODULE__, account_id})
  end

  defp schedule_heartbeat_check do
    Process.send_after(self(), :check_heartbeat, @heartbeat_interval)
  end

  defp should_terminate?(last_heartbeat) do
    DateTime.diff(DateTime.utc_now(), last_heartbeat, :millisecond) > @max_idle_time
  end
end
