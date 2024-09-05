defmodule Sequin.Tracer.Server do
  @moduledoc false
  use GenServer

  import Sequin.Consumers.Guards, only: [is_event_or_record: 1]

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.Message

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :account_id, String.t()
      field :events, list(), default: []
    end
  end

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

  def message_replicated(%PostgresDatabase{account_id: account_id}, %Message{} = message) do
    GenServer.cast(via_tuple(account_id), {:replicated, message})
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

  def init(account_id) do
    {:ok, %State{account_id: account_id, events: []}}
  end

  def handle_cast({:replicated, %Message{} = message}, %State{events: events} = state) do
    Logger.info("Replicated message: #{inspect(message)}")
    {:noreply, %{state | events: [{:replicated, message} | events]}}
  end

  def handle_cast({:filtered, _consumer, %Message{} = message}, %State{events: events} = state) do
    Logger.info("Filtered message: #{inspect(message)}")
    {:noreply, %{state | events: [{:filtered, message} | events]}}
  end

  def handle_cast({:ingested, _consumer, event_or_records}, %State{events: events} = state) do
    Logger.info("Ingested messages: #{inspect(event_or_records)}")
    {:noreply, %{state | events: [{:ingested, event_or_records} | events]}}
  end

  def handle_cast({:received, _consumer, event_or_records}, %State{events: events} = state) do
    Logger.info("Received messages: #{inspect(event_or_records)}")
    {:noreply, %{state | events: [{:received, event_or_records} | events]}}
  end

  def handle_cast({:acked, _consumer, ack_ids}, %State{events: events} = state) do
    Logger.info("Acked messages: #{inspect(ack_ids)}")
    {:noreply, %{state | events: [{:acked, ack_ids} | events]}}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  if Mix.env() == :dev do
    def handle_call(:reset_state, _from, state) do
      {:reply, :ok, %State{state | events: []}}
    end
  end

  # Helper Functions

  defp via_tuple(account_id) do
    Sequin.Registry.via_tuple({__MODULE__, account_id})
  end

  # Error Handling

  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error("Tracer.Server exited: #{inspect(reason)}")
    {:stop, reason, state}
  end
end
