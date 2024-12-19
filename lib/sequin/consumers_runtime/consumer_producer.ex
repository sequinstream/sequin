defmodule Sequin.ConsumersRuntime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Extensions.Replication

  require Logger

  @impl GenStage
  def init(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    Logger.info("Initializing consumer producer", consumer_id: consumer.id)

    if test_pid = Keyword.get(opts, :test_pid) do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    :syn.join(:consumers, {:messages_ingested, consumer.id}, self())

    state = %{
      demand: 0,
      consumer: consumer,
      receive_timer: nil,
      batch_size: Keyword.get(opts, :batch_size, 10),
      batch_timeout: Keyword.get(opts, :batch_timeout, :timer.seconds(1)),
      test_pid: test_pid,
      scheduled_handle_demand: false
    }

    state = schedule_receive_messages(state)

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_state = %{state | demand: demand + incoming_demand}
    new_state = maybe_schedule_demand(new_state)

    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:handle_demand, state) do
    handle_receive_messages(%{state | scheduled_handle_demand: false})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    new_state = schedule_receive_messages(state)
    handle_receive_messages(new_state)
  end

  @impl GenStage
  def handle_info(:messages_ingested, state) do
    new_state = maybe_schedule_demand(state)
    {:noreply, [], new_state}
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    {:ok, messages} = Replication.receive_messages(state.consumer, demand * state.consumer.batch_size)

    broadway_messages =
      messages
      |> Stream.chunk_every(state.consumer.batch_size)
      |> Enum.map(fn batch ->
        %Message{
          data: batch,
          acknowledger: {__MODULE__, {state.consumer, state.test_pid}, nil}
        }
      end)

    new_demand = demand - length(broadway_messages)
    new_demand = if new_demand < 0, do: 0, else: new_demand

    {:noreply, broadway_messages, %{state | demand: new_demand}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp schedule_receive_messages(state) do
    receive_timer = Process.send_after(self(), :receive_messages, state.batch_timeout)
    %{state | receive_timer: receive_timer}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  def ack({consumer, _test_pid}, _successful, failed) do
    if length(failed) > 0 do
      Logger.warning("Failed to ack messages for consumer #{consumer.id}")
    end

    :ok
  end

  defp maybe_schedule_demand(%{scheduled_handle_demand: false, demand: demand} = state) when demand > 0 do
    Process.send_after(self(), :handle_demand, 10)
    %{state | scheduled_handle_demand: true}
  end

  defp maybe_schedule_demand(state), do: state
end
