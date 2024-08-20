defmodule Sequin.ConsumersRuntime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Repo

  @impl GenStage
  def init(opts) do
    consumer = opts |> Keyword.fetch!(:consumer) |> Repo.preload(:http_endpoint)

    if test_pid = Keyword.get(opts, :test_pid) do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    state = %{
      demand: 0,
      consumer: consumer,
      receive_timer: nil,
      batch_size: Keyword.get(opts, :batch_size, 10),
      batch_timeout: Keyword.get(opts, :batch_timeout, 1000),
      test_pid: test_pid
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    messages_to_fetch = min(demand, state.batch_size)
    {:ok, messages} = Consumers.receive_for_consumer(state.consumer, batch_size: messages_to_fetch)

    broadway_messages =
      Enum.map(messages, fn message ->
        %Message{
          data: message,
          acknowledger: {__MODULE__, {state.consumer, state.test_pid}, nil}
        }
      end)

    new_demand = demand - length(broadway_messages)

    receive_timer =
      case {broadway_messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.batch_timeout)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, broadway_messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp schedule_receive_messages(timeout) do
    Process.send_after(self(), :receive_messages, timeout)
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  def ack({consumer, test_pid}, successful, failed) do
    successful_ids = Enum.map(successful, & &1.data.ack_id)
    failed_ids = Enum.map(failed, & &1.data.ack_id)

    if test_pid do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    if length(successful_ids) > 0 do
      Consumers.ack_messages(consumer, successful_ids)
    end

    if length(failed_ids) > 0 do
      Consumers.nack_messages(consumer, failed_ids)
    end

    if test_pid do
      send(test_pid, {:ack, successful_ids, failed_ids})
    end

    :ok
  end
end
