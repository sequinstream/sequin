defmodule Sequin.StreamsRuntime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.Message
  alias Sequin.Repo
  alias Sequin.Streams

  @impl GenStage
  def init(opts) do
    consumer = opts |> Keyword.fetch!(:consumer) |> Repo.preload(:http_endpoint)

    state = %{
      demand: 0,
      consumer: consumer,
      receive_timer: nil,
      batch_size: Keyword.get(opts, :batch_size, 10),
      batch_timeout: Keyword.get(opts, :batch_timeout, 1000)
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
    {:ok, messages} = Streams.receive_for_consumer(state.consumer, batch_size: messages_to_fetch)

    broadway_messages =
      Enum.map(messages, fn message ->
        %Message{
          data: message,
          acknowledger: {__MODULE__, state.consumer, nil}
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

  def ack(consumer, successful, failed) do
    successful_ids = Enum.map(successful, & &1.data.ack_id)
    failed_ids = Enum.map(failed, & &1.data.ack_id)

    if length(successful_ids) > 0 do
      Streams.ack_messages(consumer, successful_ids)
    end

    if length(failed_ids) > 0 do
      Streams.nack_messages(consumer, failed_ids)
    end

    :ok
  end
end
