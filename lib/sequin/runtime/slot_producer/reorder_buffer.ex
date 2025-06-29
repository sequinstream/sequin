defmodule Sequin.Runtime.SlotProducer.ReorderBuffer do
  @moduledoc false
  use GenStage

  alias Sequin.Runtime.SlotProducer.Batch
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Message

  require Logger

  def start_link(opts \\ []) do
    id = Keyword.fetch!(opts, :id)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id))
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  def handle_batch_marker(id, %BatchMarker{} = marker) do
    GenStage.async_info(via_tuple(id), {:batch_marker, marker})
  end

  def config(key) do
    Keyword.fetch!(config(), key)
  end

  def config do
    Application.fetch_env!(:sequin, __MODULE__)
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :pending_batches_by_epoch, %{non_neg_integer() => Batch.t()}, default: %{}
      field :ready_batches_by_epoch, %{non_neg_integer() => Batch.t()}, default: %{}
      field :on_batch_ready, (Batch.t() -> :ok | {:error, any()})
      field :producer_partition_count, non_neg_integer()
      field :producer_subscriptions, [%{producer: pid(), demand: integer()}], default: []
      field :flush_batch_timer_ref, reference() | nil
      field :setting_max_demand, non_neg_integer()
      field :setting_min_demand, non_neg_integer()
      field :setting_retry_flush_batch_interval, non_neg_integer()
    end
  end

  @impl GenStage
  def init(opts) do
    state = %State{
      on_batch_ready: Keyword.fetch!(opts, :on_batch_ready),
      producer_partition_count: Keyword.fetch!(opts, :producer_partitions),
      setting_max_demand: Keyword.get(opts, :max_demand),
      setting_min_demand: Keyword.get(opts, :min_demand),
      setting_retry_flush_batch_interval: Keyword.get(opts, :retry_flush_batch_interval)
    }

    {:consumer, state, subscribe_to: Keyword.get(opts, :subscribe_to, [])}
  end

  @impl GenStage
  def handle_subscribe(:producer, _opts, from, %State{} = state) do
    sub = %{producer: from, demand: 0}
    state = %{state | producer_subscriptions: [sub | state.producer_subscriptions]}
    state = ask_demand(state)

    {:manual, state}
  end

  @impl GenStage
  def handle_cancel(_reason, from, %State{} = state) do
    state = %{state | producer_subscriptions: Enum.reject(state.producer_subscriptions, &(&1.producer == from))}
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_events(events, from, %State{} = state) do
    state = Enum.reduce(events, state, &add_event_to_state/2)

    state =
      state
      |> update_subscription_demand(from, -length(events))
      |> maybe_ask_demand()

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:batch_marker, %BatchMarker{} = marker}, %State{} = state) do
    state = put_batch_marker(state, marker)
    {:noreply, [], state}
  end

  def handle_info(:flush_batch, %State{} = state) do
    # Order ready batches by epoch and ensure epoch ordering
    ready_epoch = state.ready_batches_by_epoch |> Map.keys() |> Enum.min()
    {batch, ready_batches} = Map.pop(state.ready_batches_by_epoch, ready_epoch)

    state = maybe_cancel_flush_batch_timer(state)

    case state.on_batch_ready.(batch) do
      :ok ->
        state = %{state | ready_batches_by_epoch: ready_batches}

        # Ask for demand now that we've successfully flushed a batch
        state = ask_demand(state)

        # Schedule immediate flush if more ready batches exist
        state =
          if map_size(ready_batches) > 0 do
            schedule_flush_timer(state)
          else
            state
          end

        {:noreply, [], state}

      {:error, reason} ->
        Logger.warning("[ReorderBuffer] Error pushing batch #{inspect(batch)}: #{inspect(reason)}")
        state = schedule_flush_timer_retry(state)
        {:noreply, [], state}
    end
  end

  defp maybe_ask_demand(%State{ready_batches_by_epoch: batches} = state) when map_size(batches) == 0 do
    ask_demand(state)
  end

  defp maybe_ask_demand(%State{} = state), do: state

  defp ask_demand(%State{} = state) do
    Enum.reduce(state.producer_subscriptions, state, fn sub, acc_state ->
      if sub.demand < setting(state, :setting_min_demand) do
        demand_to_ask = setting(state, :setting_max_demand) - sub.demand
        GenStage.ask(sub.producer, demand_to_ask)

        # Update the subscription with the new demand
        update_subscription_demand(acc_state, sub.producer, demand_to_ask)
      else
        acc_state
      end
    end)
  end

  defp update_subscription_demand(%State{} = state, from, demand_change) do
    updated_subs =
      Enum.map(state.producer_subscriptions, fn sub ->
        if sub.producer == from do
          %{sub | demand: sub.demand + demand_change}
        else
          sub
        end
      end)

    %{state | producer_subscriptions: updated_subs}
  end

  defp add_event_to_state(%Message{batch_epoch: epoch} = event, %State{} = state) when is_integer(epoch) do
    if Map.get(state.ready_batches_by_epoch, epoch) do
      raise "Received a message for a completed batch"
    end

    pending_batches =
      Map.update(state.pending_batches_by_epoch, epoch, %Batch{epoch: epoch, messages: [event]}, fn %Batch{} = batch ->
        %{batch | messages: [event | batch.messages]}
      end)

    %{state | pending_batches_by_epoch: pending_batches}
  end

  defp put_batch_marker(%State{} = state, %BatchMarker{epoch: epoch} = marker) when is_integer(epoch) do
    # Validate that we don't receive markers for epochs that are already ready
    if Map.has_key?(state.ready_batches_by_epoch, epoch) do
      raise "Received batch marker for epoch #{epoch} that is already ready"
    end

    batch =
      case Map.fetch(state.pending_batches_by_epoch, epoch) do
        {:ok, batch} ->
          Batch.put_marker(batch, marker)

        _ ->
          Batch.init_from_marker(marker)
      end

    if MapSet.size(batch.markers_received) == state.producer_partition_count do
      # Batch is ready - move from pending to ready
      # First, verify state
      lower_pending_epoch =
        Enum.find(Map.keys(state.pending_batches_by_epoch), fn other_epoch -> other_epoch < epoch end)

      unless is_nil(lower_pending_epoch) do
        raise "Batch epochs completed out-of-order: other_epoch=#{lower_pending_epoch} min_ready_epoch=#{epoch}"
      end

      # Then, sort messages
      messages = Enum.sort_by(batch.messages, fn %Message{} = msg -> {msg.commit_lsn, msg.commit_idx} end)

      was_empty = map_size(state.ready_batches_by_epoch) == 0
      pending_batches = Map.delete(state.pending_batches_by_epoch, epoch)
      ready_batches = Map.put(state.ready_batches_by_epoch, epoch, %{batch | messages: messages})

      state = %{state | pending_batches_by_epoch: pending_batches, ready_batches_by_epoch: ready_batches}

      # If this was the first ready batch, trigger flush
      state = if was_empty, do: schedule_flush_timer(state), else: state

      state
    else
      # Still waiting for more markers - keep in pending
      %{state | pending_batches_by_epoch: Map.put(state.pending_batches_by_epoch, epoch, batch)}
    end
  end

  defp schedule_flush_timer(%State{flush_batch_timer_ref: nil} = state) do
    ref = Process.send_after(self(), :flush_batch, 0)
    %{state | flush_batch_timer_ref: ref}
  end

  defp schedule_flush_timer_retry(%State{flush_batch_timer_ref: nil} = state) do
    interval = setting(state, :setting_retry_flush_batch_interval)
    ref = Process.send_after(self(), :flush_batch, interval)
    %{state | flush_batch_timer_ref: ref}
  end

  defp maybe_cancel_flush_batch_timer(%State{flush_batch_timer_ref: nil} = state), do: state

  defp maybe_cancel_flush_batch_timer(%State{flush_batch_timer_ref: timer} = state) do
    case Process.cancel_timer(timer) do
      false ->
        receive do
          :flush_batch -> :ok
        after
          0 -> :ok
        end

      _ ->
        :ok
    end

    %{state | flush_batch_timer_ref: nil}
  end

  def setting(%State{} = state, setting) do
    case Map.fetch!(state, setting) do
      nil ->
        key =
          case setting do
            :setting_max_demand -> :max_demand
            :setting_min_demand -> :min_demand
            :setting_retry_flush_batch_interval -> :retry_flush_batch_interval
          end

        config(key)

      setting ->
        setting
    end
  end
end
