defmodule Sequin.Runtime.SlotProducer.ReorderBuffer do
  @moduledoc false
  use GenStage
  use Sequin.GenerateBehaviour

  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Runtime.SlotProducer.Batch
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Runtime.SlotProducer.PipelineDefaults

  require Logger

  @callback flush_batch(id :: PostgresReplicationSlot.id(), batch :: Batch.t()) :: :ok

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
      field :id, String.t()
      field :pending_batches_by_idx, %{non_neg_integer() => Batch.t()}, default: %{}
      field :ready_batches_by_idx, %{non_neg_integer() => Batch.t()}, default: %{}
      field :flush_batch_fn, (PostgresReplicationSlot.id(), Batch.t() -> :ok)
      field :producer_partition_count, non_neg_integer()
      field :producer_subscriptions, [%{producer: pid(), demand: integer()}], default: []
      field :flush_batch_timer_ref, reference() | nil
      field :setting_max_demand, non_neg_integer()
      field :setting_min_demand, non_neg_integer()
      field :setting_max_ready_batches, non_neg_integer()
      field :setting_retry_flush_batch_interval, non_neg_integer()
      field :check_system_fn, (-> :ok | :fail)
      field :check_system_bytes_processed_since_last_check, non_neg_integer(), default: 0
      field :check_system_interval, keyword(), default: []
      field :check_system_last_status, :ok | :fail, default: :ok
      field :check_system_timer_ref, reference() | nil
    end
  end

  @impl GenStage
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    account_id = Keyword.fetch!(opts, :account_id)

    Logger.metadata(replication_id: id, account_id: account_id)

    Sequin.name_process({__MODULE__, id})

    state = %State{
      id: Keyword.fetch!(opts, :id),
      flush_batch_fn: Keyword.get(opts, :flush_batch_fn, &PipelineDefaults.flush_batch/2),
      producer_partition_count: Keyword.fetch!(opts, :producer_partitions),
      setting_max_demand: Keyword.get(opts, :max_demand),
      setting_min_demand: Keyword.get(opts, :min_demand),
      setting_max_ready_batches: Keyword.get(opts, :setting_max_ready_batches, 2),
      setting_retry_flush_batch_interval: Keyword.get(opts, :retry_flush_batch_interval),
      check_system_fn: Keyword.get(opts, :check_system_fn, &default_check_system_fn/0),
      check_system_interval: Keyword.get(opts, :check_system_interval, bytes: 10_000_000, retry_ms: 25)
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
  def handle_events(events, from, %State{} = state) do
    state = Enum.reduce(events, state, &add_event_to_state/2)

    state =
      state
      |> update_subscription_demand(from, -length(events))
      |> maybe_check_system()
      |> maybe_ask_demand()

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:batch_marker, %BatchMarker{} = marker}, %State{} = state) do
    state = put_batch_marker(state, marker)
    {:noreply, [], state}
  end

  def handle_info(:flush_batch, %State{} = state) do
    # Order ready batches by idx and ensure idx ordering
    ready_idx = state.ready_batches_by_idx |> Map.keys() |> Enum.min()
    {batch, ready_batches} = Map.pop(state.ready_batches_by_idx, ready_idx)

    state = maybe_cancel_flush_batch_timer(state)

    case state.flush_batch_fn.(state.id, batch) do
      :ok ->
        state = %{state | ready_batches_by_idx: ready_batches}

        # Ask for demand now that we've successfully flushed a batch
        state
        |> maybe_ask_demand()
        |> maybe_schedule_flush_timer()
        |> maybe_hibernate()

      {:error, reason} ->
        Logger.warning("[ReorderBuffer] Error pushing batch #{inspect(batch)}: #{inspect(reason)}")
        state = schedule_flush_timer_retry(state)
        {:noreply, [], state}
    end
  end

  def handle_info(:check_system, %State{} = state) do
    state = check_system(%{state | check_system_timer_ref: nil})
    {:noreply, [], state}
  end

  defp maybe_ask_demand(%State{ready_batches_by_idx: batches, check_system_last_status: :ok} = state)
       when map_size(batches) <= state.setting_max_ready_batches do
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

  defp add_event_to_state(%Message{batch_idx: idx} = event, %State{} = state) when is_integer(idx) do
    if Map.get(state.ready_batches_by_idx, idx) do
      raise "Received a message for a completed batch"
    end

    pending_batches =
      Map.update(state.pending_batches_by_idx, idx, %Batch{idx: idx, messages: [event]}, fn %Batch{} = batch ->
        %{batch | messages: [event | batch.messages]}
      end)

    %{
      state
      | pending_batches_by_idx: pending_batches,
        check_system_bytes_processed_since_last_check:
          state.check_system_bytes_processed_since_last_check + event.byte_size
    }
  end

  defp put_batch_marker(%State{} = state, %BatchMarker{idx: idx} = marker) when is_integer(idx) do
    # Validate that we don't receive markers for idxs that are already ready
    if Map.has_key?(state.ready_batches_by_idx, idx) do
      raise "Received batch marker for idx #{idx} that is already ready"
    end

    batch =
      case Map.fetch(state.pending_batches_by_idx, idx) do
        {:ok, batch} ->
          Batch.put_marker(batch, marker)

        _ ->
          Batch.init_from_marker(marker)
      end

    if MapSet.size(batch.markers_received) == state.producer_partition_count do
      # Batch is ready - move from pending to ready
      # First, verify state
      lower_pending_idx =
        Enum.find(Map.keys(state.pending_batches_by_idx), fn other_idx -> other_idx < idx end)

      if !is_nil(lower_pending_idx) do
        raise "Batch idxs completed out-of-order: other_idx=#{lower_pending_idx} min_ready_idx=#{idx}"
      end

      # Then, sort messages
      messages = Enum.sort_by(batch.messages, fn %Message{} = msg -> {msg.commit_lsn, msg.commit_idx} end)

      was_empty = map_size(state.ready_batches_by_idx) == 0
      pending_batches = Map.delete(state.pending_batches_by_idx, idx)
      ready_batches = Map.put(state.ready_batches_by_idx, idx, %{batch | messages: messages})

      state = %{state | pending_batches_by_idx: pending_batches, ready_batches_by_idx: ready_batches}

      # If this was the first ready batch, trigger flush
      state = if was_empty, do: schedule_flush_timer(state), else: state

      state
    else
      # Still waiting for more markers - keep in pending
      %{state | pending_batches_by_idx: Map.put(state.pending_batches_by_idx, idx, batch)}
    end
  end

  defp schedule_flush_timer(%State{flush_batch_timer_ref: nil} = state) do
    ref = Process.send_after(self(), :flush_batch, 0)
    %{state | flush_batch_timer_ref: ref}
  end

  defp maybe_schedule_flush_timer(%State{} = state) do
    if map_size(state.ready_batches_by_idx) > 0 do
      schedule_flush_timer(state)
    else
      state
    end
  end

  defp maybe_hibernate(%State{} = state) do
    if map_size(state.ready_batches_by_idx) == 0 and map_size(state.pending_batches_by_idx) == 0 do
      {:noreply, [], state, :hibernate}
    else
      {:noreply, [], state}
    end
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

  defp maybe_check_system(%State{} = state) do
    check_bytes = Keyword.fetch!(state.check_system_interval, :bytes)

    if state.check_system_bytes_processed_since_last_check >= check_bytes do
      state
      |> check_system()
      |> Map.put(:check_system_bytes_processed_since_last_check, 0)
    else
      state
    end
  end

  defp check_system(%State{} = state) do
    case state.check_system_fn.() do
      :ok ->
        was_failing = state.check_system_last_status == :fail

        state = if was_failing, do: maybe_ask_demand(state), else: state
        %{state | check_system_last_status: :ok}

      :fail ->
        schedule_check_system_timer_retry(state)
        %{state | check_system_last_status: :fail}
    end
  end

  defp schedule_check_system_timer_retry(%State{check_system_timer_ref: nil} = state) do
    retry_ms = Keyword.fetch!(state.check_system_interval, :retry_ms)
    ref = Process.send_after(self(), :check_system, retry_ms)
    %{state | check_system_timer_ref: ref}
  end

  defp schedule_check_system_timer_retry(state), do: state

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

  defp default_check_system_fn do
    current_memory = :erlang.memory(:total)

    if current_memory >= Application.get_env(:sequin, :max_memory_bytes) do
      :fail
    else
      :ok
    end
  end
end
