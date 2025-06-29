defmodule Sequin.Runtime.SlotProducer.ReorderBuffer do
  @moduledoc false
  use GenStage

  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProducer.BatchMarker

  require Logger

  def start_link(opts \\ []) do
    id = Keyword.fetch!(opts, :id)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id))
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  def handle_batch_marker(id, {marker, from_idx}) do
    GenStage.sync_info(via_tuple(id), {:batch_marker, {marker, from_idx}})
  end

  def config(key) do
    Keyword.fetch!(config(), key)
  end

  def config do
    Application.fetch_env!(:sequin, __MODULE__)
  end

  defmodule Batch do
    @moduledoc false
    use TypedStruct

    alias Sequin.Runtime.SlotProducer.BatchMarker

    typedstruct do
      field :marker, BatchMarker.t()
      field :epoch, non_neg_integer()
      field :messages, list(), default: []
      field :markers_received, MapSet.t(), default: MapSet.new()
      field :ready?, boolean(), default: false
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :batches_by_epoch, %{non_neg_integer() => Batch.t()}, default: %{}
      field :bytes_since_last_limit_check, non_neg_integer(), default: 0
      field :check_system_fn, (-> :ok | {:error, any()})
      field :check_system_timer_ref, reference() | nil
      field :on_batch_ready, (BatchMarker.t(), [Message.t()] -> :ok | {:error, any()})
      field :producer_partition_count, non_neg_integer()
      field :producer_subscriptions, [%{producer: pid(), demand: integer()}], default: []
      field :retry_batch_timer_ref, reference() | nil
      field :setting_bytes_between_limit_checks, non_neg_integer()
      field :setting_check_system_for_recover_interval, non_neg_integer()
      field :setting_max_demand, non_neg_integer()
      field :setting_min_demand, non_neg_integer()
      field :setting_retry_batch_interval, non_neg_integer()
      field :status, :active | :inactive, default: :active
    end
  end

  @impl GenStage
  def init(opts) do
    state = %State{
      check_system_fn: Keyword.get(opts, :check_system_fn, &default_check_system_fn/1),
      on_batch_ready: Keyword.fetch!(opts, :on_batch_ready),
      producer_partition_count: Keyword.fetch!(opts, :producer_partitions),
      setting_bytes_between_limit_checks: Keyword.get(opts, :bytes_between_limit_checks),
      setting_check_system_for_recover_interval: Keyword.get(opts, :check_system_for_recover_interval),
      setting_max_demand: Keyword.get(opts, :setting_max_demand),
      setting_min_demand: Keyword.get(opts, :setting_min_demand),
      setting_retry_batch_interval: Keyword.get(opts, :retry_batch_interval)
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
    # Update the demand for the producer that sent these events
    state =
      state
      |> update_subscription_demand(from, -length(events))
      |> add_events_to_batches(events)
      |> ask_demand()
      |> maybe_schedule_limit_check()

    {:noreply, [], state}
  end

  @impl GenStage

  def handle_info({:batch_marker, {%BatchMarker{} = marker, from_idx}}, %State{} = state) do
    %Batch{} = batch = Map.get_lazy(state.batches_by_epoch, marker.epoch, fn -> %Batch{epoch: marker.epoch} end)
    # Set the marker if it hasn't been set yet
    batch = if batch.marker == nil, do: %{batch | marker: marker}, else: batch
    batch = Map.update!(batch, :markers_received, &MapSet.put(&1, from_idx))

    batch =
      if MapSet.size(batch.markers_received) == state.producer_partition_count and not batch.ready? do
        Process.send_after(self(), :batch_ready, 0)
        %{batch | ready?: true}
      else
        batch
      end

    {:noreply, [], %{state | batches_by_epoch: Map.put(state.batches_by_epoch, marker.epoch, batch)}}
  end

  def handle_info(:batch_ready, %State{} = state) do
    # Find the lowest epoch that is ready and complete
    batches = Map.values(state.batches_by_epoch)
    min_epoch = batches |> Enum.min_by(& &1.epoch) |> Map.fetch!(:epoch)
    [batch | other_ready_batches] = Enum.filter(batches, & &1.ready?)

    unless batch.epoch == min_epoch do
      raise "Batch epochs completed out-of-order: min_epoch=#{min_epoch}, min_ready_epoch=#{batch.epoch}"
    end

    # Can be nil, as this is called on a retry timer as well as when a batch is completed
    if is_nil(batch) do
      {:noreply, [], state}
    else
      messages = Enum.sort_by(batch.messages, fn %Message{} = msg -> {msg.commit_lsn, msg.commit_idx} end)

      case state.on_batch_ready.(batch.marker, messages) do
        :ok ->
          updated_batches = Map.delete(state.batches_by_epoch, batch.epoch)

          unless other_ready_batches == [] do
            Process.send_after(self(), :batch_ready, 0)
          end

          {:noreply, [], %{state | batches_by_epoch: updated_batches}}

        {:error, reason} ->
          Logger.warning("[ReorderBuffer] Error pushing batch #{inspect(batch.marker)}: #{inspect(reason)}")
          Process.send_after(self(), :batch_ready, setting(state, :setting_retry_batch_interval))
          {:noreply, [], state}
      end
    end
  end

  def handle_info(:check_limit, %State{} = state) do
    case state.check_system_fn.() do
      :ok ->
        if state.status == :inactive do
          Logger.info("[ReorderBuffer] Resuming pipeline")
        end

        {:noreply, [], %{state | status: :active}}

      {:error, reason} ->
        Logger.warning("[ReorderBuffer] Pausing pipeline: #{inspect(reason)}")
        schedule_check_limit(state, setting(state, :setting_check_system_for_recover_interval))
        {:noreply, [], %{state | status: :inactive}}
    end

    {:noreply, [], state}
  end

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

  defp add_events_to_batches(%State{} = state, events) do
    bytes_received = Enum.reduce(events, 0, fn %Message{} = msg, acc -> msg.byte_size + acc end)
    state = %{state | bytes_since_last_limit_check: state.bytes_since_last_limit_check + bytes_received}

    Enum.reduce(events, state, fn %Message{} = event, acc_state ->
      epoch = event.batch_epoch

      if is_nil(epoch) do
        raise "Received a message without an epoch"
      end

      batch =
        Map.get_lazy(acc_state.batches_by_epoch, epoch, fn ->
          # Create a batch without a marker initially - the marker will be set via handle_batch_marker
          %Batch{epoch: epoch}
        end)

      updated_batch = %{batch | messages: [event | batch.messages]}
      %{acc_state | batches_by_epoch: Map.put(acc_state.batches_by_epoch, epoch, updated_batch)}
    end)
  end

  defp maybe_schedule_limit_check(%State{} = state) do
    if state.bytes_since_last_limit_check > setting(state, :setting_bytes_between_limit_checks) do
      state = schedule_check_limit(state, 0)
      %{state | bytes_since_last_limit_check: 0}
    else
      state
    end
  end

  defp schedule_check_limit(%State{check_system_timer_ref: nil} = state, interval) do
    ref = Process.send_after(self(), :check_limit, interval)
    %{state | check_system_timer_ref: ref}
  end

  defp schedule_check_limit(%State{} = state, _interval), do: state

  defp default_check_system_fn(_state), do: :ok

  def setting(%State{} = state, setting) do
    case Map.fetch!(state, setting) do
      nil ->
        key =
          case setting do
            :setting_bytes_between_limit_checks -> :bytes_between_limit_checks
            :setting_check_system_for_recover_interval -> :check_system_for_recover_interval
            :setting_max_demand -> :max_demand
            :setting_min_demand -> :min_demand
            :setting_retry_batch_interval -> :batch_interval
          end

        config(key)

      setting ->
        setting
    end
  end
end
