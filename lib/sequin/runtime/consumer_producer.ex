defmodule Sequin.Runtime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.consumer_producer",
    interval: :timer.seconds(30)

  use Sequin.ProcessMetrics.Decorator

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.ProcessMetrics
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore

  require Logger

  @min_log_time_ms 100

  @impl GenStage
  def init(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    slot_message_store_mod = Keyword.get(opts, :slot_message_store_mod, SlotMessageStore)
    Logger.metadata(consumer_id: consumer_id)
    Logger.info("Initializing consumer producer")

    if test_pid = Keyword.get(opts, :test_pid) do
      Sandbox.allow(Sequin.Repo, test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
      Mox.allow(Sequin.Runtime.SlotMessageStoreMock, test_pid, self())
    end

    :syn.join(:consumers, {:messages_maybe_available, consumer_id}, self())

    state = %{
      demand: 0,
      consumer_id: consumer_id,
      consumer: nil,
      receive_timer: nil,
      trim_timer: nil,
      batch_timeout: Keyword.get(opts, :batch_timeout, :timer.seconds(10)),
      test_pid: test_pid,
      scheduled_handle_demand: false,
      slot_message_store_mod: slot_message_store_mod
    }

    # Add dynamic tags for metrics
    ProcessMetrics.metadata(%{
      consumer_id: consumer_id
    })

    ProcessMetrics.start()

    Process.send_after(self(), :init, 0)

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_state = maybe_schedule_demand(state)
    new_state = %{new_state | demand: demand + incoming_demand}

    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:init, state) do
    consumer = state.consumer_id |> Consumers.get_consumer!() |> Repo.preload([:replication_slot])
    db = Databases.get_db!(consumer.replication_slot.postgres_database_id)

    # postgres_database.tables can get very big, remove for efficiency
    consumer = %{consumer | postgres_database: %{db | tables: []}}

    # We need to trigger an immediate gc after removing tables from the consumer struct
    # Ideally, we would not load tables in the first place. We'll be moving tables into their own table soon
    :erlang.garbage_collect()

    Logger.metadata(replication_slot_id: consumer.replication_slot_id)

    state =
      state
      |> schedule_receive_messages()
      |> schedule_trim_idempotency()

    {:noreply, [], %{state | consumer: consumer}}
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
  def handle_info(:messages_maybe_available, state) do
    new_state = maybe_schedule_demand(state)
    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:trim_idempotency, state) do
    %SinkConsumer{} = consumer = state.consumer

    case Postgres.confirmed_flush_lsn(consumer.postgres_database, consumer.replication_slot.slot_name) do
      {:ok, nil} ->
        :ok

      {:ok, lsn} ->
        MessageLedgers.trim_delivered_cursors_set(state.consumer.id, %{commit_lsn: lsn, commit_idx: 0})

      {:error, error} when is_exception(error) ->
        Logger.error("Error trimming idempotency seqs", error: Exception.message(error))

      {:error, error} ->
        Logger.error("Error trimming idempotency seqs", error: inspect(error))
    end

    {:noreply, [], schedule_trim_idempotency(state)}
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    {time, messages} = :timer.tc(fn -> produce_messages(state, demand) end, :millisecond)
    more_upstream_messages? = length(messages) == demand

    if time > @min_log_time_ms do
      Logger.warning(
        "[ConsumerProducer] produce_messages took longer than expected",
        count: length(messages),
        demand: demand,
        duration_ms: time
      )
    end

    # Track message processing metrics
    ProcessMetrics.increment_throughput("messages_produced", length(messages))

    # Cut this struct down as it will be passed to each process
    # Processes already have the consumer in context, but this is for the acknowledger. When we
    # consolidate pipelines, we can `configure_ack` to add the consumer to the acknowledger context.
    bare_consumer =
      %SinkConsumer{
        state.consumer
        | active_backfills: [],
          postgres_database: nil,
          replication_slot: nil,
          account: nil
      }

    broadway_messages =
      Enum.map(messages, fn message ->
        %Message{
          data: message,
          acknowledger: {SinkPipeline, {bare_consumer, state.test_pid, state.slot_message_store_mod}, nil}
        }
      end)

    new_demand = demand - length(broadway_messages)
    new_demand = if new_demand < 0, do: 0, else: new_demand
    state = %{state | demand: new_demand}

    if new_demand > 0 and more_upstream_messages? do
      {:noreply, broadway_messages, maybe_schedule_demand(state)}
    else
      {:noreply, broadway_messages, state}
    end
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  @decorate track_metrics("produce_messages")
  defp produce_messages(state, count) do
    consumer = state.consumer

    case state.slot_message_store_mod.produce(consumer, count, self()) do
      {:ok, messages} ->
        unless messages == [] do
          Health.put_event(consumer, %Event{slug: :messages_pending_delivery, status: :success})
        end

        messages

      {:error, _error} ->
        []
    end
  end

  defp schedule_receive_messages(state) do
    if state.receive_timer, do: Process.cancel_timer(state.receive_timer)
    receive_timer = Process.send_after(self(), :receive_messages, state.batch_timeout)
    %{state | receive_timer: receive_timer}
  end

  defp schedule_trim_idempotency(state) do
    trim_timer = Process.send_after(self(), :trim_idempotency, :timer.seconds(10))
    %{state | trim_timer: trim_timer}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer, trim_timer: trim_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    if trim_timer, do: Process.cancel_timer(trim_timer)
    {:noreply, [], %{state | receive_timer: nil, trim_timer: nil}}
  end

  defp maybe_schedule_demand(%{scheduled_handle_demand: false} = state) do
    Process.send_after(self(), :handle_demand, 10)
    %{state | scheduled_handle_demand: true}
  end

  defp maybe_schedule_demand(state), do: state
end
