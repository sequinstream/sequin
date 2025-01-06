defmodule Sequin.Health2 do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health2.Check
  alias Sequin.Health2.Event
  alias Sequin.Redis
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalPipeline
  alias Sequin.Time

  require Logger

  @type status :: :healthy | :warning | :error | :initializing | :waiting
  @type entity_kind :: :http_endpoint | :sink_consumer | :postgres_replication_slot | :wal_pipeline
  @type redis_error :: {:error, Error.ServiceError.t()}
  @type entity :: PostgresReplicationSlot.t() | HttpEndpoint.t() | SinkConsumer.t() | WalPipeline.t()

  typedstruct do
    field :entity_kind, entity_kind(), enforce: true
    field :entity_id, String.t(), enforce: true
    field :checks, [Check.t()], enforce: true
    field :status, status(), enforce: true

    field :last_healthy_at, DateTime.t() | nil, enforce: true
    field :erroring_since, DateTime.t() | nil, enforce: true
  end

  @debounce_window :timer.seconds(5)

  def put_event(%PostgresReplicationSlot{} = slot, %Event{} = event) do
    put_event(:postgres_replication_slot, slot.id, event)
  end

  # TODO: Allow for skipping debounce with an opt. We'll want to skip debounce inside of health
  # workers, as users may trigger a health check from the UI. (Certain put_event callsites are
  # safe to not debounce, as they will only be called so many times per min.)
  def put_event(entity_kind, entity_id, %Event{} = event) do
    validate_event!(entity_kind, event)

    event_hash = Event.debounce_hash(event)
    debounce_key = "#{entity_kind}:#{entity_id}:#{event.slug}"
    now = :os.system_time(:millisecond)

    case :ets.lookup(:sequin_health_debounce, debounce_key) do
      [{^debounce_key, ^event_hash, last_update}] when now - last_update < @debounce_window ->
        :ok

      _ ->
        :ets.insert(:sequin_health_debounce, {debounce_key, event_hash, now})

        with {:ok, existing_event} <- get_event(entity_id, event.slug) do
          event = Event.set_timestamps(existing_event, event)
          store_event(entity_id, event)
        end
    end
  end

  def health(entity) do
    with {:ok, checks} <- checks(entity) do
      entity_kind = entity_kind(entity)

      status =
        cond do
          Enum.any?(checks, fn check -> check.status == :unhealthy end) -> :error
          Enum.any?(checks, fn check -> check.status == :stale end) -> :warning
          true -> :healthy
        end

      {:ok,
       %__MODULE__{
         entity_kind: entity_kind,
         entity_id: entity.id,
         checks: checks,
         status: status,
         last_healthy_at: last_healthy_at(checks),
         erroring_since: erroring_since(checks)
       }}
    end
  end

  @doc """
  Converts a Health2 struct to a map with only the necessary fields for the frontend.
  """
  @spec to_external(t()) :: map()
  def to_external(%__MODULE__{} = health) do
    %{
      entity_kind: health.entity_kind,
      entity_id: health.entity_id,
      status: health.status,
      name: entity_name(health.entity_kind),
      # status_message: status_message(health.status, health.checks),
      checks:
        Enum.map(health.checks, fn check ->
          %{
            name: check.name,
            status: check.status,
            error: if(check.error, do: %{message: Exception.message(check.error)})
          }
        end)
    }
  end

  defp last_healthy_at(checks) do
    not_healthy_checks = Enum.filter(checks, fn check -> check.status != :healthy end)

    if Enum.any?(not_healthy_checks) do
      not_healthy_checks
      |> Enum.min_by(fn %Check{} = check -> check.last_healthy_at end, &compare_datetimes/2)
      |> Map.get(:last_healthy_at)
    else
      Sequin.utc_now()
    end
  end

  defp erroring_since(checks) do
    unhealthy_checks = Enum.filter(checks, fn check -> check.status == :unhealthy end)

    if Enum.any?(unhealthy_checks) do
      unhealthy_checks
      |> Enum.min_by(fn %Check{} = check -> check.erroring_since end, &compare_datetimes/2)
      |> Map.get(:erroring_since)
    end
  end

  # defp status_message(status, checks) do
  #   check_counts = Enum.frequencies_by(checks, fn check -> check.status end)
  #   total_checks = length(checks)

  #   cond do
  #     status == :healthy and check_counts.healthy == total_checks ->
  #       if total_checks == 1 do
  #         "The health check is passing"
  #       else
  #         "All #{total_checks} health checks are passing"
  #       end

  #     status == :error ->
  #       stale_count = Map.get(check_counts, :stale, 0)
  #       error_count = Map.get(check_counts, :error, 0)

  #       cond do
  #         stale_count > 0 and error_count > 0 ->
  #           "#{error_count} failing, #{stale_count} taking too long"

  #         stale_count > 0 ->
  #           "#{stale_count} health check#{if stale_count > 1, do: "s are", else: " is"} taking too long"

  #         true ->
  #           "#{error_count} health check#{if error_count > 1, do: "s are", else: " is"} failing"
  #       end

  #     status == :warning ->
  #       count = check_counts.warning
  #       "#{count} health check#{if count > 1, do: "s are", else: " is"} warning"

  #     status == :initializing ->
  #       "#{check_counts.healthy} of #{total_checks} check#{if total_checks > 1, do: "s", else: ""} healthy, #{check_counts.initializing} waiting"

  #     true ->
  #       count = Map.get(check_counts, status, 0)
  #       "#{count} health check#{if count > 1, do: "s are", else: " is"} #{status}"
  #   end
  # end

  defp compare_datetimes(nil, _b), do: true
  defp compare_datetimes(_a, nil), do: false
  defp compare_datetimes(a, b), do: DateTime.before?(a, b)

  defp validate_event!(:postgres_replication_slot, %Event{} = event) do
    valid = Event.valid_slug?(:postgres_replication_slot, event.slug) and Event.valid_status?(event.status)

    unless valid do
      raise ArgumentError, "Invalid event: #{event.slug} with status #{event.status}"
    end
  end

  @spec get_event(String.t(), String.t()) :: {:ok, Event.t() | nil} | redis_error()
  defp get_event(entity_id, event_slug) do
    with {:ok, event_json} when is_binary(event_json) <- Redis.command(["HGET", events_key(entity_id), event_slug]) do
      {:ok, Event.from_json!(event_json)}
    end
  end

  @spec checks(entity :: entity()) :: {:ok, [Check.t()]} | redis_error()
  defp checks(entity) do
    with {:ok, events} <- Redis.command(["HVALS", events_key(entity.id)]) do
      events = Enum.map(events, &Event.from_json!/1)
      {:ok, checks(entity, events)}
    end
  end

  defp checks(%PostgresReplicationSlot{} = slot, events) do
    reachable_check = check(:reachable, slot, events)
    config_check = check(:replication_configuration, slot, events)
    connected_check = check(:replication_connected, slot, events)
    messages_check = check(:replication_messages, slot, events)

    cond do
      reachable_check.status == :unhealthy ->
        [
          reachable_check,
          %Check{slug: :replication_configuration, name: "Valid slot", status: :initializing},
          %Check{slug: :replication_connected, name: "Replication connected", status: :initializing},
          %Check{slug: :replication_messages, name: "Replication messages", status: :initializing}
        ]

      config_check.status == :unhealthy ->
        [
          reachable_check,
          config_check,
          %Check{slug: :replication_connected, name: "Replication connected", status: :initializing},
          %Check{slug: :replication_messages, name: "Replication messages", status: :initializing}
        ]

      connected_check.status == :unhealthy ->
        [
          reachable_check,
          config_check,
          connected_check,
          %Check{slug: :replication_messages, name: "Replication messages", status: :initializing}
        ]

      true ->
        [reachable_check, config_check, connected_check, messages_check]
    end
  end

  defp check(:reachable, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :reachable, name: "Database reachable", status: :initializing}
    conn_checked_event = find_event(events, :db_connectivity_checked)

    cond do
      is_nil(conn_checked_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error = expected_event_error(slot.id, :db_connectivity_checked)
        Logger.error("[Sequin.Health] #{error.message}")
        %{base_check | status: :unhealthy, error: error}

      is_nil(conn_checked_event) ->
        base_check

      Time.before_min_ago?(conn_checked_event.last_event_at, 15) ->
        put_check_timestamps(%{base_check | status: :stale}, [conn_checked_event])

      conn_checked_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :unhealthy, error: conn_checked_event.error}, [conn_checked_event])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [conn_checked_event])
    end
  end

  defp check(:replication_configuration, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_configuration, name: "Valid slot", status: :initializing}
    config_checked_event = find_event(events, :replication_slot_checked)

    cond do
      is_nil(config_checked_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error = expected_event_error(slot.id, :replication_slot_checked)
        %{base_check | status: :unhealthy, error: error}

      is_nil(config_checked_event) ->
        base_check

      config_checked_event.status != :success ->
        put_check_timestamps(%{base_check | status: :unhealthy, error: config_checked_event.error}, [config_checked_event])

      Time.before_min_ago?(config_checked_event.last_event_at, 15) ->
        put_check_timestamps(%{base_check | status: :stale}, [config_checked_event])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [config_checked_event])
    end
  end

  defp check(:replication_connected, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_connected, name: "Replication connected", status: :initializing}
    connected_event = find_event(events, :replication_connected)

    cond do
      is_nil(connected_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error =
          Error.invariant(
            message:
              "Sequin seems to be having trouble connecting to the database's replication slot. Either Sequin is crashing or Sequin is not receiving messages from the database's replication slot."
          )

        %{base_check | status: :unhealthy, error: error}

      connected_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :unhealthy, error: connected_event.error}, [
          connected_event
        ])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [connected_event])
    end
  end

  defp check(:replication_messages, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_messages, name: "Replication messages", status: :initializing}
    messages_processed_event = find_event(events, :replication_message_processed)
    heartbeat_recv_event = find_event(events, :replication_heartbeat_received)

    cond do
      (is_nil(heartbeat_recv_event) or is_nil(messages_processed_event)) and
          Time.before_min_ago?(slot.inserted_at, 5) ->
        error =
          Error.invariant(
            message:
              "Sequin is connected, but has not received a heartbeat from the database's replication slot. Either Sequin is crashing or Sequin is not receiving messages from the database's replication slot."
          )

        %{base_check | status: :unhealthy, error: error}

      heartbeat_recv_event && Time.before_min_ago?(heartbeat_recv_event.last_event_at, 5) ->
        error =
          Error.service(
            message:
              "Sequin is connected, but has not received a heartbeat from the database's replication slot. Either Sequin is crashing or Sequin is not receiving messages from the database's replication slot.",
            service: :postgres_replication_slot
          )

        put_check_timestamps(%{base_check | status: :unhealthy, error: error}, [heartbeat_recv_event])

      messages_processed_event && messages_processed_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :unhealthy, error: messages_processed_event.error}, [
          messages_processed_event
        ])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [heartbeat_recv_event])
    end
  end

  defp expected_event_error(entity_id, event_slug) do
    Error.invariant(
      message: "Sequin internal error: Expected a `#{event_slug}` event for #{entity_id} but none was found"
    )
  end

  defp put_check_timestamps(%Check{} = check, events) do
    fail_events = Enum.filter(events, fn event -> event.status == :fail end)
    last_success_at = events |> Enum.map(& &1.last_success_at) |> Enum.min(&compare_datetimes/2)
    initial_event_at = events |> Enum.map(& &1.initial_event_at) |> Enum.min(&compare_datetimes/2)

    erroring_since =
      if Enum.any?(fail_events) do
        fail_events
        |> Enum.map(& &1.in_status_since)
        |> Enum.min(&compare_datetimes/2)
      end

    %{check | last_healthy_at: last_success_at, initial_event_at: initial_event_at, erroring_since: erroring_since}
  end

  defp find_event(events, slug) do
    Enum.find(events, fn event -> event.slug == slug end)
  end

  defp store_event(entity_id, %Event{} = event) do
    Redis.command(["HSET", events_key(entity_id), event.slug, Jason.encode!(event)])
  end

  defp events_key(entity_id) do
    "sequin:#{env()}:health:v1:#{entity_id}"
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  defp entity_name(:postgres_database), do: "Database health"
  defp entity_name(:postgres_replication_slot), do: "Database health"
  defp entity_name(:http_endpoint), do: "Endpoint health"
  defp entity_name(:sink_consumer), do: "Consumer health"
  defp entity_name(:wal_pipeline), do: "WAL Pipeline health"

  defp entity_kind(%PostgresReplicationSlot{}), do: :postgres_replication_slot
end
