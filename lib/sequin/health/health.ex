defmodule Sequin.Health do
  @moduledoc """
  Provides the **core health subsystem** for Sequin, managing the overall status of
  various entities (e.g., Postgres replication slots, HTTP endpoints, etc.). This
  module orchestrates how **health events** are recorded and aggregated to form a consolidated
  `:healthy`, `:warning`, or `:error` state.

  ## Key Concepts

  - **Entity**: A resource whose health we track, such as a `PostgresReplicationSlot` or
    `HttpEndpoint`.

  - **Event**: A single occurrence (e.g., `:replication_slot_checked`, `:db_connectivity_checked`)
    reported via  `put_event/3`. Each event has a `status` (`:success`, `:fail`, etc.)
    and timestamp fields that indicate how long it has been in that status.

  - **Check**: An **aggregated, higher-level** view of an entity's health condition, given events.
    (e.g., "reachable", "replication_connected"). Checks are constructed to be readily displayed
    to users in the frontend.

  ## Data Flow

  1. **Events emitted**:
     Parts of the system emit health events, calling `put_event/3` to store an `Event`.
     A debouncing mechanism is used to avoid excessive writes for repeated status updates.

     On write, we first read the existing event in Redis. We then use the existing event + the incoming
     `Event` to calculate the timestamps on the newly-persisted `Event`. (`Event.set_timestamps/2`).

  2. **Check computation**:
     When `health/1` is called, the system retrieves the relevant `Event` structs from Redis. It builds
     a list of `Check` structs, each representing a different aspect of the entity’s health.

  3. **Health computation**:
     Finally, overall health is derived from checks. `health/1` returns a `%Sequin.Health{}` struct
     with the entity’s checks, aggregated status, and extra fields like `:last_healthy_at` or
     `:erroring_since`.

     This structured data can be exposed to other parts of the system or serialized
     for a frontend to display.
  """
  use TypedStruct

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health.Check
  alias Sequin.Health.Event
  alias Sequin.Health.HealthSnapshot
  alias Sequin.Pagerduty
  alias Sequin.Redis
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Time

  @type status :: :healthy | :warning | :error | :initializing | :waiting | :paused
  @type entity_kind :: :http_endpoint | :sink_consumer | :postgres_replication_slot | :wal_pipeline
  @type redis_error :: {:error, Error.ServiceError.t()}
  @type entity :: PostgresReplicationSlot.t() | HttpEndpoint.t() | SinkConsumer.t() | WalPipeline.t()

  defguardp is_entity(entity)
            when is_struct(entity, PostgresReplicationSlot) or is_struct(entity, HttpEndpoint) or
                   is_struct(entity, SinkConsumer) or is_struct(entity, WalPipeline)

  typedstruct do
    field :entity_kind, entity_kind()
    field :entity_id, String.t()
    field :checks, [Check.t()]
    field :status, status()

    field :last_healthy_at, DateTime.t() | nil
    field :erroring_since, DateTime.t() | nil
  end

  @debounce_window :timer.seconds(5)

  @doc """
  Stores an event for an entity.

  Example:

      Sequin.Health.put_event(%PostgresReplicationSlot{} = slot, %Event{
        slug: :db_connectivity_checked,
        status: :success
      })
  """
  @spec put_event(entity(), Event.t()) :: :ok | {:error, Error.t()}
  def put_event(%_{} = entity, %Event{} = event) do
    case entity do
      %PostgresReplicationSlot{} = slot ->
        put_event(:postgres_replication_slot, slot.id, event)

      %SinkConsumer{} = consumer ->
        put_event(:sink_consumer, consumer.id, event)

      %HttpEndpoint{} = endpoint ->
        put_event(:http_endpoint, endpoint.id, event)

      %WalPipeline{} = pipeline ->
        put_event(:wal_pipeline, pipeline.id, event)
    end
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

        resource_id = "#{entity_id}:#{event.slug}"
        lock_requester_id = self()

        :global.trans({resource_id, lock_requester_id}, fn ->
          with {:ok, existing_event} <- get_event(entity_id, event.slug),
               event = Event.set_timestamps(existing_event, event),
               {:ok, _} <- store_event(entity_id, event) do
            :ok
          end
        end)
    end
  end

  @doc """
  Deletes an event for an entity. For use in iex/dev.
  """
  @spec delete_event(entity_id :: String.t(), event_slug :: String.t()) :: :ok | {:error, Error.t()}
  def delete_event(entity_id, event_slug) do
    Redis.command(["HDEL", events_key(entity_id), event_slug])
  end

  @doc """
  Computes the health of an entity.
  """
  @spec health(entity()) :: {:ok, t()} | {:error, Error.t()}
  def health(entity) do
    with {:ok, checks} <- checks(entity) do
      entity_kind = entity_kind(entity)

      status =
        cond do
          paused?(entity) -> :paused
          Enum.any?(checks, fn check -> check.status == :error end) -> :error
          Enum.any?(checks, fn check -> check.status == :stale end) -> :warning
          Enum.any?(checks, fn check -> check.status == :warning end) -> :warning
          Enum.any?(checks, fn check -> check.status == :initializing end) -> :initializing
          Enum.any?(checks, fn check -> check.status == :waiting end) -> :waiting
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

  @spec add_slot_health_to_consumer_health(consumer_health :: t(), slot_health :: t()) :: t()
  def add_slot_health_to_consumer_health(%__MODULE__{} = consumer_health, %__MODULE__{} = slot_health) do
    check =
      if(slot_health.status in [:error, :warning]) do
        %Check{
          slug: :slot_health,
          status: :warning,
          error: Error.invariant(message: "Database or replication slot is unhealthy, see database health for details"),
          erroring_since: slot_health.erroring_since,
          last_healthy_at: slot_health.last_healthy_at
        }
      else
        %Check{
          slug: :slot_health,
          status: :healthy,
          error: nil,
          erroring_since: nil,
          last_healthy_at: slot_health.last_healthy_at
        }
      end

    status =
      if check.status == :warning and consumer_health.status not in [:paused, :error] do
        :warning
      else
        consumer_health.status
      end

    %{consumer_health | checks: [check | consumer_health.checks], status: status}
  end

  defp paused?(%PostgresReplicationSlot{status: status}) do
    status == :disabled
  end

  defp paused?(%SinkConsumer{status: status}) do
    status == :disabled
  end

  defp paused?(%WalPipeline{status: status}) do
    status == :disabled
  end

  defp paused?(_), do: false

  @doc """
  Converts a Health struct to a map with only the necessary fields for the frontend.
  """
  @spec to_external(t()) :: map()
  def to_external(%__MODULE__{} = health) do
    %{
      entity_kind: health.entity_kind,
      entity_id: health.entity_id,
      status: if(health.status == :waiting, do: :initializing, else: health.status),
      name: entity_name(health.entity_kind),
      # status_message: status_message(health.status, health.checks),
      checks: Enum.map(health.checks, &Check.to_external/1)
    }
  end

  @doc """
  Deletes all health-related Redis keys for the test environment.
  """
  @spec clean_test_keys() :: :ok | {:error, Error.t()}
  def clean_test_keys do
    case env() do
      :test ->
        pattern = "sequin:test:health:v1:*"

        case Redis.command(["KEYS", pattern]) do
          {:ok, []} ->
            :ok

          {:ok, keys} ->
            case Redis.command(["DEL" | keys]) do
              {:ok, _} -> :ok
              {:error, error} -> raise error
            end
        end

      _ ->
        {:error, Error.invariant(message: "clean_test_keys/0 can only be called in the test environment")}
    end
  end

  def debounce_ets_table, do: :sequin_health_debounce

  @spec get_event(String.t(), String.t()) :: {:ok, Event.t() | nil} | redis_error()
  def get_event(entity_id, event_slug) do
    with {:ok, event_json} when is_binary(event_json) <- Redis.command(["HGET", events_key(entity_id), event_slug]) do
      {:ok, Event.from_json!(event_json)}
    end
  end

  #############
  ## Helpers ##
  #############

  defp validate_event!(entity_kind, %Event{} = event) do
    valid = Event.valid_slug?(entity_kind, event.slug) and Event.valid_status?(event.status)

    unless valid do
      raise ArgumentError, "Invalid event: #{event.slug} with status #{event.status}"
    end
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
    unhealthy_checks = Enum.filter(checks, fn check -> check.status == :error end)

    if Enum.any?(unhealthy_checks) do
      unhealthy_checks
      |> Enum.min_by(fn %Check{} = check -> check.erroring_since end, &compare_datetimes/2)
      |> Map.get(:erroring_since)
    end
  end

  defp compare_datetimes(nil, _b), do: true
  defp compare_datetimes(_a, nil), do: false
  defp compare_datetimes(a, b), do: DateTime.before?(a, b)

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
  defp entity_kind(%SinkConsumer{}), do: :sink_consumer
  defp entity_kind(%HttpEndpoint{}), do: :http_endpoint
  defp entity_kind(%WalPipeline{}), do: :wal_pipeline

  ############
  ## Checks ##
  ############

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
      reachable_check.status == :error ->
        [
          reachable_check,
          %Check{slug: :replication_configuration, status: :initializing},
          %Check{slug: :replication_connected, status: :initializing},
          %Check{slug: :replication_messages, status: :initializing}
        ]

      config_check.status == :error ->
        [
          reachable_check,
          config_check,
          %Check{slug: :replication_connected, status: :initializing},
          %Check{slug: :replication_messages, status: :initializing}
        ]

      connected_check.status == :error ->
        [
          reachable_check,
          config_check,
          connected_check,
          %Check{slug: :replication_messages, status: :initializing}
        ]

      true ->
        [reachable_check, config_check, connected_check, messages_check]
    end
  end

  defp checks(%SinkConsumer{} = consumer, events) do
    config_check = check(:sink_configuration, consumer, events)
    filter_check = basic_check(:messages_filtered, events, :waiting)
    ingestion_check = basic_check(:messages_ingested, events, :waiting)
    delivery_check = basic_check(:messages_pending_delivery, events, :waiting)
    acknowledge_check = basic_check(:messages_delivered, events, :waiting)

    if config_check.status == :error do
      [
        config_check,
        %Check{slug: :messages_filtered, status: :initializing},
        %Check{slug: :messages_ingested, status: :initializing},
        %Check{slug: :messages_pending_delivery, status: :initializing},
        %Check{slug: :messages_delivered, status: :initializing}
      ]
    else
      [config_check, filter_check, ingestion_check, delivery_check, acknowledge_check]
    end
  end

  defp checks(%HttpEndpoint{}, events) do
    [basic_check(:endpoint_reachable, events)]
  end

  defp checks(%WalPipeline{}, events) do
    [
      basic_check(:messages_filtered, events),
      basic_check(:messages_ingested, events),
      basic_check(:destination_insert, events)
    ]
  end

  # Helper for the simple "event exists -> healthy/unhealthy" case
  defp basic_check(event_slug, events, base_status \\ :initializing) do
    base_check = %Check{slug: event_slug, status: base_status}
    event = find_event(events, event_slug)

    cond do
      is_nil(event) ->
        base_check

      event.status == :fail ->
        put_check_timestamps(
          %{base_check | status: :error, error: event.error},
          [event]
        )

      true ->
        status = if event.status == :success, do: :healthy, else: :warning
        put_check_timestamps(%{base_check | status: status}, [event])
    end
  end

  defp check(:reachable, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :reachable, status: :initializing}
    conn_checked_event = find_event(events, :db_connectivity_checked)

    cond do
      is_nil(conn_checked_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error = expected_event_error(slot.id, :db_connectivity_checked)
        %{base_check | status: :error, error: error}

      is_nil(conn_checked_event) ->
        base_check

      Time.before_min_ago?(conn_checked_event.last_event_at, 15) ->
        put_check_timestamps(%{base_check | status: :stale}, [conn_checked_event])

      conn_checked_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :error, error: conn_checked_event.error}, [conn_checked_event])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [conn_checked_event])
    end
  end

  defp check(:replication_configuration, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_configuration, status: :initializing}
    config_checked_event = find_event(events, :replication_slot_checked)

    cond do
      is_nil(config_checked_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error = expected_event_error(slot.id, :replication_slot_checked)
        %{base_check | status: :error, error: error}

      is_nil(config_checked_event) ->
        base_check

      config_checked_event.status != :success ->
        put_check_timestamps(%{base_check | status: :error, error: config_checked_event.error}, [config_checked_event])

      Time.before_min_ago?(config_checked_event.last_event_at, 15) ->
        put_check_timestamps(%{base_check | status: :stale}, [config_checked_event])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [config_checked_event])
    end
  end

  defp check(:replication_connected, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_connected, status: :initializing}
    connected_event = find_event(events, :replication_connected)

    cond do
      is_nil(connected_event) and Time.before_min_ago?(slot.inserted_at, 5) ->
        error =
          Error.invariant(
            message:
              "Sequin seems to be having trouble connecting to the database's replication slot. Either Sequin is crashing or Sequin is not receiving messages from the database's replication slot."
          )

        %{base_check | status: :error, error: error}

      is_nil(connected_event) ->
        base_check

      connected_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :error, error: connected_event.error}, [
          connected_event
        ])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [connected_event])
    end
  end

  defp check(:replication_messages, %PostgresReplicationSlot{} = slot, events) do
    base_check = %Check{slug: :replication_messages, status: :initializing}
    messages_processed_event = find_event(events, :replication_message_processed)
    heartbeat_recv_event = find_event(events, :replication_heartbeat_received)

    cond do
      (is_nil(heartbeat_recv_event) or is_nil(messages_processed_event)) and
          Time.before_min_ago?(slot.inserted_at, 5) ->
        error =
          Error.invariant(
            message:
              "Sequin is connected, but has not received a heartbeat from the database's replication slot. Either Sequin is crashing or the replication process has stalled for some reason."
          )

        %{base_check | status: :error, error: error}

      heartbeat_recv_event && Time.before_min_ago?(heartbeat_recv_event.last_event_at, 5) ->
        error =
          Error.service(
            message:
              "Sequin is connected, but has not received a heartbeat from the database's replication slot. Either Sequin is crashing or the replication process has stalled for some reason.",
            service: :postgres_replication_slot
          )

        put_check_timestamps(%{base_check | status: :error, error: error}, [heartbeat_recv_event])

      messages_processed_event && messages_processed_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :error, error: messages_processed_event.error}, [
          messages_processed_event
        ])

      not is_nil(messages_processed_event) ->
        put_check_timestamps(%{base_check | status: :healthy}, [messages_processed_event])

      not is_nil(heartbeat_recv_event) ->
        put_check_timestamps(%{base_check | status: :healthy}, [heartbeat_recv_event])

      true ->
        base_check
    end
  end

  defp check(:sink_configuration, %SinkConsumer{} = consumer, events) do
    base_check = %Check{slug: :sink_configuration, status: :initializing}
    config_checked_event = find_event(events, :sink_config_checked)

    toast_columns_detected = find_event(events, :toast_columns_detected)
    alert_replica_identity_not_full_dismissed = find_event(events, :alert_replica_identity_not_full_dismissed)
    alert_toast_columns_detected_dismissed = find_event(events, :alert_toast_columns_detected_dismissed)

    cond do
      is_nil(config_checked_event) and Time.before_min_ago?(consumer.inserted_at, 5) ->
        error = expected_event_error(consumer.id, :sink_config_checked)
        %{base_check | status: :error, error: error}

      is_nil(config_checked_event) ->
        base_check

      Time.before_min_ago?(config_checked_event.last_event_at, 30) ->
        put_check_timestamps(%{base_check | status: :stale}, [config_checked_event])

      config_checked_event.status == :fail and
          match?(%Error.NotFoundError{entity: :publication_membership}, config_checked_event.error) ->
        put_check_timestamps(%{base_check | status: :error, error_slug: :table_not_in_publication}, [
          config_checked_event
        ])

      config_checked_event.status == :fail ->
        put_check_timestamps(%{base_check | status: :error, error: config_checked_event.error}, [
          config_checked_event
        ])

      config_checked_event.data["replica_identity"] == "full" ->
        put_check_timestamps(%{base_check | status: :healthy}, [config_checked_event])

      consumer.message_kind == :event and is_nil(alert_replica_identity_not_full_dismissed) ->
        put_check_timestamps(%{base_check | status: :notice, error_slug: :replica_identity_not_full}, [
          config_checked_event
        ])

      not is_nil(toast_columns_detected) and is_nil(alert_toast_columns_detected_dismissed) ->
        put_check_timestamps(%{base_check | status: :notice, error_slug: :toast_columns_detected}, [
          config_checked_event
        ])

      true ->
        put_check_timestamps(%{base_check | status: :healthy}, [config_checked_event])
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

  ###############
  ## Snapshots ##
  ###############

  def update_snapshots do
    active_replications =
      Replication.all_active_pg_replications()
      |> Repo.preload([:postgres_database, :account])
      |> Enum.filter(&(&1.postgres_database.use_local_tunnel == false))
      |> Enum.filter(&(&1.annotations["ignore_health"] != true))
      |> Enum.filter(&(&1.account.annotations["ignore_health"] != true))

    active_replication_ids = Enum.map(active_replications, & &1.id)

    # Update databases
    Enum.each(active_replications, &snapshot_entity/1)

    # Update consumers
    Consumers.list_active_sink_consumers()
    |> Enum.filter(&(&1.replication_slot_id in active_replication_ids))
    |> Enum.each(&snapshot_entity/1)

    # Update WAL pipelines
    Replication.list_active_wal_pipelines()
    |> Enum.filter(&(&1.replication_slot_id in active_replication_ids))
    |> Enum.each(&snapshot_entity/1)
  end

  defp snapshot_entity(entity) do
    {:ok, health} = health(entity)

    status =
      case get_snapshot(entity) do
        {:ok, current_snapshot} -> current_snapshot.status
        {:error, %Error.NotFoundError{}} -> nil
      end

    unless status == health.status do
      # :telemetry.execute(
      #   [:sequin, :health, :status_changed],
      #   %{},
      #   %{
      #     entity_id: entity.id,
      #     entity_kind: entity_kind(entity),
      #     old_status: status,
      #     new_status: health.status
      #   }
      # )

      if Pagerduty.enabled?() do
        on_status_change(entity, status, health.status)
      end
    end

    upsert_snapshot(entity)
  end

  def on_status_change(entity, _old_status, new_status) do
    entity = Repo.preload(entity, [:account])

    unless entity.annotations["ignore_health"] || entity.account.annotations["ignore_health"] do
      dedup_key = get_dedup_key(entity)

      name =
        case entity do
          %PostgresDatabase{} ->
            "Database #{entity.name}"

          %PostgresReplicationSlot{} ->
            entity = Repo.preload(entity, [:postgres_database])
            "Replication slot #{entity.postgres_database.name}"

          %SinkConsumer{} ->
            "Consumer #{entity.name}"

          %HttpEndpoint{} ->
            "Endpoint #{entity.name}"

          %WalPipeline{} ->
            "Pipeline #{entity.name}"
        end

      case new_status do
        status when status in [:error] ->
          summary = build_error_summary(name, entity)
          severity = if status == :error, do: :critical, else: :warning

          Pagerduty.alert(dedup_key, summary, severity: severity)

        _ ->
          Pagerduty.resolve(dedup_key, "#{name} is healthy")
      end
    end
  end

  def resolve_and_ignore(entity) do
    dedup_key = get_dedup_key(entity)
    Pagerduty.resolve(dedup_key, "entity is healthy")
    ignore_health(entity)
  end

  def ignore_health(%PostgresReplicationSlot{} = slot) do
    slot = Repo.preload(slot, [:postgres_database])
    Databases.update_db(slot.postgres_database, %{annotations: %{"ignore_health" => true}})
  end

  def ignore_health(%PostgresDatabase{} = db) do
    Databases.update_db(db, %{annotations: %{"ignore_health" => true}})
  end

  def ignore_health(%SinkConsumer{} = consumer) do
    Consumers.update_sink_consumer(consumer, %{annotations: %{"ignore_health" => true}}, skip_lifecycle: true)
  end

  def ignore_health(%WalPipeline{} = pipeline) do
    Replication.update_wal_pipeline(pipeline, %{annotations: %{"ignore_health" => true}})
  end

  defp get_dedup_key(%PostgresDatabase{} = entity), do: "database_health_#{entity.id}"
  defp get_dedup_key(%PostgresReplicationSlot{} = entity), do: "replication_slot_health_#{entity.id}"
  defp get_dedup_key(%SinkConsumer{} = entity), do: "consumer_health_#{entity.id}"
  defp get_dedup_key(%HttpEndpoint{} = entity), do: "endpoint_health_#{entity.id}"
  defp get_dedup_key(%WalPipeline{} = entity), do: "pipeline_health_#{entity.id}"

  defp build_error_summary(name, entity) do
    {:ok, health} = health(entity)
    error_checks = Enum.filter(health.checks, &(&1.status in [:error, :warning]))

    check_details =
      Enum.map_join(error_checks, "\n", fn check ->
        "- #{check.slug}: #{check.status}"
      end)

    """
    #{name} (account "#{entity.account.name}") (id: #{entity.id}) is experiencing issues:
    #{check_details}
    """
  end

  @doc """
  Gets the latest health snapshot for an entity, if one exists.
  """
  @spec get_snapshot(entity()) :: {:ok, HealthSnapshot.t()} | {:error, Error.t()}
  def get_snapshot(entity) when is_entity(entity) do
    case Repo.get_by(HealthSnapshot, entity_id: entity.id) do
      nil -> {:error, Error.not_found(entity: :health_snapshot)}
      snapshot -> {:ok, snapshot}
    end
  end

  @doc """
  Upserts a health snapshot for the given entity based on its current health state.
  """
  @spec upsert_snapshot(entity()) :: {:ok, HealthSnapshot.t()} | {:error, Error.t()}
  def upsert_snapshot(entity) when is_entity(entity) do
    with {:ok, health} <- health(entity) do
      now = DateTime.utc_now()

      %HealthSnapshot{}
      |> HealthSnapshot.changeset(%{
        entity_id: entity.id,
        entity_kind: entity_kind(entity),
        status: health.status,
        health_json: Map.from_struct(health),
        sampled_at: now
      })
      |> Repo.insert(
        on_conflict: {:replace, [:status, :health_json, :sampled_at, :updated_at]},
        conflict_target: [:entity_kind, :entity_id]
      )
    end
  end
end
