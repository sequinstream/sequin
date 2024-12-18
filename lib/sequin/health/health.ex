defmodule Sequin.Health do
  @moduledoc """
  This module tracks the health of various entities in the system.
  """

  use TypedStruct

  import Sequin.Error.Guards, only: [is_error: 1]

  alias __MODULE__
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health.Check
  alias Sequin.JSON
  alias Sequin.Replication.WalPipeline

  @type status :: :healthy | :warning | :error | :initializing | :waiting
  @type entity ::
          HttpEndpoint.t()
          | SinkConsumer.t()
          | PostgresDatabase.t()
          | WalPipeline.t()

  @derive Jason.Encoder
  typedstruct do
    field :org_id, String.t()
    field :entity_id, String.t()
    field :name, String.t()

    field :entity_kind,
          :http_endpoint
          | :http_push_consumer
          | :postgres_database
          | :wal_pipeline

    field :status, status()
    field :checks, [Check.t()]
    field :last_healthy_at, DateTime.t() | nil
    field :erroring_since, DateTime.t() | nil
    field :consecutive_errors, non_neg_integer()
  end

  @spec from_json!(String.t()) :: t()
  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> JSON.decode_atom("status")
    |> JSON.decode_atom("entity_kind")
    |> JSON.decode_timestamp("last_healthy_at")
    |> JSON.decode_timestamp("erroring_since")
    |> JSON.decode_list_of_structs("checks", Check)
    |> JSON.struct(Health)
  end

  defguard is_entity(entity)
           when is_struct(entity, HttpEndpoint) or
                  is_struct(entity, SinkConsumer) or
                  is_struct(entity, PostgresDatabase) or
                  is_struct(entity, WalPipeline)

  @subject_prefix "sequin-health"
  @debounce_window :timer.seconds(10)

  def subject_prefix, do: @subject_prefix
  def debounce_ets_table, do: :sequin_health_debounce

  @doc """
  Updates the `Health` of the given entity using the given `Check`.

  Generates one or more `:nats` messages to notify subscribers of the new status and any status changes.
  """
  @spec update(entity(), atom(), status(), Error.t() | nil) :: :ok | {:error, Error.t()}
  def update(entity, check_id, status, error \\ nil)

  def update(entity, check_id, status, error) when is_entity(entity) do
    validate_status_and_error!(status, error)

    key = "#{entity.id}:#{check_id}"
    now = :os.system_time(:millisecond)

    case :ets.lookup(:sequin_health_debounce, key) do
      [{^key, ^status, last_update}] when now - last_update < @debounce_window ->
        :ok

      _ ->
        :ets.insert(:sequin_health_debounce, {key, status, now})
        do_update(entity, check_id, status, error)
    end
  end

  defp do_update(entity, check_id, status, error) do
    resource_id = "#{entity.id}:#{check_id}"
    lock_requester_id = self()

    :global.trans({resource_id, lock_requester_id}, fn ->
      with {:ok, old_health} <- get_health(entity) do
        %Check{} = expected_check = expected_check(entity, check_id, status, error)
        new_health = update_health_with_check(old_health, expected_check)
        set_health(entity.id, new_health)
      end
    end)
  end

  @doc """
  Retrieves the `Health` of the given entity.
  """
  @spec get(entity() | String.t()) :: {:ok, Health.t()} | {:error, Error.t()}
  def get(entity) when is_entity(entity) do
    get_health(entity)
  end

  @spec get!(entity() | String.t()) :: Health.t() | no_return()
  def get!(entity) when is_entity(entity) do
    case get(entity) do
      {:ok, health} -> health
      {:error, error} -> raise error
    end
  end

  #####################
  ## Expected Checks ##
  #####################

  defp expected_check(entity, check_id, status, error \\ nil)

  @postgres_checks [:reachable, :replication_connected, :replication_messages]
  defp expected_check(%PostgresDatabase{}, check_id, status, error) when check_id in @postgres_checks do
    case check_id do
      :reachable ->
        %Check{id: :reachable, name: "Database Reachable", status: status, error: error, created_at: DateTime.utc_now()}

      :replication_connected ->
        %Check{
          id: :replication_connected,
          name: "Replication Connected",
          status: status,
          error: error,
          created_at: DateTime.utc_now()
        }

      :replication_messages ->
        %Check{
          id: :replication_messages,
          name: "Replication Messages",
          status: status,
          error: error,
          created_at: DateTime.utc_now()
        }
    end
  end

  @http_endpoint_checks [:reachable]
  defp expected_check(%HttpEndpoint{}, check_id, status, error) when check_id in @http_endpoint_checks do
    case check_id do
      :reachable ->
        %Check{id: :reachable, name: "Endpoint Reachable", status: status, error: error, created_at: DateTime.utc_now()}
    end
  end

  @stream_consumer_checks [:filters, :ingestion, :receive, :acknowledge]
  defp expected_check(%SinkConsumer{type: :sequin_stream}, check_id, status, error)
       when check_id in @stream_consumer_checks do
    case check_id do
      :filters ->
        %Check{id: :filters, name: "Filters", status: status, error: error, created_at: DateTime.utc_now()}

      :ingestion ->
        %Check{id: :ingestion, name: "Ingest", status: status, error: error, created_at: DateTime.utc_now()}

      :receive ->
        %Check{id: :receive, name: "Stream", status: status, error: error, created_at: DateTime.utc_now()}

      :acknowledge ->
        %Check{id: :acknowledge, name: "Acknowledge", status: status, error: error, created_at: DateTime.utc_now()}
    end
  end

  @sink_consumer_checks [:filters, :ingestion, :receive, :push, :acknowledge]
  defp expected_check(%SinkConsumer{}, check_id, status, error) when check_id in @sink_consumer_checks do
    case check_id do
      :filters ->
        %Check{id: :filters, name: "Filters", status: status, error: error, created_at: DateTime.utc_now()}

      :ingestion ->
        %Check{id: :ingestion, name: "Ingest", status: status, error: error, created_at: DateTime.utc_now()}

      :receive ->
        %Check{id: :receive, name: "Produce", status: status, error: error, created_at: DateTime.utc_now()}

      :push ->
        %Check{id: :push, name: "Push", status: status, error: error, created_at: DateTime.utc_now()}

      :acknowledge ->
        %Check{id: :acknowledge, name: "Acknowledge", status: status, error: error, created_at: DateTime.utc_now()}
    end
  end

  @wal_pipeline_checks [:filters, :ingestion, :destination_insert]
  defp expected_check(%WalPipeline{}, check_id, status, error) when check_id in @wal_pipeline_checks do
    case check_id do
      :filters ->
        %Check{id: :filters, name: "Filters", status: status, error: error, created_at: DateTime.utc_now()}

      :ingestion ->
        %Check{id: :ingestion, name: "Ingestion", status: status, error: error, created_at: DateTime.utc_now()}

      :destination_insert ->
        %Check{
          id: :destination_insert,
          name: "Sink insert",
          status: status,
          error: error,
          created_at: DateTime.utc_now()
        }
    end
  end

  #####################
  ## Initial Health ##
  #####################

  defp initial_health(%SinkConsumer{type: :sequin_stream} = entity) do
    checks =
      @stream_consumer_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :receive} = check ->
          %{check | message: "Stream messages from the consumer."}

        %Check{id: :acknowledge} = check ->
          %{check | message: "Acknowledge messages via stream."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Consumer health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%SinkConsumer{} = entity) do
    checks =
      @sink_consumer_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :receive} = check ->
          %{check | message: "Whether the consumer is producing messages."}

        %Check{id: :push} = check ->
          %{check | message: "Pushing messages to your endpoint via HTTP."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Consumer health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%PostgresDatabase{} = entity) do
    checks =
      @postgres_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :replication_messages} = check ->
          %{check | status: :waiting, message: "Messages will replicate when there is a change in your database."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Database health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%HttpEndpoint{} = entity) do
    checks = Enum.map(@http_endpoint_checks, &expected_check(entity, &1, :initializing))

    %Health{
      name: "Endpoint health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%WalPipeline{} = entity) do
    checks =
      @wal_pipeline_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :ingestion} = check ->
          %{check | message: "Ingesting changes from the source table."}

        %Check{id: :destination_insert} = check ->
          %{check | message: "Inserting changes to the destination table."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "WAL Pipeline health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(entity) when is_entity(entity) do
    raise "Not implemented for #{entity_kind(entity)}"
  end

  ##############
  ## Internal ##
  ##############

  defp update_health_with_check(%Health{} = health, %Check{} = check) do
    new_checks = replace_check_in_list(health.checks, check)
    new_status = calculate_overall_status(new_checks)

    %Health{
      health
      | status: new_status,
        checks: new_checks,
        last_healthy_at: if(new_status == :healthy, do: DateTime.utc_now(), else: health.last_healthy_at),
        erroring_since: if(new_status == :error, do: DateTime.utc_now(), else: health.erroring_since),
        consecutive_errors: if(new_status == :error, do: health.consecutive_errors + 1, else: 0)
    }
  end

  defp calculate_overall_status(checks) do
    checks
    |> Enum.map(& &1.status)
    |> Enum.min_by(&status_priority/1)
  end

  defp status_priority(status) do
    case status do
      :error -> 0
      :warning -> 1
      :initializing -> 2
      :healthy -> 3
      :waiting -> 4
    end
  end

  defp replace_check_in_list(checks, new_check) do
    Enum.map(checks, fn check ->
      if check.id == new_check.id, do: new_check, else: check
    end)
  end

  defp entity_kind(%HttpEndpoint{}), do: :http_endpoint
  defp entity_kind(%SinkConsumer{}), do: :push_consumer
  defp entity_kind(%PostgresDatabase{}), do: :postgres_database
  defp entity_kind(%WalPipeline{}), do: :wal_pipeline

  defp get_health(%{id: nil} = entity) when is_entity(entity) do
    raise ArgumentError, "entity_id cannot be nil"
  end

  defp get_health(entity) when is_entity(entity) do
    ["GET", key(entity.id)]
    |> RedixCluster.command()
    |> case do
      {:ok, nil} ->
        {:ok, initial_health(entity)}

      {:ok, json} ->
        health = Health.from_json!(json)
        {:ok, compute_derived_fields(health)}

      {:error, error} ->
        {:error, to_service_error(error)}
    end
  end

  @doc """
  Public for testing
  """
  @spec set_health(String.t(), Health.t()) :: :ok | {:error, Error.t()}
  def set_health(entity_id, %Health{} = health) do
    ["SET", key(entity_id), Jason.encode!(health)]
    |> RedixCluster.command()
    |> case do
      {:ok, "OK"} -> :ok
      {:error, error} -> {:error, to_service_error(error)}
    end
  end

  defp to_service_error(error) when is_exception(error) do
    Error.service(service: :redis, message: Exception.message(error))
  end

  defp to_service_error(error) do
    Error.service(service: :redis, message: "Redis error: #{inspect(error)}")
  end

  defp validate_status_and_error!(:healthy, nil), do: :ok
  defp validate_status_and_error!(:initializing, nil), do: :ok
  defp validate_status_and_error!(:warning, nil), do: :ok
  defp validate_status_and_error!(:warning, error) when is_error(error), do: :ok
  defp validate_status_and_error!(:error, error) when is_error(error), do: :ok

  defp validate_status_and_error!(:error, _), do: raise(ArgumentError, "error must be an Error struct for :error status")
  defp validate_status_and_error!(status, _), do: raise(ArgumentError, "Unexpected status: #{status}")

  @doc """
  Converts a Health struct to a map with only the necessary fields for the frontend.
  """
  @spec to_external(t()) :: map()
  def to_external(%Health{} = health) do
    checks = Enum.reject(health.checks, &(&1.status == :waiting))

    %{
      entity_kind: health.entity_kind,
      entity_id: health.entity_id,
      status: health.status,
      name: health.name,
      checks:
        Enum.map(checks, fn check ->
          %{
            name: check.name,
            status: check.status,
            error: if(check.error, do: %{message: Exception.message(check.error)}),
            message: check.message
          }
        end)
    }
  end

  defp compute_derived_fields(%Health{} = health) do
    {_checks, health} = Enum.map_reduce(health.checks, health, &compute_derived_fields/2)

    health
  end

  defp compute_derived_fields(
         %Check{id: :replication_connected, status: :initializing} = check,
         %Health{entity_kind: :postgres_database} = health
       ) do
    thirty_seconds_ago = DateTime.add(DateTime.utc_now(), -30 * 1000, :millisecond)

    if DateTime.before?(check.created_at, thirty_seconds_ago) do
      updated_check = %{check | status: :error, message: "Replication took too long to connect."}
      updated_health = update_health_status(health, updated_check)
      {updated_check, updated_health}
    else
      {check, health}
    end
  end

  defp compute_derived_fields(check, health), do: {check, health}

  defp update_health_status(health, updated_check) do
    updated_checks =
      Enum.map(health.checks, fn check ->
        if check.id == updated_check.id, do: updated_check, else: check
      end)

    new_status = calculate_overall_status(updated_checks)
    %{health | status: new_status, checks: updated_checks}
  end

  @doc """
  Resets the health for the given entity to initializing.
  """
  @spec reset(entity()) :: :ok | {:error, Error.t()}
  def reset(entity) when is_entity(entity) do
    new_health = initial_health(entity)
    set_health(entity.id, new_health)
  end

  defp key(entity_id) do
    env = if env() in [:dev, :test], do: "#{env()}:", else: ""
    "ix:#{env}health:v0:#{entity_id}"
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  @doc """
  Deletes all health-related Redis keys for the test environment.
  """
  @spec clean_test_keys() :: :ok | {:error, Error.t()}
  def clean_test_keys do
    case env() do
      :test ->
        pattern = "ix:test:health:*"

        case RedixCluster.command(["KEYS", pattern]) do
          {:ok, []} ->
            :ok

          {:ok, keys} ->
            case RedixCluster.command(["DEL" | keys]) do
              {:ok, _} -> :ok
              {:error, error} -> raise error
            end
        end

      _ ->
        {:error, Error.invariant(message: "clean_test_keys/0 can only be called in the test environment")}
    end
  end
end
